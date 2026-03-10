import argparse
import asyncio
import contextlib
import hmac
import json
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import aiohttp
from aiohttp import web

try:
    from eth_account import Account
except Exception:  # pragma: no cover - optional dependency before live deploy
    Account = None  # type: ignore


ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
HEX_RE = re.compile(r"^0x[a-fA-F0-9]*$")


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def parse_int_like(value: Any, *, default: int = 0, field: str = "value") -> int:
    if value is None:
        return int(default)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return int(default)
    if raw.startswith("0x") or raw.startswith("0X"):
        return int(raw, 16)
    if raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
        return int(raw)
    raise ValueError(f"{field} must be integer (decimal or 0x)")


def to_hex_quantity(value: int) -> str:
    v = int(value)
    if v < 0:
        raise ValueError("quantity cannot be negative")
    return hex(v)


def normalize_address(addr: str, field: str = "address") -> str:
    raw = str(addr or "").strip()
    if not ADDRESS_RE.match(raw):
        raise ValueError(f"{field} is invalid")
    return raw.lower()


@dataclass
class SignerConfig:
    host: str
    port: int
    rpc_url: str
    chain_id: int
    private_key: Optional[str]
    auth_token: Optional[str]
    builder_webhook_url: Optional[str]
    builder_auth_token: Optional[str]
    timeout_sec: int
    mock_mode: bool
    gas_limit_multiplier: float
    gas_limit_fallback: int
    gas_price_multiplier: float
    max_priority_fee_wei: int
    wait_receipt_sec: int


def load_json_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_signer_config(path: str) -> SignerConfig:
    raw = load_json_config(path)

    host = str(raw.get("HOST", "127.0.0.1")).strip() or "127.0.0.1"
    port = parse_int_like(raw.get("PORT"), default=19090, field="PORT")
    rpc_url = str(raw.get("RPC_URL", "")).strip()
    chain_id = parse_int_like(raw.get("CHAIN_ID"), default=8453, field="CHAIN_ID")
    private_key = str(raw.get("PRIVATE_KEY", "")).strip() or None
    auth_token = str(raw.get("AUTH_TOKEN", "")).strip() or None
    builder_webhook_url = str(raw.get("BUILDER_WEBHOOK_URL", "")).strip() or None
    builder_auth_token = str(raw.get("BUILDER_AUTH_TOKEN", "")).strip() or None
    timeout_sec = parse_int_like(raw.get("TIMEOUT_SEC"), default=15, field="TIMEOUT_SEC")
    mock_mode = parse_bool(raw.get("MOCK_MODE"), default=True)
    gas_limit_multiplier = float(raw.get("GAS_LIMIT_MULTIPLIER", 1.15))
    gas_limit_fallback = parse_int_like(
        raw.get("GAS_LIMIT_FALLBACK"), default=800000, field="GAS_LIMIT_FALLBACK"
    )
    gas_price_multiplier = float(raw.get("GAS_PRICE_MULTIPLIER", 1.10))
    max_priority_fee_wei = parse_int_like(
        raw.get("MAX_PRIORITY_FEE_WEI"), default=50000000, field="MAX_PRIORITY_FEE_WEI"
    )
    wait_receipt_sec = parse_int_like(
        raw.get("WAIT_RECEIPT_SEC"), default=0, field="WAIT_RECEIPT_SEC"
    )

    if not rpc_url:
        raise ValueError("RPC_URL is required")
    if port <= 0 or port > 65535:
        raise ValueError("PORT must be in [1, 65535]")
    if chain_id <= 0:
        raise ValueError("CHAIN_ID must be > 0")
    if timeout_sec < 3 or timeout_sec > 120:
        raise ValueError("TIMEOUT_SEC must be in [3, 120]")
    if gas_limit_multiplier < 1.0 or gas_limit_multiplier > 3.0:
        raise ValueError("GAS_LIMIT_MULTIPLIER must be in [1.0, 3.0]")
    if gas_price_multiplier < 1.0 or gas_price_multiplier > 3.0:
        raise ValueError("GAS_PRICE_MULTIPLIER must be in [1.0, 3.0]")
    if gas_limit_fallback <= 21000:
        raise ValueError("GAS_LIMIT_FALLBACK must be > 21000")
    if max_priority_fee_wei < 0:
        raise ValueError("MAX_PRIORITY_FEE_WEI must be >= 0")
    if wait_receipt_sec < 0 or wait_receipt_sec > 180:
        raise ValueError("WAIT_RECEIPT_SEC must be in [0, 180]")
    if (not mock_mode) and (not private_key):
        raise ValueError("PRIVATE_KEY is required when MOCK_MODE=false")
    if (not mock_mode) and (Account is None):
        raise ValueError(
            "eth-account is not installed; run: python -m pip install -r requirements.txt"
        )

    return SignerConfig(
        host=host,
        port=port,
        rpc_url=rpc_url,
        chain_id=chain_id,
        private_key=private_key,
        auth_token=auth_token,
        builder_webhook_url=builder_webhook_url,
        builder_auth_token=builder_auth_token,
        timeout_sec=timeout_sec,
        mock_mode=mock_mode,
        gas_limit_multiplier=gas_limit_multiplier,
        gas_limit_fallback=gas_limit_fallback,
        gas_price_multiplier=gas_price_multiplier,
        max_priority_fee_wei=max_priority_fee_wei,
        wait_receipt_sec=wait_receipt_sec,
    )


class JsonRpcClient:
    def __init__(self, rpc_url: str, timeout_sec: int):
        self.rpc_url = rpc_url
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec)
        self._session: Optional[aiohttp.ClientSession] = None
        self._id = 1

    async def __aenter__(self) -> "JsonRpcClient":
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def call(self, method: str, params: Any) -> Any:
        if not self._session:
            raise RuntimeError("RPC session is not initialized")
        req_id = self._id
        self._id += 1
        payload = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        async with self._session.post(self.rpc_url, json=payload) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"RPC HTTP {resp.status}: {text}")
            data = json.loads(text)
            if "error" in data:
                raise RuntimeError(f"RPC error: {data['error']}")
            return data.get("result")


class SignerService:
    def __init__(self, cfg: SignerConfig):
        self.cfg = cfg
        self.rpc = JsonRpcClient(cfg.rpc_url, timeout_sec=cfg.timeout_sec)
        self.nonce_lock = asyncio.Lock()
        self.started_at = int(time.time())
        self.stats = {
            "requests_total": 0,
            "requests_ok": 0,
            "requests_failed": 0,
            "builder_calls": 0,
            "broadcasts": 0,
        }
        self.account = None
        self.account_address = ""
        if cfg.private_key:
            if Account is None:
                raise RuntimeError(
                    "eth-account is not installed; run: python -m pip install -r requirements.txt"
                )
            self.account = Account.from_key(cfg.private_key)
            self.account_address = self.account.address.lower()

    async def __aenter__(self) -> "SignerService":
        await self.rpc.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.rpc.__aexit__(exc_type, exc, tb)

    def _authorized(self, request: web.Request) -> bool:
        if not self.cfg.auth_token:
            return True
        auth_header = str(request.headers.get("Authorization", "")).strip()
        if not auth_header.lower().startswith("bearer "):
            return False
        candidate = auth_header[7:].strip()
        return hmac.compare_digest(candidate, self.cfg.auth_token)

    async def health_handler(self, request: web.Request) -> web.Response:
        payload = {
            "ok": True,
            "service": "vlh-signer",
            "mockMode": bool(self.cfg.mock_mode),
            "chainId": int(self.cfg.chain_id),
            "account": self.account_address or None,
            "builderConfigured": bool(self.cfg.builder_webhook_url),
            "startedAt": int(self.started_at),
            "stats": dict(self.stats),
        }
        return web.json_response(payload)

    async def _call_builder(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        if not self.cfg.builder_webhook_url:
            raise RuntimeError("BUILDER_WEBHOOK_URL is not configured")
        timeout = aiohttp.ClientTimeout(total=self.cfg.timeout_sec)
        headers = {"Content-Type": "application/json"}
        if self.cfg.builder_auth_token:
            headers["Authorization"] = f"Bearer {self.cfg.builder_auth_token}"
        body = {
            "intent": intent,
            "signer": {
                "chain_id": int(self.cfg.chain_id),
                "address": self.account_address or None,
            },
        }
        self.stats["builder_calls"] = int(self.stats.get("builder_calls", 0)) + 1
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.cfg.builder_webhook_url, headers=headers, json=body) as resp:
                raw = await resp.text()
                parsed: Dict[str, Any] = {}
                with contextlib.suppress(Exception):
                    parsed = json.loads(raw) if raw else {}
                if resp.status >= 400:
                    msg = str(parsed.get("error") or parsed.get("message") or raw or "").strip()
                    raise RuntimeError(f"builder webhook failed: {msg or resp.status}")
                if not isinstance(parsed, dict):
                    raise RuntimeError("builder response must be json object")
                return parsed

    async def _get_nonce(self, address: str) -> int:
        raw = await self.rpc.call("eth_getTransactionCount", [address, "pending"])
        return parse_int_like(raw, field="nonce")

    async def _get_latest_block(self) -> Dict[str, Any]:
        out = await self.rpc.call("eth_getBlockByNumber", ["latest", False])
        return out or {}

    async def _estimate_gas(self, tx_for_estimate: Dict[str, Any]) -> int:
        raw = await self.rpc.call("eth_estimateGas", [tx_for_estimate])
        est = parse_int_like(raw, field="gas")
        gas = int(est * float(self.cfg.gas_limit_multiplier))
        return max(21000, gas)

    async def _wait_receipt(self, tx_hash: str, timeout_sec: int) -> Optional[Dict[str, Any]]:
        deadline = time.time() + max(0, timeout_sec)
        while time.time() < deadline:
            rec = await self.rpc.call("eth_getTransactionReceipt", [tx_hash])
            if rec:
                return rec
            await asyncio.sleep(1)
        return None

    def _build_explorer_url(self, tx_hash: str) -> str:
        # Base mainnet default explorer.
        if int(self.cfg.chain_id) == 8453:
            return f"https://basescan.org/tx/{tx_hash}"
        return ""

    def _normalize_unsigned_tx(self, raw_tx: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(raw_tx, dict):
            raise ValueError("tx must be an object")
        to_addr = normalize_address(raw_tx.get("to", ""), "tx.to")
        data_hex = str(raw_tx.get("data", "0x")).strip()
        if not data_hex:
            data_hex = "0x"
        if not HEX_RE.match(data_hex):
            raise ValueError("tx.data must be hex string")
        value_int = parse_int_like(raw_tx.get("value", 0), field="tx.value")
        if value_int < 0:
            raise ValueError("tx.value cannot be negative")
        tx: Dict[str, Any] = {
            "to": to_addr,
            "data": data_hex,
            "value": int(value_int),
            "chainId": int(self.cfg.chain_id),
        }
        if raw_tx.get("nonce") is not None:
            tx["nonce"] = parse_int_like(raw_tx.get("nonce"), field="tx.nonce")
        if raw_tx.get("gas") is not None:
            tx["gas"] = parse_int_like(raw_tx.get("gas"), field="tx.gas")
        if raw_tx.get("gasPrice") is not None:
            tx["gasPrice"] = parse_int_like(raw_tx.get("gasPrice"), field="tx.gasPrice")
        if raw_tx.get("maxFeePerGas") is not None:
            tx["maxFeePerGas"] = parse_int_like(
                raw_tx.get("maxFeePerGas"), field="tx.maxFeePerGas"
            )
        if raw_tx.get("maxPriorityFeePerGas") is not None:
            tx["maxPriorityFeePerGas"] = parse_int_like(
                raw_tx.get("maxPriorityFeePerGas"), field="tx.maxPriorityFeePerGas"
            )
        if raw_tx.get("type") is not None:
            tx["type"] = parse_int_like(raw_tx.get("type"), field="tx.type")
        return tx

    async def _fill_tx_fees_and_gas(self, tx: Dict[str, Any], from_addr: str) -> Dict[str, Any]:
        tx_final = dict(tx)
        if "gas" not in tx_final:
            try:
                tx_final["gas"] = await self._estimate_gas(
                    {
                        "from": from_addr,
                        "to": tx_final["to"],
                        "data": tx_final.get("data", "0x"),
                        "value": to_hex_quantity(parse_int_like(tx_final.get("value", 0))),
                    }
                )
            except Exception:
                tx_final["gas"] = int(self.cfg.gas_limit_fallback)

        has_legacy = "gasPrice" in tx_final
        has_1559 = "maxFeePerGas" in tx_final or "maxPriorityFeePerGas" in tx_final
        if has_legacy:
            gp = parse_int_like(tx_final["gasPrice"], field="tx.gasPrice")
            tx_final["gasPrice"] = int(gp * float(self.cfg.gas_price_multiplier))
            return tx_final
        if has_1559:
            if "maxPriorityFeePerGas" not in tx_final:
                tx_final["maxPriorityFeePerGas"] = int(self.cfg.max_priority_fee_wei)
            if "maxFeePerGas" not in tx_final:
                tx_final["maxFeePerGas"] = int(
                    parse_int_like(tx_final["maxPriorityFeePerGas"])
                    + parse_int_like(tx_final.get("maxPriorityFeePerGas", 0))
                )
            tx_final["type"] = int(tx_final.get("type", 2))
            return tx_final

        latest = await self._get_latest_block()
        base_fee = latest.get("baseFeePerGas")
        if base_fee is not None:
            base = parse_int_like(base_fee, field="baseFeePerGas")
            priority = int(self.cfg.max_priority_fee_wei)
            max_fee = int(base * 2 + priority)
            tx_final["type"] = 2
            tx_final["maxPriorityFeePerGas"] = priority
            tx_final["maxFeePerGas"] = int(max_fee * float(self.cfg.gas_price_multiplier))
            return tx_final

        gas_price_raw = await self.rpc.call("eth_gasPrice", [])
        gas_price = parse_int_like(gas_price_raw, field="gasPrice")
        tx_final["gasPrice"] = int(gas_price * float(self.cfg.gas_price_multiplier))
        return tx_final

    async def _sign_and_send_tx(self, unsigned_tx: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        if not self.account or not self.cfg.private_key:
            raise RuntimeError("signer account is not configured")
        from_addr = self.account.address.lower()
        async with self.nonce_lock:
            tx = dict(unsigned_tx)
            if "nonce" not in tx:
                tx["nonce"] = await self._get_nonce(from_addr)
            tx = await self._fill_tx_fees_and_gas(tx, from_addr)
            signed = Account.sign_transaction(tx, self.cfg.private_key)
            raw_hex = signed.raw_transaction.hex()
            tx_hash = await self.rpc.call("eth_sendRawTransaction", [raw_hex])
            self.stats["broadcasts"] = int(self.stats.get("broadcasts", 0)) + 1
            return str(tx_hash), tx

    async def execute_handler(self, request: web.Request) -> web.Response:
        self.stats["requests_total"] = int(self.stats.get("requests_total", 0)) + 1
        if not self._authorized(request):
            self.stats["requests_failed"] = int(self.stats.get("requests_failed", 0)) + 1
            return web.json_response({"error": "unauthorized"}, status=401)

        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("json body must be object")
        except Exception as e:
            self.stats["requests_failed"] = int(self.stats.get("requests_failed", 0)) + 1
            return web.json_response({"error": f"invalid json body: {e}"}, status=400)

        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else payload
        if not isinstance(intent, dict):
            self.stats["requests_failed"] = int(self.stats.get("requests_failed", 0)) + 1
            return web.json_response({"error": "intent must be object"}, status=400)

        chain_id = parse_int_like(intent.get("chain_id"), default=self.cfg.chain_id, field="chain_id")
        if chain_id != int(self.cfg.chain_id):
            self.stats["requests_failed"] = int(self.stats.get("requests_failed", 0)) + 1
            return web.json_response(
                {"error": f"chain_id mismatch: intent={chain_id}, signer={self.cfg.chain_id}"},
                status=400,
            )

        if self.cfg.mock_mode:
            mock_hash = f"mock-{int(intent.get('trade_id', 0) or 0)}-{int(time.time())}"
            self.stats["requests_ok"] = int(self.stats.get("requests_ok", 0)) + 1
            return web.json_response({"ok": True, "accepted": True, "tx_hash": mock_hash})

        try:
            tx_payload = payload.get("tx")
            if not isinstance(tx_payload, dict):
                tx_payload = None
            builder_response: Optional[Dict[str, Any]] = None
            if tx_payload is None and self.cfg.builder_webhook_url:
                builder_response = await self._call_builder(intent)
                tx_payload = builder_response.get("tx")
                if tx_payload is None and ("to" in builder_response and "data" in builder_response):
                    tx_payload = builder_response
                if tx_payload is None and bool(builder_response.get("accepted")):
                    self.stats["requests_ok"] = int(self.stats.get("requests_ok", 0)) + 1
                    return web.json_response(
                        {
                            "ok": True,
                            "accepted": True,
                            "tx_hash": str(builder_response.get("tx_hash") or ""),
                            "note": "builder accepted without unsigned tx",
                        }
                    )

            if not isinstance(tx_payload, dict):
                raise RuntimeError(
                    "no unsigned tx provided; set BUILDER_WEBHOOK_URL or include tx in request"
                )

            unsigned_tx = self._normalize_unsigned_tx(tx_payload)
            tx_hash, signed_tx = await self._sign_and_send_tx(unsigned_tx)
            explorer_url = self._build_explorer_url(tx_hash)
            response: Dict[str, Any] = {
                "ok": True,
                "tx_hash": tx_hash,
                "chain_id": int(self.cfg.chain_id),
                "from": self.account_address or None,
                "explorer_url": explorer_url or None,
                "gas": int(signed_tx.get("gas", 0)),
            }
            if self.cfg.wait_receipt_sec > 0:
                receipt = await self._wait_receipt(tx_hash, timeout_sec=self.cfg.wait_receipt_sec)
                response["receipt"] = receipt
            self.stats["requests_ok"] = int(self.stats.get("requests_ok", 0)) + 1
            return web.json_response(response)
        except Exception as e:
            self.stats["requests_failed"] = int(self.stats.get("requests_failed", 0)) + 1
            return web.json_response({"error": f"{type(e).__name__}: {e}"}, status=400)

    async def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/health", self.health_handler)
        app.router.add_post("/execute", self.execute_handler)
        return app


async def run_service(config_path: str) -> None:
    cfg = load_signer_config(config_path)
    async with SignerService(cfg) as svc:
        app = await svc.create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=cfg.host, port=cfg.port)
        await site.start()

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _on_stop() -> None:
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _on_stop)

        await stop_event.wait()
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="VLH signer webhook service")
    parser.add_argument("--config", default="./signer_config.json", help="signer config path")
    args = parser.parse_args()
    asyncio.run(run_service(args.config))


if __name__ == "__main__":
    main()
