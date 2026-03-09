import argparse
import asyncio
import contextlib
import json
import signal
import sqlite3
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import aiohttp
from aiohttp import web

getcontext().prec = 60

TRANSFER_TOPIC0 = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)
DECIMALS_SELECTOR = "0x313ce567"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
GET_RESERVES_SELECTOR = "0x0902f1ac"


def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        raise ValueError(f"address must be a string, got: {type(addr)}")
    addr = addr.strip().lower()
    if not addr.startswith("0x") or len(addr) != 42:
        raise ValueError(f"invalid address format: {addr}")
    int(addr[2:], 16)
    return addr


def topic_address(addr: str) -> str:
    return "0x" + ("0" * 24) + normalize_address(addr)[2:]


def parse_hex_int(value: Optional[str]) -> int:
    if value is None:
        return 0
    return int(value, 16)


def parse_bool_like(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if value is None:
        return False
    raw = str(value).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def parse_bool_request(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError("paused must be a boolean")


def decode_topic_address(topic: str) -> str:
    topic = topic.lower()
    if topic.startswith("0x"):
        topic = topic[2:]
    return "0x" + topic[-40:]


def decimal_to_str(v: Optional[Decimal], places: int = 18) -> Optional[str]:
    if v is None:
        return None
    q = Decimal(10) ** -places
    return str(v.quantize(q))


def raw_to_decimal(value: int, decimals: int) -> Decimal:
    return Decimal(value) / (Decimal(10) ** decimals)


EVENT_DECIMAL_FIELDS = {
    "token_bought",
    "fee_v",
    "tax_v",
    "spent_v_est",
    "spent_v_actual",
    "cost_v",
    "total_supply",
    "virtual_price_usd",
    "breakeven_fdv_v",
    "breakeven_fdv_usd",
}
EVENT_INT_FIELDS = {"block_number", "block_timestamp"}
EVENT_BOOL_FIELDS = {"is_my_wallet", "anomaly", "is_price_stale"}


def serialize_event_for_bus(event: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(event)
    for key in EVENT_DECIMAL_FIELDS:
        if key in out and out[key] is not None:
            out[key] = str(out[key])
    for key in EVENT_INT_FIELDS:
        if key in out and out[key] is not None:
            out[key] = int(out[key])
    for key in EVENT_BOOL_FIELDS:
        if key in out and out[key] is not None:
            out[key] = bool(out[key])
    return out


def deserialize_event_from_bus(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    for key in EVENT_DECIMAL_FIELDS:
        if key in out and out[key] is not None:
            out[key] = Decimal(str(out[key]))
    for key in EVENT_INT_FIELDS:
        if key in out and out[key] is not None:
            out[key] = int(out[key])
    for key in EVENT_BOOL_FIELDS:
        if key in out and out[key] is not None:
            out[key] = bool(out[key])
    return out


@dataclass
class LaunchConfig:
    name: str
    internal_pool_addr: str
    fee_addr: str
    tax_addr: str
    token_total_supply: Decimal
    fee_rate: Decimal


@dataclass
class AppConfig:
    chain_id: int
    ws_rpc_url: str
    http_rpc_url: str
    backfill_http_rpc_url: Optional[str]
    virtual_token_addr: str
    fee_rate_default: Decimal
    total_supply_default: Decimal
    launch_configs: List[LaunchConfig]
    my_wallets: Set[str]
    top_n: int
    confirmations: int
    agg_minute_window: int
    price_mode: str
    virtual_usdc_pair_addr: Optional[str]
    price_refresh_sec: int
    db_mode: str
    sqlite_path: str
    db_batch_size: int
    db_flush_ms: int
    receipt_workers: int
    receipt_workers_realtime: int
    receipt_workers_backfill: int
    max_rpc_retries: int
    backfill_chunk_blocks: int
    backfill_interval_sec: int
    log_level: str
    jsonl_path: str
    event_bus_sqlite_path: str
    api_host: str
    api_port: int
    cors_allow_origins: List[str]


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    forbidden_keys = {"SUPABASE_URL", "SUPABASE_KEY"}
    found_forbidden = forbidden_keys.intersection(set(raw.keys()))
    if found_forbidden:
        bad = ", ".join(sorted(found_forbidden))
        raise ValueError(f"v1.1 does not allow Supabase hot path config keys: {bad}")

    chain_id = int(raw.get("CHAIN_ID", 8453))
    ws_rpc_url = str(raw["WS_RPC_URL"]).strip()
    http_rpc_url = str(raw["HTTP_RPC_URL"]).strip()
    backfill_http_rpc_url_raw = str(raw.get("BACKFILL_HTTP_RPC_URL", "")).strip()
    backfill_http_rpc_url = backfill_http_rpc_url_raw or None
    virtual_token_addr = normalize_address(raw["VIRTUAL_TOKEN_ADDR"])

    fee_rate_default = Decimal(str(raw.get("FEE_RATE_DEFAULT", "0.01")))
    total_supply_default = Decimal(str(raw.get("TOTAL_SUPPLY_DEFAULT", "1000000000")))
    if fee_rate_default <= 0 or fee_rate_default >= 1:
        raise ValueError("FEE_RATE_DEFAULT must be in (0,1)")

    launch_configs: List[LaunchConfig] = []
    for item in raw.get("LAUNCH_CONFIGS", []):
        fee_rate = Decimal(str(item.get("fee_rate", fee_rate_default)))
        if fee_rate <= 0 or fee_rate >= 1:
            raise ValueError(f"project {item.get('name')} fee_rate is invalid: {fee_rate}")
        launch_configs.append(
            LaunchConfig(
                name=str(item["name"]),
                internal_pool_addr=normalize_address(item["internal_pool_addr"]),
                fee_addr=normalize_address(item["fee_addr"]),
                tax_addr=normalize_address(item["tax_addr"]),
                token_total_supply=Decimal(
                    str(item.get("token_total_supply", total_supply_default))
                ),
                fee_rate=fee_rate,
            )
        )
    if not launch_configs:
        raise ValueError("LAUNCH_CONFIGS cannot be empty")

    my_wallets = {normalize_address(x) for x in raw.get("MY_WALLETS", [])}

    price_mode = str(raw.get("PRICE_MODE", "onchain_pool"))
    pair = raw.get("VIRTUAL_USDC_PAIR_ADDR")
    virtual_usdc_pair_addr = normalize_address(pair) if pair else None

    db_mode = str(raw.get("DB_MODE", "sqlite")).lower()
    if db_mode not in {"sqlite", "postgres"}:
        raise ValueError("DB_MODE only supports sqlite or postgres")
    if db_mode == "postgres":
        raise ValueError("current runtime only supports sqlite; set DB_MODE=sqlite")

    receipt_workers = int(raw.get("RECEIPT_WORKERS", 8))
    receipt_workers_realtime = int(
        raw.get("RECEIPT_WORKERS_REALTIME", receipt_workers)
    )
    receipt_workers_backfill = int(
        raw.get("RECEIPT_WORKERS_BACKFILL", receipt_workers)
    )
    if receipt_workers <= 0:
        raise ValueError("RECEIPT_WORKERS must be >= 1")
    if receipt_workers_realtime <= 0:
        raise ValueError("RECEIPT_WORKERS_REALTIME must be >= 1")
    if receipt_workers_backfill <= 0:
        raise ValueError("RECEIPT_WORKERS_BACKFILL must be >= 1")

    cors_allow_origins_raw = raw.get("CORS_ALLOW_ORIGINS", [])
    cors_allow_origins: List[str] = []
    if isinstance(cors_allow_origins_raw, str):
        cors_allow_origins = [
            x.strip().rstrip("/")
            for x in cors_allow_origins_raw.split(",")
            if x and x.strip()
        ]
    elif isinstance(cors_allow_origins_raw, list):
        cors_allow_origins = [
            str(x).strip().rstrip("/")
            for x in cors_allow_origins_raw
            if str(x).strip()
        ]

    return AppConfig(
        chain_id=chain_id,
        ws_rpc_url=ws_rpc_url,
        http_rpc_url=http_rpc_url,
        backfill_http_rpc_url=backfill_http_rpc_url,
        virtual_token_addr=virtual_token_addr,
        fee_rate_default=fee_rate_default,
        total_supply_default=total_supply_default,
        launch_configs=launch_configs,
        my_wallets=my_wallets,
        top_n=int(raw.get("TOP_N", 50)),
        confirmations=int(raw.get("CONFIRMATIONS", 1)),
        agg_minute_window=int(raw.get("AGG_MINUTE_WINDOW", 0)),
        price_mode=price_mode,
        virtual_usdc_pair_addr=virtual_usdc_pair_addr,
        price_refresh_sec=int(raw.get("PRICE_REFRESH_SEC", 3)),
        db_mode=db_mode,
        sqlite_path=str(raw.get("SQLITE_PATH", "./data/virtuals_v11.db")),
        db_batch_size=int(raw.get("DB_BATCH_SIZE", 200)),
        db_flush_ms=int(raw.get("DB_FLUSH_MS", 500)),
        receipt_workers=receipt_workers,
        receipt_workers_realtime=receipt_workers_realtime,
        receipt_workers_backfill=receipt_workers_backfill,
        max_rpc_retries=int(raw.get("MAX_RPC_RETRIES", 5)),
        backfill_chunk_blocks=int(raw.get("BACKFILL_CHUNK_BLOCKS", 20)),
        backfill_interval_sec=int(raw.get("BACKFILL_INTERVAL_SEC", 8)),
        log_level=str(raw.get("LOG_LEVEL", "info")).lower(),
        jsonl_path=str(raw.get("JSONL_PATH", "./data/events.jsonl")),
        event_bus_sqlite_path=str(raw.get("EVENT_BUS_SQLITE_PATH", "./data/virtuals_bus.db")),
        api_host=str(raw.get("API_HOST", "127.0.0.1")),
        api_port=int(raw.get("API_PORT", 8080)),
        cors_allow_origins=cors_allow_origins,
    )


class RPCClient:
    def __init__(self, url: str, max_retries: int = 5, timeout_sec: int = 12):
        self.url = url
        self.max_retries = max_retries
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec)
        self._session: Optional[aiohttp.ClientSession] = None
        self._id = 1

    async def __aenter__(self) -> "RPCClient":
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()

    async def call(self, method: str, params: List[Any]) -> Any:
        if not self._session:
            raise RuntimeError("RPC session is not initialized")
        payload = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}
        self._id += 1

        backoff = 0.5
        for attempt in range(1, self.max_retries + 1):
            try:
                async with self._session.post(self.url, json=payload) as resp:
                    data = await resp.json(content_type=None)
                if "error" in data:
                    raise RuntimeError(f"RPC error: {data['error']}")
                return data.get("result")
            except Exception:
                if attempt >= self.max_retries:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2

    async def get_receipt(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        return await self.call("eth_getTransactionReceipt", [tx_hash])

    async def get_block_by_number(self, block_number: int) -> Optional[Dict[str, Any]]:
        return await self.call("eth_getBlockByNumber", [hex(block_number), False])

    async def get_latest_block_number(self) -> int:
        result = await self.call("eth_blockNumber", [])
        return int(result, 16)

    async def get_logs(
        self,
        from_block: int,
        to_block: int,
        address: Optional[str] = None,
        topics: Optional[List[Any]] = None,
    ) -> List[Dict[str, Any]]:
        f: Dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if address:
            f["address"] = address
        if topics:
            f["topics"] = topics
        result = await self.call("eth_getLogs", [f])
        return result or []

    async def eth_call(self, to: str, data: str) -> str:
        result = await self.call("eth_call", [{"to": to, "data": data}, "latest"])
        return result


class PriceService:
    def __init__(
        self,
        cfg: AppConfig,
        rpc: RPCClient,
        is_paused: Optional[Callable[[], bool]] = None,
    ):
        self.cfg = cfg
        self.rpc = rpc
        self._is_paused = is_paused
        self._price: Optional[Decimal] = None
        self._last_updated: float = 0.0
        self._token0: Optional[str] = None
        self._token1: Optional[str] = None
        self._token0_decimals: Optional[int] = None
        self._token1_decimals: Optional[int] = None
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self.cfg.price_mode != "onchain_pool":
            return
        if not self.cfg.virtual_usdc_pair_addr:
            return
        if not (self._is_paused and self._is_paused()):
            await self.refresh_once()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        while True:
            try:
                if self._is_paused and self._is_paused():
                    await asyncio.sleep(1)
                    continue
                await self.refresh_once()
            except Exception:
                pass
            await asyncio.sleep(max(1, self.cfg.price_refresh_sec))

    async def _read_address(self, to: str, selector: str) -> str:
        out = await self.rpc.eth_call(to, selector)
        return "0x" + out[-40:].lower()

    async def _read_decimals(self, token: str) -> int:
        out = await self.rpc.eth_call(token, DECIMALS_SELECTOR)
        return int(out, 16)

    async def refresh_once(self) -> None:
        if not self.cfg.virtual_usdc_pair_addr:
            return
        pair = self.cfg.virtual_usdc_pair_addr
        async with self._lock:
            if not self._token0:
                self._token0 = normalize_address(await self._read_address(pair, TOKEN0_SELECTOR))
                self._token1 = normalize_address(await self._read_address(pair, TOKEN1_SELECTOR))
                self._token0_decimals = await self._read_decimals(self._token0)
                self._token1_decimals = await self._read_decimals(self._token1)

            reserves_hex = await self.rpc.eth_call(pair, GET_RESERVES_SELECTOR)
            if not reserves_hex or reserves_hex == "0x":
                return
            data = reserves_hex[2:]
            if len(data) < 64 * 3:
                return
            reserve0 = int(data[0:64], 16)
            reserve1 = int(data[64:128], 16)
            if reserve0 == 0 or reserve1 == 0:
                return

            vaddr = self.cfg.virtual_token_addr
            if self._token0 == vaddr:
                v = raw_to_decimal(reserve0, self._token0_decimals or 18)
                q = raw_to_decimal(reserve1, self._token1_decimals or 6)
            elif self._token1 == vaddr:
                v = raw_to_decimal(reserve1, self._token1_decimals or 18)
                q = raw_to_decimal(reserve0, self._token0_decimals or 6)
            else:
                return
            if v > 0:
                self._price = q / v
                self._last_updated = time.time()

    async def get_price(self) -> Tuple[Optional[Decimal], bool]:
        async with self._lock:
            if self._price is None:
                return None, True
            age = time.time() - self._last_updated
            return self._price, age > 120


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                block_timestamp INTEGER NOT NULL,
                internal_pool TEXT NOT NULL,
                fee_addr TEXT NOT NULL,
                tax_addr TEXT NOT NULL,
                buyer TEXT NOT NULL,
                token_addr TEXT NOT NULL,
                token_bought TEXT NOT NULL,
                fee_v TEXT NOT NULL,
                tax_v TEXT NOT NULL,
                spent_v_est TEXT NOT NULL,
                spent_v_actual TEXT,
                cost_v TEXT NOT NULL,
                total_supply TEXT NOT NULL,
                virtual_price_usd TEXT,
                breakeven_fdv_v TEXT NOT NULL,
                breakeven_fdv_usd TEXT,
                is_my_wallet INTEGER NOT NULL,
                anomaly INTEGER NOT NULL,
                is_price_stale INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(project, tx_hash, buyer, token_addr)
            );

            CREATE INDEX IF NOT EXISTS idx_events_project_time
                ON events(project, block_timestamp);

            CREATE TABLE IF NOT EXISTS wallet_positions (
                project TEXT NOT NULL,
                wallet TEXT NOT NULL,
                token_addr TEXT NOT NULL,
                sum_fee_v TEXT NOT NULL,
                sum_spent_v_est TEXT NOT NULL,
                sum_token_bought TEXT NOT NULL,
                avg_cost_v TEXT NOT NULL,
                total_supply TEXT NOT NULL,
                breakeven_fdv_v TEXT NOT NULL,
                virtual_price_usd TEXT,
                breakeven_fdv_usd TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(project, wallet, token_addr)
            );

            CREATE TABLE IF NOT EXISTS minute_agg (
                project TEXT NOT NULL,
                minute_key INTEGER NOT NULL,
                minute_spent_v TEXT NOT NULL,
                minute_fee_v TEXT NOT NULL,
                minute_tax_v TEXT NOT NULL,
                minute_buy_count INTEGER NOT NULL,
                minute_unique_buyers INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(project, minute_key)
            );

            CREATE TABLE IF NOT EXISTS minute_buyers (
                project TEXT NOT NULL,
                minute_key INTEGER NOT NULL,
                buyer TEXT NOT NULL,
                PRIMARY KEY(project, minute_key, buyer)
            );

            CREATE TABLE IF NOT EXISTS leaderboard (
                project TEXT NOT NULL,
                buyer TEXT NOT NULL,
                sum_spent_v_est TEXT NOT NULL,
                sum_token_bought TEXT NOT NULL,
                last_tx_time INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(project, buyer)
            );

            CREATE TABLE IF NOT EXISTS project_stats (
                project TEXT PRIMARY KEY,
                sum_tax_v TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS launch_configs (
                name TEXT PRIMARY KEY,
                internal_pool_addr TEXT NOT NULL,
                fee_addr TEXT NOT NULL,
                tax_addr TEXT NOT NULL,
                token_total_supply TEXT NOT NULL,
                fee_rate TEXT NOT NULL,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS monitored_wallets (
                wallet TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dead_letters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_hash TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload TEXT,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scanned_backfill_txs (
                project TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                scanned_at INTEGER NOT NULL,
                PRIMARY KEY(project, tx_hash)
            );
            """
        )
        self.conn.commit()

    def get_state(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM system_state WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set_state(self, key: str, value: str) -> None:
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO system_state(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
        self.conn.commit()

    def seed_launch_configs(self, launch_configs: List[LaunchConfig]) -> None:
        now = int(time.time())
        cur = self.conn.cursor()
        for lc in launch_configs:
            cur.execute(
                """
                INSERT OR IGNORE INTO launch_configs(
                    name, internal_pool_addr, fee_addr, tax_addr,
                    token_total_supply, fee_rate, is_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    lc.name,
                    lc.internal_pool_addr,
                    lc.fee_addr,
                    lc.tax_addr,
                    decimal_to_str(lc.token_total_supply, 0),
                    decimal_to_str(lc.fee_rate, 18),
                    now,
                    now,
                ),
            )
        self.conn.commit()

    def seed_monitored_wallets(self, wallets: Set[str]) -> None:
        if not wallets:
            return
        now = int(time.time())
        cur = self.conn.cursor()
        for wallet in sorted(wallets):
            cur.execute(
                """
                INSERT OR IGNORE INTO monitored_wallets(wallet, created_at, updated_at)
                VALUES (?, ?, ?)
                """,
                (normalize_address(wallet), now, now),
            )
        self.conn.commit()

    def list_launch_configs(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM launch_configs
            ORDER BY updated_at DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def list_monitored_wallets(self) -> List[str]:
        rows = self.conn.execute(
            """
            SELECT wallet
            FROM monitored_wallets
            ORDER BY updated_at DESC, wallet ASC
            """
        ).fetchall()
        return [normalize_address(str(r["wallet"])) for r in rows if r["wallet"]]

    def list_projects(self) -> List[str]:
        rows = self.conn.execute(
            """
            SELECT name AS project FROM launch_configs
            UNION
            SELECT project FROM events
            UNION
            SELECT project FROM minute_agg
            UNION
            SELECT project FROM leaderboard
            UNION
            SELECT project FROM wallet_positions
            ORDER BY project ASC
            """
        ).fetchall()
        return [str(r["project"]) for r in rows if r["project"]]

    def get_enabled_launch_configs(self) -> List[LaunchConfig]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM launch_configs
            WHERE is_enabled = 1
            ORDER BY name ASC
            """
        ).fetchall()
        result: List[LaunchConfig] = []
        for r in rows:
            result.append(
                LaunchConfig(
                    name=r["name"],
                    internal_pool_addr=normalize_address(r["internal_pool_addr"]),
                    fee_addr=normalize_address(r["fee_addr"]),
                    tax_addr=normalize_address(r["tax_addr"]),
                    token_total_supply=Decimal(str(r["token_total_supply"])),
                    fee_rate=Decimal(str(r["fee_rate"])),
                )
            )
        return result

    def get_launch_config_by_name(self, name: str) -> Optional[LaunchConfig]:
        row = self.conn.execute(
            """
            SELECT *
            FROM launch_configs
            WHERE name = ?
            """,
            (name,),
        ).fetchone()
        if not row:
            return None
        return LaunchConfig(
            name=row["name"],
            internal_pool_addr=normalize_address(row["internal_pool_addr"]),
            fee_addr=normalize_address(row["fee_addr"]),
            tax_addr=normalize_address(row["tax_addr"]),
            token_total_supply=Decimal(str(row["token_total_supply"])),
            fee_rate=Decimal(str(row["fee_rate"])),
        )

    def upsert_launch_config(
        self,
        *,
        name: str,
        internal_pool_addr: str,
        fee_addr: str,
        tax_addr: str,
        token_total_supply: Decimal,
        fee_rate: Decimal,
        is_enabled: bool = True,
    ) -> None:
        name = name.strip()
        if not name:
            raise ValueError("name cannot be empty")
        if fee_rate <= 0 or fee_rate >= 1:
            raise ValueError("fee_rate must be in (0,1)")
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO launch_configs(
                name, internal_pool_addr, fee_addr, tax_addr,
                token_total_supply, fee_rate, is_enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                internal_pool_addr = excluded.internal_pool_addr,
                fee_addr = excluded.fee_addr,
                tax_addr = excluded.tax_addr,
                token_total_supply = excluded.token_total_supply,
                fee_rate = excluded.fee_rate,
                is_enabled = excluded.is_enabled,
                updated_at = excluded.updated_at
            """,
            (
                name,
                normalize_address(internal_pool_addr),
                normalize_address(fee_addr),
                normalize_address(tax_addr),
                decimal_to_str(token_total_supply, 0),
                decimal_to_str(fee_rate, 18),
                1 if is_enabled else 0,
                now,
                now,
            ),
        )
        self.conn.commit()

    def delete_launch_config(self, name: str) -> bool:
        name = name.strip()
        if not name:
            raise ValueError("name cannot be empty")
        cur = self.conn.execute(
            """
            DELETE FROM launch_configs
            WHERE name = ?
            """,
            (name,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def set_launch_config_enabled_only(self, name: str) -> None:
        self.conn.execute(
            """
            UPDATE launch_configs
            SET is_enabled = CASE WHEN name = ? THEN 1 ELSE 0 END,
                updated_at = ?
            """,
            (name, int(time.time())),
        )
        self.conn.commit()

    def add_monitored_wallet(self, wallet: str) -> None:
        now = int(time.time())
        normalized = normalize_address(wallet)
        self.conn.execute(
            """
            INSERT INTO monitored_wallets(wallet, created_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                updated_at = excluded.updated_at
            """,
            (normalized, now, now),
        )
        self.conn.commit()

    def delete_monitored_wallet(self, wallet: str) -> bool:
        normalized = normalize_address(wallet)
        cur = self.conn.execute(
            """
            DELETE FROM monitored_wallets
            WHERE wallet = ?
            """,
            (normalized,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def _event_tuple(self, event: Dict[str, Any]) -> Tuple[Any, ...]:
        return (
            event["project"],
            event["tx_hash"],
            event["block_number"],
            event["block_timestamp"],
            event["internal_pool"],
            event["fee_addr"],
            event["tax_addr"],
            event["buyer"],
            event["token_addr"],
            decimal_to_str(event["token_bought"], 18),
            decimal_to_str(event["fee_v"], 18),
            decimal_to_str(event["tax_v"], 18),
            decimal_to_str(event["spent_v_est"], 18),
            decimal_to_str(event["spent_v_actual"], 18)
            if event.get("spent_v_actual") is not None
            else None,
            decimal_to_str(event["cost_v"], 18),
            decimal_to_str(event["total_supply"], 0),
            decimal_to_str(event.get("virtual_price_usd"), 18)
            if event.get("virtual_price_usd") is not None
            else None,
            decimal_to_str(event["breakeven_fdv_v"], 18),
            decimal_to_str(event.get("breakeven_fdv_usd"), 18)
            if event.get("breakeven_fdv_usd") is not None
            else None,
            1 if event["is_my_wallet"] else 0,
            1 if event["anomaly"] else 0,
            1 if event["is_price_stale"] else 0,
            int(time.time()),
        )

    def save_dead_letter(self, tx_hash: str, reason: str, payload: Optional[Dict[str, Any]]) -> None:
        self.conn.execute(
            """
            INSERT INTO dead_letters(tx_hash, reason, payload, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (tx_hash, reason, json.dumps(payload, ensure_ascii=False) if payload else None, int(time.time())),
        )
        self.conn.commit()

    def get_known_backfill_txs(self, project: str, tx_hashes: List[str]) -> Set[str]:
        project = str(project).strip()
        if not project or not tx_hashes:
            return set()
        unique_hashes = sorted({str(x).lower() for x in tx_hashes if x})
        if not unique_hashes:
            return set()

        found: Set[str] = set()
        chunk_size = 400
        for i in range(0, len(unique_hashes), chunk_size):
            chunk = unique_hashes[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = f"""
                SELECT tx_hash
                FROM scanned_backfill_txs
                WHERE project = ? AND tx_hash IN ({placeholders})
                UNION
                SELECT tx_hash
                FROM events
                WHERE project = ? AND tx_hash IN ({placeholders})
            """
            params = [project, *chunk, project, *chunk]
            rows = self.conn.execute(sql, params).fetchall()
            found.update(str(r["tx_hash"]).lower() for r in rows if r["tx_hash"])
        return found

    def mark_backfill_scanned_txs(self, project: str, tx_hashes: List[str]) -> None:
        project = str(project).strip()
        if not project or not tx_hashes:
            return
        now = int(time.time())
        rows = [
            (project, str(x).lower(), now)
            for x in tx_hashes
            if x
        ]
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO scanned_backfill_txs(project, tx_hash, scanned_at)
            VALUES (?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def flush_events(self, events: List[Dict[str, Any]], max_block: int) -> List[Dict[str, Any]]:
        if not events and max_block <= 0:
            return []

        inserted_events: List[Dict[str, Any]] = []
        wallet_deltas: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        minute_deltas: Dict[Tuple[str, int], Dict[str, Any]] = {}
        minute_buyers: Set[Tuple[str, int, str]] = set()
        leaderboard_deltas: Dict[Tuple[str, str], Dict[str, Any]] = {}
        project_tax_deltas: Dict[str, Decimal] = {}

        cur = self.conn.cursor()
        cur.execute("BEGIN")
        try:
            for e in events:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO events(
                        project, tx_hash, block_number, block_timestamp,
                        internal_pool, fee_addr, tax_addr, buyer, token_addr,
                        token_bought, fee_v, tax_v, spent_v_est, spent_v_actual,
                        cost_v, total_supply, virtual_price_usd, breakeven_fdv_v,
                        breakeven_fdv_usd, is_my_wallet, anomaly, is_price_stale, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._event_tuple(e),
                )
                if cur.rowcount != 1:
                    continue

                inserted_events.append(e)

                mkey = (e["project"], int(e["block_timestamp"] // 60 * 60))
                md = minute_deltas.setdefault(
                    mkey,
                    {
                        "spent": Decimal(0),
                        "fee": Decimal(0),
                        "tax": Decimal(0),
                        "buy_count": 0,
                        "unique_inc": 0,
                    },
                )
                md["spent"] += e["spent_v_est"]
                md["fee"] += e["fee_v"]
                md["tax"] += e["tax_v"]
                md["buy_count"] += 1
                minute_buyers.add((e["project"], mkey[1], e["buyer"]))

                lkey = (e["project"], e["buyer"])
                ld = leaderboard_deltas.setdefault(
                    lkey,
                    {"spent": Decimal(0), "token": Decimal(0), "last_tx_time": 0},
                )
                ld["spent"] += e["spent_v_est"]
                ld["token"] += e["token_bought"]
                ld["last_tx_time"] = max(ld["last_tx_time"], int(e["block_timestamp"]))
                project_tax_deltas[e["project"]] = (
                    project_tax_deltas.get(e["project"], Decimal(0)) + e["tax_v"]
                )

                if e["is_my_wallet"]:
                    wkey = (e["project"], e["buyer"], e["token_addr"])
                    wd = wallet_deltas.setdefault(
                        wkey,
                        {
                            "fee": Decimal(0),
                            "spent": Decimal(0),
                            "token": Decimal(0),
                            "total_supply": e["total_supply"],
                            "virtual_price_usd": e.get("virtual_price_usd"),
                        },
                    )
                    wd["fee"] += e["fee_v"]
                    wd["spent"] += e["spent_v_est"]
                    wd["token"] += e["token_bought"]
                    wd["total_supply"] = e["total_supply"]
                    if e.get("virtual_price_usd") is not None:
                        wd["virtual_price_usd"] = e["virtual_price_usd"]

            for project, minute_key, buyer in minute_buyers:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO minute_buyers(project, minute_key, buyer)
                    VALUES (?, ?, ?)
                    """,
                    (project, minute_key, buyer),
                )
                if cur.rowcount == 1:
                    minute_deltas[(project, minute_key)]["unique_inc"] += 1

            for (project, wallet, token_addr), d in wallet_deltas.items():
                row = cur.execute(
                    """
                    SELECT sum_fee_v, sum_spent_v_est, sum_token_bought, total_supply, virtual_price_usd
                    FROM wallet_positions
                    WHERE project = ? AND wallet = ? AND token_addr = ?
                    """,
                    (project, wallet, token_addr),
                ).fetchone()
                old_fee = Decimal(row["sum_fee_v"]) if row else Decimal(0)
                old_spent = Decimal(row["sum_spent_v_est"]) if row else Decimal(0)
                old_token = Decimal(row["sum_token_bought"]) if row else Decimal(0)
                old_price = Decimal(row["virtual_price_usd"]) if row and row["virtual_price_usd"] else None

                new_fee = old_fee + d["fee"]
                new_spent = old_spent + d["spent"]
                new_token = old_token + d["token"]
                avg_cost = (new_spent / new_token) if new_token > 0 else Decimal(0)
                total_supply = d["total_supply"] if d["total_supply"] else (Decimal(row["total_supply"]) if row else Decimal(0))
                fdv_v = avg_cost * total_supply if total_supply else Decimal(0)
                price = d["virtual_price_usd"] if d["virtual_price_usd"] is not None else old_price
                fdv_usd = (fdv_v * price) if price is not None else None

                cur.execute(
                    """
                    INSERT INTO wallet_positions(
                        project, wallet, token_addr, sum_fee_v, sum_spent_v_est, sum_token_bought,
                        avg_cost_v, total_supply, breakeven_fdv_v, virtual_price_usd, breakeven_fdv_usd, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(project, wallet, token_addr) DO UPDATE SET
                        sum_fee_v = excluded.sum_fee_v,
                        sum_spent_v_est = excluded.sum_spent_v_est,
                        sum_token_bought = excluded.sum_token_bought,
                        avg_cost_v = excluded.avg_cost_v,
                        total_supply = excluded.total_supply,
                        breakeven_fdv_v = excluded.breakeven_fdv_v,
                        virtual_price_usd = excluded.virtual_price_usd,
                        breakeven_fdv_usd = excluded.breakeven_fdv_usd,
                        updated_at = excluded.updated_at
                    """,
                    (
                        project,
                        wallet,
                        token_addr,
                        decimal_to_str(new_fee, 18),
                        decimal_to_str(new_spent, 18),
                        decimal_to_str(new_token, 18),
                        decimal_to_str(avg_cost, 18),
                        decimal_to_str(total_supply, 0),
                        decimal_to_str(fdv_v, 18),
                        decimal_to_str(price, 18) if price is not None else None,
                        decimal_to_str(fdv_usd, 18) if fdv_usd is not None else None,
                        int(time.time()),
                    ),
                )

            for (project, minute_key), d in minute_deltas.items():
                row = cur.execute(
                    """
                    SELECT minute_spent_v, minute_fee_v, minute_tax_v, minute_buy_count, minute_unique_buyers
                    FROM minute_agg
                    WHERE project = ? AND minute_key = ?
                    """,
                    (project, minute_key),
                ).fetchone()
                old_spent = Decimal(row["minute_spent_v"]) if row else Decimal(0)
                old_fee = Decimal(row["minute_fee_v"]) if row else Decimal(0)
                old_tax = Decimal(row["minute_tax_v"]) if row else Decimal(0)
                old_count = int(row["minute_buy_count"]) if row else 0
                old_unique = int(row["minute_unique_buyers"]) if row else 0
                cur.execute(
                    """
                    INSERT INTO minute_agg(
                        project, minute_key, minute_spent_v, minute_fee_v, minute_tax_v,
                        minute_buy_count, minute_unique_buyers, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(project, minute_key) DO UPDATE SET
                        minute_spent_v = excluded.minute_spent_v,
                        minute_fee_v = excluded.minute_fee_v,
                        minute_tax_v = excluded.minute_tax_v,
                        minute_buy_count = excluded.minute_buy_count,
                        minute_unique_buyers = excluded.minute_unique_buyers,
                        updated_at = excluded.updated_at
                    """,
                    (
                        project,
                        minute_key,
                        decimal_to_str(old_spent + d["spent"], 18),
                        decimal_to_str(old_fee + d["fee"], 18),
                        decimal_to_str(old_tax + d["tax"], 18),
                        old_count + d["buy_count"],
                        old_unique + d["unique_inc"],
                        int(time.time()),
                    ),
                )

            for (project, buyer), d in leaderboard_deltas.items():
                row = cur.execute(
                    """
                    SELECT sum_spent_v_est, sum_token_bought, last_tx_time
                    FROM leaderboard
                    WHERE project = ? AND buyer = ?
                    """,
                    (project, buyer),
                ).fetchone()
                old_spent = Decimal(row["sum_spent_v_est"]) if row else Decimal(0)
                old_token = Decimal(row["sum_token_bought"]) if row else Decimal(0)
                old_last = int(row["last_tx_time"]) if row else 0
                cur.execute(
                    """
                    INSERT INTO leaderboard(
                        project, buyer, sum_spent_v_est, sum_token_bought, last_tx_time, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(project, buyer) DO UPDATE SET
                        sum_spent_v_est = excluded.sum_spent_v_est,
                        sum_token_bought = excluded.sum_token_bought,
                        last_tx_time = excluded.last_tx_time,
                        updated_at = excluded.updated_at
                    """,
                    (
                        project,
                        buyer,
                        decimal_to_str(old_spent + d["spent"], 18),
                        decimal_to_str(old_token + d["token"], 18),
                        max(old_last, d["last_tx_time"]),
                        int(time.time()),
                    ),
                )

            for project, tax_delta in project_tax_deltas.items():
                row = cur.execute(
                    """
                    SELECT sum_tax_v
                    FROM project_stats
                    WHERE project = ?
                    """,
                    (project,),
                ).fetchone()
                old_tax = Decimal(row["sum_tax_v"]) if row else Decimal(0)
                cur.execute(
                    """
                    INSERT INTO project_stats(project, sum_tax_v, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(project) DO UPDATE SET
                        sum_tax_v = excluded.sum_tax_v,
                        updated_at = excluded.updated_at
                    """,
                    (
                        project,
                        decimal_to_str(old_tax + tax_delta, 18),
                        int(time.time()),
                    ),
                )

            if max_block > 0:
                old = self.get_state("last_processed_block")
                old_b = int(old) if old else 0
                if max_block > old_b:
                    self.set_state("last_processed_block", str(max_block))

            self.conn.commit()
            return inserted_events
        except Exception:
            self.conn.rollback()
            raise

    def query_wallets(
        self,
        wallet: Optional[str] = None,
        project: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if wallet and project:
            rows = self.conn.execute(
                """
                SELECT * FROM wallet_positions
                WHERE wallet = ? AND project = ?
                ORDER BY updated_at DESC
                """,
                (wallet, project),
            ).fetchall()
        elif wallet:
            rows = self.conn.execute(
                """
                SELECT * FROM wallet_positions
                WHERE wallet = ?
                ORDER BY updated_at DESC
                """,
                (wallet,),
            ).fetchall()
        elif project:
            rows = self.conn.execute(
                """
                SELECT * FROM wallet_positions
                WHERE project = ?
                ORDER BY updated_at DESC
                """,
                (project,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM wallet_positions
                ORDER BY updated_at DESC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def query_minutes(self, project: str, from_ts: int, to_ts: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM minute_agg
            WHERE project = ? AND minute_key >= ? AND minute_key <= ?
            ORDER BY minute_key ASC
            """,
            (project, from_ts, to_ts),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_leaderboard(self, project: str, top_n: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM leaderboard
            WHERE project = ?
            ORDER BY CAST(sum_token_bought AS REAL) DESC, CAST(sum_spent_v_est AS REAL) DESC
            LIMIT ?
            """,
            (project, top_n),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_event_delays(self, project: str, limit_n: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
                project,
                tx_hash,
                MIN(block_timestamp) AS block_timestamp,
                MIN(created_at) AS recorded_at,
                CAST(MIN(created_at) - MIN(block_timestamp) AS INTEGER) AS delay_sec
            FROM events
            WHERE project = ?
            GROUP BY project, tx_hash
            ORDER BY recorded_at DESC
            LIMIT ?
            """,
            (project, limit_n),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_project_tax(self, project: str) -> Dict[str, Any]:
        project = str(project).strip()
        if not project:
            raise ValueError("project is required")
        row = self.conn.execute(
            """
            SELECT project, sum_tax_v, updated_at
            FROM project_stats
            WHERE project = ?
            """,
            (project,),
        ).fetchone()
        if row:
            return dict(row)

        # Backward compatibility: build initial total from historical minute_agg once.
        rows = self.conn.execute(
            """
            SELECT minute_tax_v
            FROM minute_agg
            WHERE project = ?
            """,
            (project,),
        ).fetchall()
        total_tax = Decimal(0)
        for r in rows:
            if r["minute_tax_v"] is not None:
                total_tax += Decimal(str(r["minute_tax_v"]))
        now_ts = int(time.time())
        total_tax_str = decimal_to_str(total_tax, 18)
        self.conn.execute(
            """
            INSERT INTO project_stats(project, sum_tax_v, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(project) DO UPDATE SET
                sum_tax_v = excluded.sum_tax_v,
                updated_at = excluded.updated_at
            """,
            (project, total_tax_str, now_ts),
        )
        self.conn.commit()
        return {"project": project, "sum_tax_v": total_tax_str, "updated_at": now_ts}

    def rebuild_wallet_position_for_project_wallet(self, project: str, wallet: str) -> Dict[str, Any]:
        project = str(project).strip()
        wallet = normalize_address(wallet)
        if not project:
            raise ValueError("project is required")

        rows = self.conn.execute(
            """
            SELECT
                token_addr,
                fee_v,
                spent_v_est,
                token_bought,
                total_supply,
                virtual_price_usd,
                block_timestamp,
                created_at
            FROM events
            WHERE project = ? AND buyer = ?
            ORDER BY block_timestamp ASC, created_at ASC
            """,
            (project, wallet),
        ).fetchall()

        token_deltas: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            token_addr = normalize_address(str(r["token_addr"]))
            d = token_deltas.setdefault(
                token_addr,
                {
                    "sum_fee_v": Decimal(0),
                    "sum_spent_v_est": Decimal(0),
                    "sum_token_bought": Decimal(0),
                    "total_supply": Decimal(0),
                    "virtual_price_usd": None,
                },
            )
            d["sum_fee_v"] += Decimal(str(r["fee_v"]))
            d["sum_spent_v_est"] += Decimal(str(r["spent_v_est"]))
            d["sum_token_bought"] += Decimal(str(r["token_bought"]))
            d["total_supply"] = Decimal(str(r["total_supply"]))
            if r["virtual_price_usd"] is not None:
                d["virtual_price_usd"] = Decimal(str(r["virtual_price_usd"]))

        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("BEGIN")
        try:
            cur.execute(
                """
                DELETE FROM wallet_positions
                WHERE project = ? AND wallet = ?
                """,
                (project, wallet),
            )

            inserted = 0
            for token_addr, d in token_deltas.items():
                sum_fee_v = d["sum_fee_v"]
                sum_spent_v_est = d["sum_spent_v_est"]
                sum_token_bought = d["sum_token_bought"]
                total_supply = d["total_supply"]
                virtual_price_usd = d["virtual_price_usd"]
                avg_cost_v = (sum_spent_v_est / sum_token_bought) if sum_token_bought > 0 else Decimal(0)
                breakeven_fdv_v = avg_cost_v * total_supply if total_supply > 0 else Decimal(0)
                breakeven_fdv_usd = (
                    breakeven_fdv_v * virtual_price_usd if virtual_price_usd is not None else None
                )
                cur.execute(
                    """
                    INSERT INTO wallet_positions(
                        project, wallet, token_addr, sum_fee_v, sum_spent_v_est, sum_token_bought,
                        avg_cost_v, total_supply, breakeven_fdv_v, virtual_price_usd, breakeven_fdv_usd, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project,
                        wallet,
                        token_addr,
                        decimal_to_str(sum_fee_v, 18),
                        decimal_to_str(sum_spent_v_est, 18),
                        decimal_to_str(sum_token_bought, 18),
                        decimal_to_str(avg_cost_v, 18),
                        decimal_to_str(total_supply, 0),
                        decimal_to_str(breakeven_fdv_v, 18),
                        decimal_to_str(virtual_price_usd, 18) if virtual_price_usd is not None else None,
                        decimal_to_str(breakeven_fdv_usd, 18) if breakeven_fdv_usd is not None else None,
                        now,
                    ),
                )
                inserted += 1

            self.conn.commit()
            return {
                "project": project,
                "wallet": wallet,
                "eventCount": len(rows),
                "tokenCount": inserted,
            }
        except Exception:
            self.conn.rollback()
            raise

    def count_events(self, project: Optional[str] = None) -> int:
        if project:
            row = self.conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM events
                WHERE project = ?
                """,
                (project,),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM events
                """
            ).fetchone()
        return int(row["c"]) if row else 0


class EventBusStorage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS event_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_event_queue_id ON event_queue(id);

            CREATE TABLE IF NOT EXISTS role_heartbeats (
                role TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scan_jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                project TEXT,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                finished_at INTEGER,
                error TEXT,
                from_block INTEGER,
                to_block INTEGER,
                total_chunks INTEGER,
                processed_chunks INTEGER,
                scanned_tx INTEGER NOT NULL DEFAULT 0,
                skipped_tx INTEGER NOT NULL DEFAULT 0,
                processed_tx INTEGER NOT NULL DEFAULT 0,
                parsed_delta INTEGER NOT NULL DEFAULT 0,
                inserted_delta INTEGER NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                cancel_requested_at INTEGER,
                current_block INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_scan_jobs_status_created
                ON scan_jobs(status, created_at);
            """
        )
        # Recover unfinished manual scan jobs after restart.
        cur.execute(
            """
            UPDATE scan_jobs
            SET status = 'queued',
                started_at = NULL,
                error = 'requeued after restart'
            WHERE status = 'running'
            """
        )
        self.conn.commit()

    def enqueue_events(self, source: str, events: List[Dict[str, Any]]) -> int:
        if not events:
            return 0
        now = int(time.time())
        rows = []
        for event in events:
            payload = json.dumps(serialize_event_for_bus(event), ensure_ascii=False)
            rows.append(
                (
                    source,
                    str(event.get("tx_hash", "")).lower(),
                    int(event.get("block_number", 0)),
                    payload,
                    now,
                )
            )
        self.conn.executemany(
            """
            INSERT INTO event_queue(source, tx_hash, block_number, payload, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def fetch_events(self, limit_n: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT id, payload, block_number
            FROM event_queue
            ORDER BY id ASC
            LIMIT ?
            """,
            (max(1, int(limit_n)),),
        ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            payload = json.loads(str(row["payload"]))
            result.append(
                {
                    "id": int(row["id"]),
                    "block_number": int(row["block_number"]),
                    "event": deserialize_event_from_bus(payload),
                }
            )
        return result

    def ack_events(self, ids: List[int]) -> None:
        if not ids:
            return
        unique_ids = sorted({int(x) for x in ids})
        placeholders = ",".join("?" for _ in unique_ids)
        self.conn.execute(
            f"DELETE FROM event_queue WHERE id IN ({placeholders})",
            unique_ids,
        )
        self.conn.commit()

    def queue_size(self) -> int:
        row = self.conn.execute("SELECT COUNT(1) AS c FROM event_queue").fetchone()
        return int(row["c"]) if row else 0

    def upsert_role_heartbeat(self, role: str, payload: Dict[str, Any]) -> None:
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO role_heartbeats(role, payload, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(role) DO UPDATE SET
                payload = excluded.payload,
                updated_at = excluded.updated_at
            """,
            (role, json.dumps(payload, ensure_ascii=False), now),
        )
        self.conn.commit()

    def get_role_heartbeat(self, role: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT role, payload, updated_at
            FROM role_heartbeats
            WHERE role = ?
            """,
            (role,),
        ).fetchone()
        if not row:
            return None
        payload: Dict[str, Any]
        try:
            payload = json.loads(str(row["payload"]))
        except Exception:
            payload = {}
        return {
            "role": str(row["role"]),
            "payload": payload,
            "updated_at": int(row["updated_at"]),
        }

    def create_scan_job(self, project: Optional[str], start_ts: int, end_ts: int) -> str:
        now = int(time.time())
        job_id = uuid.uuid4().hex[:12]
        self.conn.execute(
            """
            INSERT INTO scan_jobs(
                id, status, project, start_ts, end_ts, created_at,
                cancel_requested, scanned_tx, skipped_tx, processed_tx, parsed_delta, inserted_delta
            ) VALUES (?, 'queued', ?, ?, ?, ?, 0, 0, 0, 0, 0, 0)
            """,
            (job_id, project, int(start_ts), int(end_ts), now),
        )
        self.conn.commit()
        return job_id

    def _scan_job_row_to_api(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": str(row["id"]),
            "status": str(row["status"]),
            "project": row["project"],
            "startTs": int(row["start_ts"]),
            "endTs": int(row["end_ts"]),
            "createdAt": int(row["created_at"]),
            "startedAt": int(row["started_at"]) if row["started_at"] is not None else None,
            "finishedAt": int(row["finished_at"]) if row["finished_at"] is not None else None,
            "error": row["error"],
            "fromBlock": int(row["from_block"]) if row["from_block"] is not None else None,
            "toBlock": int(row["to_block"]) if row["to_block"] is not None else None,
            "totalChunks": int(row["total_chunks"]) if row["total_chunks"] is not None else 0,
            "processedChunks": int(row["processed_chunks"]) if row["processed_chunks"] is not None else 0,
            "scannedTx": int(row["scanned_tx"]),
            "skippedTx": int(row["skipped_tx"]),
            "processedTx": int(row["processed_tx"]),
            "parsedDelta": int(row["parsed_delta"]),
            "insertedDelta": int(row["inserted_delta"]),
            "cancelRequested": bool(row["cancel_requested"]),
            "cancelRequestedAt": int(row["cancel_requested_at"]) if row["cancel_requested_at"] is not None else None,
            "currentBlock": int(row["current_block"]) if row["current_block"] is not None else None,
        }

    def get_scan_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT *
            FROM scan_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if not row:
            return None
        return self._scan_job_row_to_api(row)

    def request_scan_job_cancel(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT *
            FROM scan_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if not row:
            return None
        status = str(row["status"])
        now = int(time.time())
        if status in {"done", "failed", "canceled"}:
            return self._scan_job_row_to_api(row)

        if status == "queued":
            self.conn.execute(
                """
                UPDATE scan_jobs
                SET status = 'canceled',
                    cancel_requested = 1,
                    cancel_requested_at = ?,
                    finished_at = ?,
                    error = 'canceled by user'
                WHERE id = ?
                """,
                (now, now, job_id),
            )
        else:
            self.conn.execute(
                """
                UPDATE scan_jobs
                SET cancel_requested = 1,
                    cancel_requested_at = ?
                WHERE id = ?
                """,
                (now, job_id),
            )
        self.conn.commit()
        return self.get_scan_job(job_id)

    def claim_next_scan_job(self) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        row = cur.execute(
            """
            SELECT id
            FROM scan_jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            self.conn.rollback()
            return None
        job_id = str(row["id"])
        now = int(time.time())
        cur.execute(
            """
            UPDATE scan_jobs
            SET status = 'running',
                started_at = ?,
                error = NULL
            WHERE id = ? AND status = 'queued'
            """,
            (now, job_id),
        )
        if cur.rowcount != 1:
            self.conn.rollback()
            return None
        row2 = cur.execute(
            """
            SELECT *
            FROM scan_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        self.conn.commit()
        if not row2:
            return None
        return self._scan_job_row_to_api(row2)

    def update_scan_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        cols = []
        vals: List[Any] = []
        for key, value in fields.items():
            cols.append(f"{key} = ?")
            vals.append(value)
        vals.append(job_id)
        sql = f"UPDATE scan_jobs SET {', '.join(cols)} WHERE id = ?"
        self.conn.execute(sql, vals)
        self.conn.commit()

    def is_scan_job_cancel_requested(self, job_id: str) -> bool:
        row = self.conn.execute(
            """
            SELECT cancel_requested
            FROM scan_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if not row:
            return False
        return bool(row["cancel_requested"])

    def count_scan_jobs(self, only_active: bool = False) -> int:
        if only_active:
            row = self.conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM scan_jobs
                WHERE status IN ('queued', 'running')
                """
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM scan_jobs
                """
            ).fetchone()
        return int(row["c"]) if row else 0


class VirtualsBot:
    def __init__(self, cfg: AppConfig, role: str = "all"):
        if role not in {"all", "writer", "realtime", "backfill"}:
            raise ValueError(f"invalid role: {role}")
        self.cfg = cfg
        self.role = role
        self.is_writer_role = role in {"all", "writer"}
        self.is_realtime_role = role in {"all", "realtime"}
        self.is_backfill_role = role in {"all", "backfill"}
        self.emit_events_to_bus = role in {"realtime", "backfill"}
        self.consume_events_from_bus = role == "writer"
        self.enable_api = role in {"all", "writer"}
        self.cors_allow_origins = {
            str(x).strip().rstrip("/") for x in cfg.cors_allow_origins if str(x).strip()
        }
        template = cfg.launch_configs[0]
        self.fixed_fee_addr = template.fee_addr
        self.fixed_tax_addr = template.tax_addr
        self.fixed_token_total_supply = template.token_total_supply
        self.fixed_fee_rate = template.fee_rate
        self.base_dir = Path(__file__).resolve().parent
        self.storage = Storage(cfg.sqlite_path)
        runtime_db_batch_size = self.storage.get_state("runtime_db_batch_size")
        if runtime_db_batch_size:
            with contextlib.suppress(Exception):
                cfg.db_batch_size = max(1, int(runtime_db_batch_size))
        self.storage.seed_launch_configs(cfg.launch_configs)
        if not self.storage.get_state("my_wallets_seeded"):
            self.storage.seed_monitored_wallets(cfg.my_wallets)
            self.storage.set_state("my_wallets_seeded", "1")
        self.event_bus = EventBusStorage(cfg.event_bus_sqlite_path)
        self.launch_configs: List[LaunchConfig] = []
        self.my_wallets: Set[str] = set()
        self.reload_launch_configs()
        self.reload_my_wallets()
        if not self.storage.get_state("launch_configs_rev"):
            self.storage.set_state("launch_configs_rev", str(int(time.time())))
        if not self.storage.get_state("my_wallets_rev"):
            self.storage.set_state("my_wallets_rev", str(int(time.time())))
        if self.storage.get_state("runtime_bootstrap_v2") != "1":
            self.storage.set_state("runtime_paused", "1")
            self.storage.set_state("runtime_bootstrap_v2", "1")
        elif self.storage.get_state("runtime_paused") is None:
            self.storage.set_state("runtime_paused", "1")
        self.ws_reconnect_event = asyncio.Event()
        self.http_rpc = RPCClient(cfg.http_rpc_url, max_retries=cfg.max_rpc_retries)
        if cfg.backfill_http_rpc_url:
            self.backfill_http_rpc = RPCClient(
                cfg.backfill_http_rpc_url, max_retries=cfg.max_rpc_retries
            )
        else:
            self.backfill_http_rpc = self.http_rpc
        self.backfill_rpc_separate = self.backfill_http_rpc is not self.http_rpc
        self.ws_timeout = aiohttp.ClientTimeout(total=None)
        self.runtime_ui_heartbeat_timeout_sec = 2 * 60 * 60
        self.runtime_manual_paused = True
        self.runtime_ui_last_seen_at = 0
        self.runtime_ui_online = False
        self.runtime_paused = True
        self.runtime_pause_updated_at = int(time.time())
        self._last_runtime_pause_check_ts = 0.0
        self.refresh_runtime_pause_state(force=True)
        self.price_service = PriceService(
            cfg,
            self.http_rpc,
            is_paused=lambda: self.runtime_paused,
        )
        self.queue: asyncio.Queue[Tuple[str, int, bool]] = asyncio.Queue(maxsize=10000)
        self.pending_txs: Set[str] = set()
        self.stop_event = asyncio.Event()
        self.tasks: List[asyncio.Task] = []
        self.flush_lock = asyncio.Lock()
        self.wallet_recalc_lock = asyncio.Lock()
        self.pending_events: List[Dict[str, Any]] = []
        self.pending_max_block = 0
        self.decimals_cache: Dict[str, int] = {}
        self.block_ts_cache: Dict[int, int] = {}
        self.scan_jobs: Dict[str, Dict[str, Any]] = {}
        self.scan_lock = asyncio.Lock()
        self.stats: Dict[str, Any] = {
            "ws_connected": False,
            "enqueued_txs": 0,
            "processed_txs": 0,
            "parsed_events": 0,
            "inserted_events": 0,
            "rpc_errors": 0,
            "dead_letters": 0,
            "last_ws_block": 0,
            "last_backfill_block": 0,
            "last_flush_at": 0,
            "started_at": int(time.time()),
            "role": role,
        }
        self.last_launch_cfg_rev = self.storage.get_state("launch_configs_rev") or ""
        self.last_my_wallets_rev = self.storage.get_state("my_wallets_rev") or ""

        self.jsonl_file = None
        if self.is_writer_role:
            Path(cfg.jsonl_path).parent.mkdir(parents=True, exist_ok=True)
            self.jsonl_file = open(cfg.jsonl_path, "a", encoding="utf-8")

    def reload_launch_configs(self) -> None:
        launch_configs = self.storage.get_enabled_launch_configs()
        self.launch_configs = launch_configs

    def get_launch_configs(self) -> List[LaunchConfig]:
        return list(self.launch_configs)

    def reload_my_wallets(self) -> None:
        self.my_wallets = set(self.storage.list_monitored_wallets())

    def get_my_wallets(self) -> Set[str]:
        return set(self.my_wallets)

    def get_runtime_db_batch_size(self) -> int:
        return max(1, int(self.cfg.db_batch_size))

    def set_runtime_db_batch_size(self, value: int) -> int:
        v = max(1, int(value))
        self.cfg.db_batch_size = v
        self.storage.set_state("runtime_db_batch_size", str(v))
        return v

    def bump_launch_config_revision(self) -> None:
        rev = str(int(time.time()))
        self.storage.set_state("launch_configs_rev", rev)
        self.last_launch_cfg_rev = rev

    def bump_my_wallet_revision(self) -> None:
        rev = str(int(time.time()))
        self.storage.set_state("my_wallets_rev", rev)
        self.last_my_wallets_rev = rev

    def _load_runtime_ui_last_seen(self) -> int:
        raw = self.storage.get_state("runtime_ui_last_seen")
        if raw is None:
            return 0
        try:
            return max(0, int(str(raw)))
        except Exception:
            return 0

    def touch_runtime_ui_heartbeat(self) -> int:
        now = int(time.time())
        self.storage.set_state("runtime_ui_last_seen", str(now))
        self.runtime_ui_last_seen_at = now
        return now

    def get_runtime_paused(self) -> bool:
        return self.refresh_runtime_pause_state(force=True)

    def refresh_runtime_pause_state(self, force: bool = False) -> bool:
        now = time.time()
        if (not force) and (now - self._last_runtime_pause_check_ts < 1.0):
            return self.runtime_paused
        self._last_runtime_pause_check_ts = now
        prev = self.runtime_paused
        manual_paused = parse_bool_like(self.storage.get_state("runtime_paused"))
        ui_last_seen_at = self._load_runtime_ui_last_seen()
        ui_online = (ui_last_seen_at > 0) and (
            int(now) - ui_last_seen_at <= self.runtime_ui_heartbeat_timeout_sec
        )
        effective_paused = bool(manual_paused or (not ui_online))
        self.runtime_manual_paused = manual_paused
        self.runtime_ui_last_seen_at = ui_last_seen_at
        self.runtime_ui_online = ui_online
        self.runtime_paused = effective_paused
        if self.runtime_paused != prev:
            self.runtime_pause_updated_at = int(now)
            if self.is_realtime_role:
                self.ws_reconnect_event.set()
            if self.runtime_paused:
                self.stats["ws_connected"] = False
        return self.runtime_paused

    def set_runtime_paused(self, paused: bool) -> bool:
        value = "1" if bool(paused) else "0"
        self.storage.set_state("runtime_paused", value)
        if not paused:
            # Runtime must be kept alive by active dashboard heartbeat.
            self.touch_runtime_ui_heartbeat()
        return self.refresh_runtime_pause_state(force=True)

    def runtime_pause_payload(self) -> Dict[str, Any]:
        self.refresh_runtime_pause_state(force=True)
        return {
            "runtimePaused": bool(self.runtime_paused),
            "runtimeManualPaused": bool(self.runtime_manual_paused),
            "runtimeUiOnline": bool(self.runtime_ui_online),
            "runtimeUiLastSeenAt": (
                int(self.runtime_ui_last_seen_at) if self.runtime_ui_last_seen_at > 0 else None
            ),
            "runtimeUiHeartbeatTimeoutSec": int(self.runtime_ui_heartbeat_timeout_sec),
            "updatedAt": int(self.runtime_pause_updated_at),
        }

    async def wait_until_resumed(self) -> bool:
        while not self.stop_event.is_set():
            if not self.refresh_runtime_pause_state():
                return True
            await asyncio.sleep(0.5)
        return False

    async def launch_config_watch_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(2)
            latest = self.storage.get_state("launch_configs_rev") or ""
            if latest != self.last_launch_cfg_rev:
                self.last_launch_cfg_rev = latest
                self.reload_launch_configs()
                if self.is_realtime_role:
                    self.ws_reconnect_event.set()

    async def my_wallet_watch_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(2)
            latest = self.storage.get_state("my_wallets_rev") or ""
            if latest != self.last_my_wallets_rev:
                self.last_my_wallets_rev = latest
                self.reload_my_wallets()

    def write_inserted_events_jsonl(self, inserted: List[Dict[str, Any]]) -> None:
        if not inserted or not self.jsonl_file:
            return
        for e in inserted:
            self.jsonl_file.write(
                json.dumps(
                    {
                        "project": e["project"],
                        "txHash": e["tx_hash"],
                        "blockNumber": e["block_number"],
                        "timestamp": e["block_timestamp"],
                        "internalPool": e["internal_pool"],
                        "feeAddr": e["fee_addr"],
                        "taxAddr": e["tax_addr"],
                        "buyer": e["buyer"],
                        "tokenAddr": e["token_addr"],
                        "tokenBought": decimal_to_str(e["token_bought"], 18),
                        "feeV": decimal_to_str(e["fee_v"], 18),
                        "taxV": decimal_to_str(e["tax_v"], 18),
                        "spentV_est": decimal_to_str(e["spent_v_est"], 18),
                        "spentV_actual": decimal_to_str(e["spent_v_actual"], 18),
                        "costV": decimal_to_str(e["cost_v"], 18),
                        "totalSupply": decimal_to_str(e["total_supply"], 0),
                        "breakevenFDV_V": decimal_to_str(e["breakeven_fdv_v"], 18),
                        "virtualPriceUSD": decimal_to_str(e["virtual_price_usd"], 18)
                        if e.get("virtual_price_usd") is not None
                        else None,
                        "breakevenFDV_USD": decimal_to_str(e["breakeven_fdv_usd"], 18)
                        if e.get("breakeven_fdv_usd") is not None
                        else None,
                        "isMyWallet": e["is_my_wallet"],
                        "anomaly": e["anomaly"],
                        "isPriceStale": e["is_price_stale"],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        self.jsonl_file.flush()

    def persist_events_batch(self, events: List[Dict[str, Any]], max_block: int) -> int:
        inserted = self.storage.flush_events(events, max_block)
        self.stats["inserted_events"] += len(inserted)
        self.stats["last_flush_at"] = int(time.time())
        self.write_inserted_events_jsonl(inserted)
        return len(inserted)

    async def emit_parsed_events(self, events: List[Dict[str, Any]], block_number: int) -> None:
        if not events:
            return
        if self.emit_events_to_bus:
            self.event_bus.enqueue_events(self.role, events)
        else:
            async with self.flush_lock:
                self.pending_events.extend(events)
                self.pending_max_block = max(self.pending_max_block, block_number)
        self.stats["parsed_events"] += len(events)

    def build_heartbeat_payload(self) -> Dict[str, Any]:
        return {
            "role": self.role,
            "runtime_paused": bool(self.runtime_paused),
            "runtime_manual_paused": bool(self.runtime_manual_paused),
            "runtime_ui_online": bool(self.runtime_ui_online),
            "ws_connected": bool(self.stats.get("ws_connected", False)),
            "enqueued_txs": int(self.stats.get("enqueued_txs", 0)),
            "processed_txs": int(self.stats.get("processed_txs", 0)),
            "parsed_events": int(self.stats.get("parsed_events", 0)),
            "rpc_errors": int(self.stats.get("rpc_errors", 0)),
            "dead_letters": int(self.stats.get("dead_letters", 0)),
            "last_ws_block": int(self.stats.get("last_ws_block", 0)),
            "last_backfill_block": int(self.stats.get("last_backfill_block", 0)),
            "queue_size": int(self.queue.qsize()),
            "pending_txs": int(len(self.pending_txs)),
            "updated_at": int(time.time()),
        }

    async def role_heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.refresh_runtime_pause_state(force=True)
                self.event_bus.upsert_role_heartbeat(self.role, self.build_heartbeat_payload())
            except Exception:
                pass
            await asyncio.sleep(2)

    async def bus_writer_loop(self) -> None:
        while not self.stop_event.is_set():
            batch_size = max(1, int(self.cfg.db_batch_size))
            idle_sleep = max(0.05, self.cfg.db_flush_ms / 1000.0)
            rows = self.event_bus.fetch_events(batch_size)
            if not rows:
                await asyncio.sleep(idle_sleep)
                continue
            try:
                events = [x["event"] for x in rows]
                max_block = max((int(x.get("block_number", 0)) for x in events), default=0)
                self.stats["parsed_events"] += len(events)
                self.persist_events_batch(events, max_block)
                self.event_bus.ack_events([int(x["id"]) for x in rows])
            except Exception:
                self.stats["rpc_errors"] += 1
                await asyncio.sleep(0.5)

    async def scan_job_dispatch_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.refresh_runtime_pause_state():
                await asyncio.sleep(1)
                continue
            job = self.event_bus.claim_next_scan_job()
            if not job:
                await asyncio.sleep(1)
                continue
            await self.run_scan_range_job_bus(job)

    async def __aenter__(self) -> "VirtualsBot":
        await self.http_rpc.__aenter__()
        if self.backfill_rpc_separate:
            await self.backfill_http_rpc.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self.stop_event.is_set():
            await self.shutdown()
        await self.http_rpc.__aexit__(exc_type, exc, tb)
        if self.backfill_rpc_separate:
            await self.backfill_http_rpc.__aexit__(exc_type, exc, tb)
        self.storage.close()
        self.event_bus.close()
        if self.jsonl_file:
            self.jsonl_file.close()

    async def get_token_decimals(
        self, token_addr: str, rpc: Optional[RPCClient] = None
    ) -> int:
        token_addr = normalize_address(token_addr)
        cached = self.decimals_cache.get(token_addr)
        if cached is not None:
            return cached
        rpc_client = rpc or self.http_rpc
        out = await rpc_client.eth_call(token_addr, DECIMALS_SELECTOR)
        dec = int(out, 16)
        self.decimals_cache[token_addr] = dec
        return dec

    async def get_block_timestamp(
        self, block_number: int, rpc: Optional[RPCClient] = None
    ) -> int:
        cached = self.block_ts_cache.get(block_number)
        if cached is not None:
            return cached
        rpc_client = rpc or self.http_rpc
        block = await rpc_client.get_block_by_number(block_number)
        if not block:
            return int(time.time())
        ts = int(block["timestamp"], 16)
        self.block_ts_cache[block_number] = ts
        if len(self.block_ts_cache) > 5000:
            oldest = sorted(self.block_ts_cache.keys())[:1000]
            for b in oldest:
                self.block_ts_cache.pop(b, None)
        return ts

    def is_related_to_launch(self, receipt_logs: List[Dict[str, Any]], launch: LaunchConfig) -> bool:
        vaddr = self.cfg.virtual_token_addr
        for lg in receipt_logs:
            topics = lg.get("topics") or []
            if not topics:
                continue
            if topics[0].lower() != TRANSFER_TOPIC0:
                continue
            token = normalize_address(lg["address"])
            from_addr = decode_topic_address(topics[1]) if len(topics) > 1 else ""
            to_addr = decode_topic_address(topics[2]) if len(topics) > 2 else ""
            if token == vaddr and (
                from_addr == launch.internal_pool_addr
                or to_addr == launch.internal_pool_addr
                or to_addr == launch.fee_addr
                or to_addr == launch.tax_addr
            ):
                return True
            if from_addr == launch.internal_pool_addr or to_addr == launch.internal_pool_addr:
                return True
        return False

    async def parse_receipt_for_launch(
        self,
        launch: LaunchConfig,
        receipt: Dict[str, Any],
        timestamp: int,
        virtual_price_usd: Optional[Decimal],
        is_price_stale: bool,
        rpc: Optional[RPCClient] = None,
    ) -> List[Dict[str, Any]]:
        logs = receipt.get("logs", [])
        tx_hash = receipt["transactionHash"].lower()
        block_number = int(receipt["blockNumber"], 16)

        transfer_logs = []
        for idx, lg in enumerate(logs):
            topics = lg.get("topics") or []
            if not topics or topics[0].lower() != TRANSFER_TOPIC0 or len(topics) < 3:
                continue
            try:
                token_addr = normalize_address(lg["address"])
                from_addr = decode_topic_address(topics[1])
                to_addr = decode_topic_address(topics[2])
                amount_raw = parse_hex_int(lg.get("data"))
            except Exception:
                continue
            transfer_logs.append(
                {
                    "token_addr": token_addr,
                    "from": from_addr,
                    "to": to_addr,
                    "amount_raw": amount_raw,
                    "idx": idx,
                }
            )

        candidate_buyers: Set[str] = set()
        buyer_fee_raw: Dict[str, int] = defaultdict(int)
        buyer_tax_raw: Dict[str, int] = defaultdict(int)
        buyer_virtual_out_raw: Dict[str, int] = defaultdict(int)
        buyer_virtual_in_raw: Dict[str, int] = defaultdict(int)
        token_received_raw: Dict[Tuple[str, str], int] = defaultdict(int)
        token_received_first_idx: Dict[Tuple[str, str], int] = {}
        token_outgoing_logs: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

        vaddr = self.cfg.virtual_token_addr
        for lg in transfer_logs:
            token = lg["token_addr"]
            from_addr = lg["from"]
            to_addr = lg["to"]
            amount = lg["amount_raw"]
            if amount <= 0:
                continue

            token_outgoing_logs[(from_addr, token)].append(lg)

            if token == vaddr:
                if to_addr in {launch.fee_addr, launch.tax_addr, launch.internal_pool_addr}:
                    candidate_buyers.add(from_addr)
                if to_addr == launch.fee_addr:
                    buyer_fee_raw[from_addr] += amount
                if to_addr == launch.tax_addr:
                    buyer_tax_raw[from_addr] += amount
                buyer_virtual_out_raw[from_addr] += amount
                buyer_virtual_in_raw[to_addr] += amount

            if from_addr == launch.internal_pool_addr and amount > 0:
                token_received_raw[(to_addr, token)] += amount
                key = (to_addr, token)
                if key not in token_received_first_idx:
                    token_received_first_idx[key] = int(lg["idx"])

        virtual_decimals = await self.get_token_decimals(vaddr, rpc=rpc)

        events: List[Dict[str, Any]] = []
        for (buyer, token_addr), raw_amount in token_received_raw.items():
            if buyer not in candidate_buyers:
                continue

            # Keep strict swap validation, but attribute buyer to the final
            # destination of the token flow originating from internal pool.
            effective_buyer = buyer
            effective_raw_amount = raw_amount
            cur_idx = int(token_received_first_idx.get((buyer, token_addr), -1))
            visited: Set[str] = {launch.internal_pool_addr}
            for _ in range(8):
                outs = [
                    x
                    for x in token_outgoing_logs.get((effective_buyer, token_addr), [])
                    if int(x["idx"]) > cur_idx
                    and x["to"] != launch.internal_pool_addr
                    and int(x["amount_raw"]) > 0
                ]
                if not outs:
                    break
                nxt = max(outs, key=lambda x: (int(x["amount_raw"]), int(x["idx"])))
                nxt_to = str(nxt["to"])
                if nxt_to in visited:
                    break
                visited.add(nxt_to)
                effective_buyer = nxt_to
                cur_idx = int(nxt["idx"])
                effective_raw_amount = min(effective_raw_amount, int(nxt["amount_raw"]))

            token_decimals = await self.get_token_decimals(token_addr, rpc=rpc)
            token_bought = raw_to_decimal(effective_raw_amount, token_decimals)
            if token_bought <= 0:
                continue

            fee_raw = buyer_fee_raw.get(buyer, 0)
            tax_raw = buyer_tax_raw.get(buyer, 0)
            fee_v = raw_to_decimal(fee_raw, virtual_decimals)
            tax_v = raw_to_decimal(tax_raw, virtual_decimals)
            if launch.fee_rate <= 0:
                continue
            spent_v_est = fee_v / launch.fee_rate
            if spent_v_est <= 0:
                continue

            cost_v = spent_v_est / token_bought
            total_supply = launch.token_total_supply
            breakeven_fdv_v = cost_v * total_supply
            breakeven_fdv_usd = (
                breakeven_fdv_v * virtual_price_usd if virtual_price_usd is not None else None
            )

            virtual_out = raw_to_decimal(buyer_virtual_out_raw.get(buyer, 0), virtual_decimals)
            virtual_in = raw_to_decimal(buyer_virtual_in_raw.get(buyer, 0), virtual_decimals)
            spent_v_actual = virtual_out - virtual_in
            anomaly = False
            if spent_v_est > 0:
                gap = abs(spent_v_actual - spent_v_est) / spent_v_est
                anomaly = gap > Decimal("0.02")

            events.append(
                {
                    "project": launch.name,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                    "block_timestamp": timestamp,
                    "internal_pool": launch.internal_pool_addr,
                    "fee_addr": launch.fee_addr,
                    "tax_addr": launch.tax_addr,
                    "buyer": effective_buyer,
                    "token_addr": token_addr,
                    "token_bought": token_bought,
                    "fee_v": fee_v,
                    "tax_v": tax_v,
                    "spent_v_est": spent_v_est,
                    "spent_v_actual": spent_v_actual,
                    "cost_v": cost_v,
                    "total_supply": total_supply,
                    "virtual_price_usd": virtual_price_usd,
                    "breakeven_fdv_v": breakeven_fdv_v,
                    "breakeven_fdv_usd": breakeven_fdv_usd,
                    "is_my_wallet": effective_buyer in self.my_wallets,
                    "anomaly": anomaly,
                    "is_price_stale": is_price_stale,
                }
            )
        return events

    async def enqueue_tx(
        self, tx_hash: str, block_number: int, use_backfill_rpc: bool = False
    ) -> None:
        tx_hash = tx_hash.lower()
        if tx_hash in self.pending_txs:
            return
        self.pending_txs.add(tx_hash)
        await self.queue.put((tx_hash, block_number, use_backfill_rpc))
        self.stats["enqueued_txs"] += 1

    async def process_tx(
        self,
        tx_hash: str,
        hint_block: int,
        launch_configs: Optional[List[LaunchConfig]] = None,
        rpc: Optional[RPCClient] = None,
    ) -> None:
        try:
            rpc_client = rpc or self.http_rpc
            receipt = await rpc_client.get_receipt(tx_hash)
            if not receipt:
                return
            if receipt.get("status") and int(receipt["status"], 16) == 0:
                return

            block_number = int(receipt["blockNumber"], 16)
            timestamp = await self.get_block_timestamp(block_number, rpc=rpc_client)
            virtual_price_usd, is_price_stale = await self.price_service.get_price()

            all_events: List[Dict[str, Any]] = []
            logs = receipt.get("logs", [])
            active_launch_configs = launch_configs if launch_configs is not None else self.get_launch_configs()
            for launch in active_launch_configs:
                if not self.is_related_to_launch(logs, launch):
                    continue
                events = await self.parse_receipt_for_launch(
                    launch=launch,
                    receipt=receipt,
                    timestamp=timestamp,
                    virtual_price_usd=virtual_price_usd,
                    is_price_stale=is_price_stale,
                    rpc=rpc_client,
                )
                all_events.extend(events)

            if all_events:
                await self.emit_parsed_events(all_events, block_number)

            self.stats["processed_txs"] += 1
        except Exception as e:
            self.stats["rpc_errors"] += 1
            self.storage.save_dead_letter(
                tx_hash=tx_hash,
                reason=f"process_tx_failed: {type(e).__name__}: {e}",
                payload={"hint_block": hint_block},
            )
            self.stats["dead_letters"] += 1
        finally:
            self.pending_txs.discard(tx_hash)

    async def consumer_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.refresh_runtime_pause_state():
                await asyncio.sleep(0.5)
                continue
            tx_hash, block_number, use_backfill_rpc = await self.queue.get()
            try:
                rpc_client = self.backfill_http_rpc if use_backfill_rpc else self.http_rpc
                await self.process_tx(tx_hash, block_number, rpc=rpc_client)
            finally:
                self.queue.task_done()

    async def flush_loop(self) -> None:
        interval = max(0.1, self.cfg.db_flush_ms / 1000.0)
        while not self.stop_event.is_set():
            await asyncio.sleep(interval)
            await self.flush_once(force=False)

    async def flush_once(self, force: bool) -> None:
        async with self.flush_lock:
            if not self.pending_events and not force:
                return
            if (not force) and (len(self.pending_events) < self.cfg.db_batch_size):
                return
            events = self.pending_events
            max_block = self.pending_max_block
            self.pending_events = []
            self.pending_max_block = 0

        self.persist_events_batch(events, max_block)

    async def ws_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if self.refresh_runtime_pause_state():
                    self.stats["ws_connected"] = False
                    await asyncio.sleep(1)
                    continue
                launch_configs = self.get_launch_configs()
                if not launch_configs:
                    await asyncio.sleep(2)
                    continue

                async with aiohttp.ClientSession(timeout=self.ws_timeout) as session:
                    async with session.ws_connect(self.cfg.ws_rpc_url, heartbeat=20) as ws:
                        self.stats["ws_connected"] = True
                        self.storage.set_state("ws_last_reconnect_time", str(int(time.time())))
                        self.ws_reconnect_event.clear()

                        sub_filters: List[Dict[str, Any]] = []
                        vaddr = self.cfg.virtual_token_addr
                        for launch in launch_configs:
                            internal = topic_address(launch.internal_pool_addr)
                            fee = topic_address(launch.fee_addr)
                            tax = topic_address(launch.tax_addr)
                            sub_filters.append(
                                {
                                    "address": vaddr,
                                    "topics": [TRANSFER_TOPIC0, None, [internal, fee, tax]],
                                }
                            )
                            sub_filters.append(
                                {
                                    "address": vaddr,
                                    "topics": [TRANSFER_TOPIC0, internal],
                                }
                            )
                            sub_filters.append(
                                {
                                    "topics": [TRANSFER_TOPIC0, internal],
                                }
                            )
                            sub_filters.append(
                                {
                                    "topics": [TRANSFER_TOPIC0, None, internal],
                                }
                            )

                        for idx, f in enumerate(sub_filters, start=1):
                            await ws.send_json(
                                {
                                    "jsonrpc": "2.0",
                                    "id": idx,
                                    "method": "eth_subscribe",
                                    "params": ["logs", f],
                                }
                            )

                        while not self.stop_event.is_set():
                            if self.ws_reconnect_event.is_set():
                                break
                            if self.refresh_runtime_pause_state():
                                self.stats["ws_connected"] = False
                                break
                            try:
                                msg = await ws.receive(timeout=5)
                            except asyncio.TimeoutError:
                                continue

                            if msg.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = msg.json(loads=json.loads)
                            result = data.get("params", {}).get("result")
                            if not result:
                                continue
                            tx_hash = result.get("transactionHash")
                            block_number = parse_hex_int(result.get("blockNumber"))
                            if not tx_hash:
                                continue
                            if block_number > 0:
                                self.stats["last_ws_block"] = max(self.stats["last_ws_block"], block_number)
                            await self.enqueue_tx(tx_hash, block_number)
            except Exception:
                self.stats["ws_connected"] = False
                await asyncio.sleep(2)

    async def fetch_backfill_txhashes(
        self,
        from_block: int,
        to_block: int,
        launch_configs: List[LaunchConfig],
        rpc: Optional[RPCClient] = None,
    ) -> Set[str]:
        txs: Set[str] = set()
        rpc_client = rpc or self.http_rpc
        vaddr = self.cfg.virtual_token_addr
        for launch in launch_configs:
            internal = topic_address(launch.internal_pool_addr)
            fee = topic_address(launch.fee_addr)
            tax = topic_address(launch.tax_addr)

            logs = await rpc_client.get_logs(
                from_block=from_block,
                to_block=to_block,
                address=vaddr,
                topics=[TRANSFER_TOPIC0, None, [internal, fee, tax]],
            )
            txs.update(x["transactionHash"].lower() for x in logs if x.get("transactionHash"))

            logs = await rpc_client.get_logs(
                from_block=from_block,
                to_block=to_block,
                address=vaddr,
                topics=[TRANSFER_TOPIC0, internal],
            )
            txs.update(x["transactionHash"].lower() for x in logs if x.get("transactionHash"))

            logs = await rpc_client.get_logs(
                from_block=from_block,
                to_block=to_block,
                topics=[TRANSFER_TOPIC0, internal],
            )
            txs.update(x["transactionHash"].lower() for x in logs if x.get("transactionHash"))

            logs = await rpc_client.get_logs(
                from_block=from_block,
                to_block=to_block,
                topics=[TRANSFER_TOPIC0, None, internal],
            )
            txs.update(x["transactionHash"].lower() for x in logs if x.get("transactionHash"))
        return txs

    async def find_block_gte_timestamp(
        self, target_ts: int, rpc: Optional[RPCClient] = None
    ) -> int:
        rpc_client = rpc or self.http_rpc
        latest = await rpc_client.get_latest_block_number()
        latest_ts = await self.get_block_timestamp(latest, rpc=rpc_client)
        if target_ts >= latest_ts:
            return latest

        low = 0
        high = latest
        while low < high:
            mid = (low + high) // 2
            mid_ts = await self.get_block_timestamp(mid, rpc=rpc_client)
            if mid_ts < target_ts:
                low = mid + 1
            else:
                high = mid
        return low

    async def find_block_lte_timestamp(
        self, target_ts: int, rpc: Optional[RPCClient] = None
    ) -> int:
        rpc_client = rpc or self.http_rpc
        latest = await rpc_client.get_latest_block_number()
        first_ts = await self.get_block_timestamp(0, rpc=rpc_client)
        if target_ts <= first_ts:
            return 0
        latest_ts = await self.get_block_timestamp(latest, rpc=rpc_client)
        if target_ts >= latest_ts:
            return latest

        low = 0
        high = latest
        while low < high:
            mid = (low + high + 1) // 2
            mid_ts = await self.get_block_timestamp(mid, rpc=rpc_client)
            if mid_ts <= target_ts:
                low = mid
            else:
                high = mid - 1
        return low

    async def run_scan_range_job(
        self,
        job_id: str,
        project: Optional[str],
        start_ts: int,
        end_ts: int,
    ) -> None:
        job = self.scan_jobs[job_id]
        async with self.scan_lock:
            try:
                if job.get("cancelRequested") or job.get("status") == "canceled":
                    job["status"] = "canceled"
                    job["finishedAt"] = int(time.time())
                    job["error"] = "canceled by user"
                    return

                job["status"] = "running"
                job["startedAt"] = int(time.time())
                scan_rpc = self.backfill_http_rpc
                if not await self.wait_until_resumed():
                    return

                if project:
                    selected = self.storage.get_launch_config_by_name(project)
                    launch_configs = [selected] if selected else []
                else:
                    launch_configs = self.get_launch_configs()
                if not launch_configs:
                    raise ValueError("no enabled launch configs to scan; please enable a project first")

                from_block = await self.find_block_gte_timestamp(start_ts, rpc=scan_rpc)
                to_block = await self.find_block_lte_timestamp(end_ts, rpc=scan_rpc)
                if to_block < from_block:
                    raise ValueError("invalid block range for time range")

                chunk = max(1, self.cfg.backfill_chunk_blocks)
                total_blocks = to_block - from_block + 1
                total_chunks = (total_blocks + chunk - 1) // chunk

                job["fromBlock"] = from_block
                job["toBlock"] = to_block
                job["totalChunks"] = total_chunks
                job["processedChunks"] = 0
                job["scannedTx"] = 0
                job["skippedTx"] = 0
                job["processedTx"] = 0
                parsed_before = int(self.stats.get("parsed_events", 0))
                inserted_before = self.storage.count_events(project=project)
                canceled = False

                current = from_block
                while current <= to_block and not self.stop_event.is_set():
                    if not await self.wait_until_resumed():
                        break
                    if job.get("cancelRequested"):
                        canceled = True
                        break
                    end_block = min(to_block, current + chunk - 1)
                    txs = await self.fetch_backfill_txhashes(
                        current, end_block, launch_configs, rpc=scan_rpc
                    )
                    tx_list = sorted(list(txs))
                    job["scannedTx"] = int(job.get("scannedTx", 0)) + len(tx_list)
                    todo_list = tx_list
                    if project and tx_list:
                        known = self.storage.get_known_backfill_txs(project, tx_list)
                        if known:
                            job["skippedTx"] = int(job.get("skippedTx", 0)) + len(known)
                            todo_list = [x for x in tx_list if x not in known]

                    for tx_hash in todo_list:
                        if not await self.wait_until_resumed():
                            break
                        if job.get("cancelRequested"):
                            canceled = True
                            break
                        await self.process_tx(
                            tx_hash,
                            end_block,
                            launch_configs=launch_configs,
                            rpc=scan_rpc,
                        )
                        job["processedTx"] = int(job.get("processedTx", 0)) + 1
                    if canceled:
                        break
                    if project and todo_list:
                        self.storage.mark_backfill_scanned_txs(project, todo_list)
                    await self.flush_once(force=True)

                    job["processedChunks"] = int(job.get("processedChunks", 0)) + 1
                    job["currentBlock"] = end_block
                    current = end_block + 1

                parsed_after = int(self.stats.get("parsed_events", 0))
                inserted_after = self.storage.count_events(project=project)
                job["parsedDelta"] = max(0, parsed_after - parsed_before)
                job["insertedDelta"] = max(0, inserted_after - inserted_before)
                if canceled:
                    job["status"] = "canceled"
                    job["error"] = "canceled by user"
                    job["canceledAt"] = int(time.time())
                else:
                    job["status"] = "done"
                job["finishedAt"] = int(time.time())
            except Exception as e:
                job["status"] = "failed"
                job["error"] = str(e)
                job["finishedAt"] = int(time.time())

    async def run_scan_range_job_bus(self, job: Dict[str, Any]) -> None:
        job_id = str(job["id"])
        project = str(job["project"]).strip() if job.get("project") else None
        start_ts = int(job.get("startTs", 0))
        end_ts = int(job.get("endTs", 0))

        async with self.scan_lock:
            try:
                scan_rpc = self.backfill_http_rpc
                if not await self.wait_until_resumed():
                    return
                if project:
                    selected = self.storage.get_launch_config_by_name(project)
                    launch_configs = [selected] if selected else []
                else:
                    launch_configs = self.get_launch_configs()
                if not launch_configs:
                    raise ValueError("no enabled launch configs to scan")

                from_block = await self.find_block_gte_timestamp(start_ts, rpc=scan_rpc)
                to_block = await self.find_block_lte_timestamp(end_ts, rpc=scan_rpc)
                if to_block < from_block:
                    raise ValueError("invalid block range for time range")

                chunk = max(1, self.cfg.backfill_chunk_blocks)
                total_blocks = to_block - from_block + 1
                total_chunks = (total_blocks + chunk - 1) // chunk
                self.event_bus.update_scan_job(
                    job_id,
                    from_block=from_block,
                    to_block=to_block,
                    total_chunks=total_chunks,
                    processed_chunks=0,
                    scanned_tx=0,
                    skipped_tx=0,
                    processed_tx=0,
                    parsed_delta=0,
                    inserted_delta=0,
                    current_block=from_block,
                )

                parsed_before = int(self.stats.get("parsed_events", 0))
                inserted_before = self.storage.count_events(project=project)
                canceled = False
                processed_chunks = 0
                scanned_tx = 0
                skipped_tx = 0
                processed_tx = 0

                current = from_block
                while current <= to_block and not self.stop_event.is_set():
                    if not await self.wait_until_resumed():
                        break
                    if self.event_bus.is_scan_job_cancel_requested(job_id):
                        canceled = True
                        break
                    end_block = min(to_block, current + chunk - 1)
                    txs = await self.fetch_backfill_txhashes(
                        current, end_block, launch_configs, rpc=scan_rpc
                    )
                    tx_list = sorted(list(txs))
                    scanned_tx += len(tx_list)
                    todo_list = tx_list
                    if project and tx_list:
                        known = self.storage.get_known_backfill_txs(project, tx_list)
                        if known:
                            skipped_tx += len(known)
                            todo_list = [x for x in tx_list if x not in known]

                    for tx_hash in todo_list:
                        if not await self.wait_until_resumed():
                            break
                        if self.event_bus.is_scan_job_cancel_requested(job_id):
                            canceled = True
                            break
                        await self.process_tx(
                            tx_hash,
                            end_block,
                            launch_configs=launch_configs,
                            rpc=scan_rpc,
                        )
                        processed_tx += 1
                    if canceled:
                        break
                    if project and todo_list:
                        self.storage.mark_backfill_scanned_txs(project, todo_list)

                    processed_chunks += 1
                    current = end_block + 1
                    self.event_bus.update_scan_job(
                        job_id,
                        processed_chunks=processed_chunks,
                        current_block=end_block,
                        scanned_tx=scanned_tx,
                        skipped_tx=skipped_tx,
                        processed_tx=processed_tx,
                    )

                parsed_after = int(self.stats.get("parsed_events", 0))
                inserted_after = self.storage.count_events(project=project)
                done_status = "canceled" if canceled else "done"
                done_error = "canceled by user" if canceled else None
                self.event_bus.update_scan_job(
                    job_id,
                    status=done_status,
                    error=done_error,
                    finished_at=int(time.time()),
                    processed_chunks=processed_chunks,
                    scanned_tx=scanned_tx,
                    skipped_tx=skipped_tx,
                    processed_tx=processed_tx,
                    parsed_delta=max(0, parsed_after - parsed_before),
                    inserted_delta=max(0, inserted_after - inserted_before),
                )
            except Exception as e:
                self.event_bus.update_scan_job(
                    job_id,
                    status="failed",
                    error=str(e),
                    finished_at=int(time.time()),
                )

    async def backfill_loop(self) -> None:
        checkpoint_raw = self.storage.get_state("last_processed_block")
        scan_rpc = self.backfill_http_rpc
        if checkpoint_raw:
            cursor = int(checkpoint_raw)
        else:
            cursor = None

        while not self.stop_event.is_set():
            try:
                if self.refresh_runtime_pause_state():
                    await asyncio.sleep(1)
                    continue
                if cursor is None:
                    latest = await scan_rpc.get_latest_block_number()
                    cursor = max(0, latest - 20)
                    self.storage.set_state("last_processed_block", str(cursor))
                if self.scan_lock.locked():
                    await asyncio.sleep(1)
                    continue
                latest = await scan_rpc.get_latest_block_number()
                target = max(0, latest - self.cfg.confirmations)
                if cursor >= target:
                    await asyncio.sleep(self.cfg.backfill_interval_sec)
                    continue

                launch_configs = self.get_launch_configs()
                if not launch_configs:
                    await asyncio.sleep(self.cfg.backfill_interval_sec)
                    continue
                to_block = min(target, cursor + self.cfg.backfill_chunk_blocks)
                txs = await self.fetch_backfill_txhashes(
                    cursor + 1, to_block, launch_configs, rpc=scan_rpc
                )
                for tx_hash in txs:
                    await self.enqueue_tx(tx_hash, to_block, use_backfill_rpc=True)
                cursor = to_block
                self.storage.set_state("last_processed_block", str(cursor))
                self.stats["last_backfill_block"] = cursor
            except Exception:
                await asyncio.sleep(2)

    async def health_handler(self, request: web.Request) -> web.Response:
        runtime_paused = self.refresh_runtime_pause_state(force=True)
        p, _ = await self.price_service.get_price()
        stats = dict(self.stats)
        queue_size = int(self.queue.qsize())
        pending_tx = int(len(self.pending_txs))
        scan_jobs = int(len(self.scan_jobs))

        if self.role == "writer":
            now = int(time.time())
            queue_size = self.event_bus.queue_size()
            scan_jobs = self.event_bus.count_scan_jobs(only_active=True)

            rt_hb = self.event_bus.get_role_heartbeat("realtime")
            if rt_hb and (now - int(rt_hb["updated_at"]) <= 20):
                rp = rt_hb.get("payload", {})
                stats["ws_connected"] = bool(rp.get("ws_connected", False))
                stats["last_ws_block"] = int(rp.get("last_ws_block", 0))
                stats["enqueued_txs"] = int(rp.get("enqueued_txs", 0))
                stats["processed_txs"] = int(rp.get("processed_txs", 0))
                pending_tx = int(rp.get("pending_txs", 0))
            else:
                stats["ws_connected"] = False

            bf_hb = self.event_bus.get_role_heartbeat("backfill")
            if bf_hb and (now - int(bf_hb["updated_at"]) <= 20):
                bp = bf_hb.get("payload", {})
                stats["last_backfill_block"] = int(bp.get("last_backfill_block", 0))
        if runtime_paused:
            stats["ws_connected"] = False
        runtime_data = self.runtime_pause_payload()

        return web.json_response(
            {
                "ok": True,
                "queueSize": queue_size,
                "pendingTx": pending_tx,
                "stats": stats,
                "lastProcessedBlock": self.storage.get_state("last_processed_block"),
                "price": decimal_to_str(p, 18) if p is not None else None,
                "monitoringProjects": [x.name for x in self.get_launch_configs()],
                "scanJobs": scan_jobs,
                "backfillRpcMode": "separate" if self.backfill_rpc_separate else "shared",
                "role": self.role,
                "runtimePaused": runtime_data["runtimePaused"],
                "runtimeManualPaused": runtime_data["runtimeManualPaused"],
                "runtimeUiOnline": runtime_data["runtimeUiOnline"],
                "runtimeUiLastSeenAt": runtime_data["runtimeUiLastSeenAt"],
                "runtimeUiHeartbeatTimeoutSec": runtime_data["runtimeUiHeartbeatTimeoutSec"],
                "runtimePauseUpdatedAt": runtime_data["updatedAt"],
            }
        )

    async def scan_range_handler(self, request: web.Request) -> web.Response:
        if self.refresh_runtime_pause_state():
            return web.json_response({"error": "runtime is paused"}, status=409)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json body"}, status=400)

        try:
            start_ts = int(payload.get("start_ts"))
            end_ts = int(payload.get("end_ts"))
        except Exception:
            return web.json_response({"error": "start_ts/end_ts must be integer unix seconds"}, status=400)

        if start_ts <= 0 or end_ts <= 0 or end_ts < start_ts:
            return web.json_response({"error": "invalid time range"}, status=400)

        project = payload.get("project")
        if project is not None:
            project = str(project).strip() or None

        if self.role == "writer":
            job_id = self.event_bus.create_scan_job(project, start_ts, end_ts)
            return web.json_response({"ok": True, "jobId": job_id})

        job_id = uuid.uuid4().hex[:12]
        self.scan_jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "project": project,
            "startTs": start_ts,
            "endTs": end_ts,
            "createdAt": int(time.time()),
            "cancelRequested": False,
        }
        asyncio.create_task(self.run_scan_range_job(job_id, project, start_ts, end_ts))
        return web.json_response({"ok": True, "jobId": job_id})

    async def scan_job_detail_handler(self, request: web.Request) -> web.Response:
        job_id = str(request.match_info.get("job_id", "")).strip()
        if self.role == "writer":
            job = self.event_bus.get_scan_job(job_id)
            if not job:
                return web.json_response({"error": "job not found"}, status=404)
            return web.json_response(job)

        job = self.scan_jobs.get(job_id)
        if not job:
            return web.json_response({"error": "job not found"}, status=404)
        return web.json_response(job)

    async def scan_job_cancel_handler(self, request: web.Request) -> web.Response:
        job_id = str(request.match_info.get("job_id", "")).strip()
        if self.role == "writer":
            job = self.event_bus.request_scan_job_cancel(job_id)
            if not job:
                return web.json_response({"error": "job not found"}, status=404)
            final = str(job.get("status") or "") in {"done", "failed", "canceled"}
            return web.json_response({"ok": True, "alreadyFinal": final, "job": job})

        job = self.scan_jobs.get(job_id)
        if not job:
            return web.json_response({"error": "job not found"}, status=404)

        status = str(job.get("status") or "")
        if status in {"done", "failed", "canceled"}:
            return web.json_response({"ok": True, "alreadyFinal": True, "job": job})

        job["cancelRequested"] = True
        job["cancelRequestedAt"] = int(time.time())
        if status == "queued":
            job["status"] = "canceled"
            job["error"] = "canceled by user"
            job["canceledAt"] = int(time.time())
            job["finishedAt"] = int(time.time())
        return web.json_response({"ok": True, "job": job})

    async def launch_configs_handler(self, request: web.Request) -> web.Response:
        rows = self.storage.list_launch_configs()
        return web.json_response({"count": len(rows), "items": rows})

    async def monitored_wallets_handler(self, request: web.Request) -> web.Response:
        rows = self.storage.list_monitored_wallets()
        return web.json_response({"count": len(rows), "items": rows})

    async def monitored_wallet_add_handler(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json body"}, status=400)

        try:
            wallet = normalize_address(str(payload.get("wallet", "")).strip())
            self.storage.add_monitored_wallet(wallet)
            self.reload_my_wallets()
            self.bump_my_wallet_revision()
            return web.json_response(
                {
                    "ok": True,
                    "wallets": sorted(self.get_my_wallets()),
                    "items": self.storage.list_monitored_wallets(),
                    "count": len(self.get_my_wallets()),
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    async def monitored_wallet_delete_handler(self, request: web.Request) -> web.Response:
        wallet_raw = str(request.match_info.get("wallet", "")).strip()
        if not wallet_raw:
            return web.json_response({"error": "wallet is required"}, status=400)
        try:
            wallet = normalize_address(wallet_raw)
            deleted = self.storage.delete_monitored_wallet(wallet)
            if not deleted:
                return web.json_response({"error": f"wallet not found: {wallet}"}, status=404)
            self.reload_my_wallets()
            self.bump_my_wallet_revision()
            return web.json_response(
                {
                    "ok": True,
                    "wallets": sorted(self.get_my_wallets()),
                    "items": self.storage.list_monitored_wallets(),
                    "count": len(self.get_my_wallets()),
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    async def wallet_recalc_handler(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json body"}, status=400)

        project = str(payload.get("project", "")).strip()
        wallet_raw = str(payload.get("wallet", "")).strip()
        if not project:
            return web.json_response({"error": "project is required"}, status=400)
        if not wallet_raw:
            return web.json_response({"error": "wallet is required"}, status=400)

        try:
            wallet = normalize_address(wallet_raw)
            if wallet not in self.get_my_wallets():
                return web.json_response({"error": f"wallet not monitored: {wallet}"}, status=400)

            if self.wallet_recalc_lock.locked():
                return web.json_response({"error": "another wallet recalc is running"}, status=409)

            started = time.time()
            async with self.wallet_recalc_lock:
                result = self.storage.rebuild_wallet_position_for_project_wallet(project, wallet)
            duration_ms = int((time.time() - started) * 1000)
            return web.json_response(
                {
                    "ok": True,
                    "project": result["project"],
                    "wallet": result["wallet"],
                    "eventCount": int(result["eventCount"]),
                    "tokenCount": int(result["tokenCount"]),
                    "durationMs": duration_ms,
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    async def runtime_db_batch_size_get_handler(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "dbBatchSize": self.get_runtime_db_batch_size(),
            }
        )

    async def runtime_db_batch_size_set_handler(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json body"}, status=400)

        raw = payload.get("db_batch_size")
        try:
            value = int(raw)
        except Exception:
            return web.json_response({"error": "db_batch_size must be integer"}, status=400)
        if value < 1 or value > 100:
            return web.json_response({"error": "db_batch_size must be between 1 and 100"}, status=400)
        try:
            applied = self.set_runtime_db_batch_size(value)
            return web.json_response(
                {
                    "ok": True,
                    "dbBatchSize": applied,
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    async def runtime_pause_get_handler(self, request: web.Request) -> web.Response:
        data = self.runtime_pause_payload()
        data["ok"] = True
        return web.json_response(data)

    async def runtime_pause_set_handler(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json body"}, status=400)

        if "paused" not in payload:
            return web.json_response({"error": "paused is required"}, status=400)
        try:
            paused = parse_bool_request(payload.get("paused"))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

        self.set_runtime_paused(paused)
        data = self.runtime_pause_payload()
        data["ok"] = True
        return web.json_response(data)

    async def runtime_heartbeat_handler(self, request: web.Request) -> web.Response:
        self.touch_runtime_ui_heartbeat()
        data = self.runtime_pause_payload()
        data["ok"] = True
        return web.json_response(data)

    async def launch_config_upsert_handler(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json body"}, status=400)

        try:
            name = str(payload.get("name", "")).strip()
            if not name:
                raise ValueError("name cannot be empty")
            internal_pool_addr = normalize_address(str(payload.get("internal_pool_addr", "")).strip())
            is_enabled = bool(payload.get("is_enabled", True))
            switch_only = bool(payload.get("switch_only", False))

            self.storage.upsert_launch_config(
                name=name,
                internal_pool_addr=internal_pool_addr,
                fee_addr=self.fixed_fee_addr,
                tax_addr=self.fixed_tax_addr,
                token_total_supply=self.fixed_token_total_supply,
                fee_rate=self.fixed_fee_rate,
                is_enabled=is_enabled,
            )
            if switch_only:
                self.storage.set_launch_config_enabled_only(name)

            self.reload_launch_configs()
            self.bump_launch_config_revision()
            if self.is_realtime_role:
                self.ws_reconnect_event.set()
            rows = self.storage.list_launch_configs()
            return web.json_response(
                {
                    "ok": True,
                    "monitoringProjects": [x.name for x in self.get_launch_configs()],
                    "items": rows,
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    async def launch_config_delete_handler(self, request: web.Request) -> web.Response:
        name = str(request.match_info.get("name", "")).strip()
        if not name:
            return web.json_response({"error": "name is required"}, status=400)
        try:
            deleted = self.storage.delete_launch_config(name)
            if not deleted:
                return web.json_response({"error": f"project not found: {name}"}, status=404)
            self.reload_launch_configs()
            self.bump_launch_config_revision()
            if self.is_realtime_role:
                self.ws_reconnect_event.set()
            rows = self.storage.list_launch_configs()
            return web.json_response(
                {
                    "ok": True,
                    "monitoringProjects": [x.name for x in self.get_launch_configs()],
                    "items": rows,
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    async def meta_handler(self, request: web.Request) -> web.Response:
        runtime_data = self.runtime_pause_payload()
        launch_configs = self.storage.list_launch_configs()
        projects = self.storage.list_projects()
        monitoring_projects = [x.name for x in self.get_launch_configs()]
        wallets = sorted(list(self.get_my_wallets()))
        return web.json_response(
            {
                "projects": projects,
                "wallets": wallets,
                "topN": self.cfg.top_n,
                "launchConfigs": launch_configs,
                "monitoringProjects": monitoring_projects,
                "fixedDefaults": {
                    "fee_addr": self.fixed_fee_addr,
                    "tax_addr": self.fixed_tax_addr,
                    "token_total_supply": decimal_to_str(self.fixed_token_total_supply, 0),
                    "fee_rate": decimal_to_str(self.fixed_fee_rate, 18),
                },
                "runtimeTuning": {
                    "db_batch_size": self.get_runtime_db_batch_size(),
                    "runtime_paused": runtime_data["runtimePaused"],
                    "runtime_manual_paused": runtime_data["runtimeManualPaused"],
                    "runtime_ui_online": runtime_data["runtimeUiOnline"],
                    "runtime_ui_last_seen_at": runtime_data["runtimeUiLastSeenAt"],
                    "runtime_ui_heartbeat_timeout_sec": runtime_data["runtimeUiHeartbeatTimeoutSec"],
                },
            }
        )

    async def dashboard_handler(self, request: web.Request) -> web.Response:
        return web.FileResponse(self.base_dir / "dashboard.html")

    async def favicon_handler(self, request: web.Request) -> web.Response:
        return web.FileResponse(self.base_dir / "favicon-vpulse.svg")

    async def favicon_ico_handler(self, request: web.Request) -> web.Response:
        favicon_ico = self.base_dir / "favicon" / "favicon.ico"
        if favicon_ico.is_file():
            return web.FileResponse(favicon_ico)
        return web.FileResponse(self.base_dir / "favicon-vpulse.svg")

    async def wallets_handler(self, request: web.Request) -> web.Response:
        project = request.query.get("project")
        project = str(project).strip() if project else None
        data = self.storage.query_wallets(project=project)
        current_wallets = self.get_my_wallets()
        data = [x for x in data if normalize_address(str(x.get("wallet", ""))) in current_wallets]
        return web.json_response({"count": len(data), "items": data})

    async def wallet_detail_handler(self, request: web.Request) -> web.Response:
        addr = normalize_address(request.match_info["addr"])
        project = request.query.get("project")
        project = str(project).strip() if project else None
        data = self.storage.query_wallets(wallet=addr, project=project)
        return web.json_response({"wallet": addr, "count": len(data), "items": data})

    async def minutes_handler(self, request: web.Request) -> web.Response:
        project = request.query.get("project")
        if not project:
            return web.json_response({"error": "project is required"}, status=400)
        try:
            from_ts = int(request.query.get("from", "0"))
            to_ts = int(request.query.get("to", str(int(time.time()))))
        except ValueError:
            return web.json_response({"error": "from/to must be integer"}, status=400)
        data = self.storage.query_minutes(project, from_ts, to_ts)
        return web.json_response({"project": project, "count": len(data), "items": data})

    async def leaderboard_handler(self, request: web.Request) -> web.Response:
        project = request.query.get("project")
        if not project:
            return web.json_response({"error": "project is required"}, status=400)
        top = int(request.query.get("top", str(self.cfg.top_n)))
        data = self.storage.query_leaderboard(project, top)

        launch_cfg = self.storage.get_launch_config_by_name(project)
        total_supply = (
            launch_cfg.token_total_supply if launch_cfg is not None else self.fixed_token_total_supply
        )
        virtual_price_usd, _ = await self.price_service.get_price()

        enriched: List[Dict[str, Any]] = []
        for row in data:
            spent = Decimal(str(row.get("sum_spent_v_est", "0")))
            token = Decimal(str(row.get("sum_token_bought", "0")))
            avg_cost_v = (spent / token) if token > 0 else Decimal(0)
            fdv_v = avg_cost_v * total_supply
            fdv_usd = (fdv_v * virtual_price_usd) if virtual_price_usd is not None else None

            x = dict(row)
            x["avg_cost_v"] = decimal_to_str(avg_cost_v, 18)
            x["breakeven_fdv_v"] = decimal_to_str(fdv_v, 18)
            x["breakeven_fdv_usd"] = (
                decimal_to_str(fdv_usd, 18) if fdv_usd is not None else None
            )
            enriched.append(x)

        return web.json_response({"project": project, "top": top, "items": enriched})

    async def event_delays_handler(self, request: web.Request) -> web.Response:
        project = request.query.get("project")
        if not project:
            return web.json_response({"error": "project is required"}, status=400)
        try:
            limit_n = int(request.query.get("limit", "100"))
        except ValueError:
            return web.json_response({"error": "limit must be integer"}, status=400)
        limit_n = max(1, min(limit_n, 500))
        data = self.storage.query_event_delays(project, limit_n)
        return web.json_response({"project": project, "count": len(data), "items": data})

    async def project_tax_handler(self, request: web.Request) -> web.Response:
        project = request.query.get("project")
        if not project:
            return web.json_response({"error": "project is required"}, status=400)
        try:
            data = self.storage.query_project_tax(project)
            return web.json_response(data)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

    def resolve_cors_origin(self, request_origin: Optional[str]) -> Optional[str]:
        if not request_origin or not self.cors_allow_origins:
            return None
        origin = str(request_origin).strip().rstrip("/")
        if not origin:
            return None
        if "*" in self.cors_allow_origins:
            return "*"
        if origin in self.cors_allow_origins:
            return origin
        return None

    async def create_api_app(self) -> web.Application:
        @web.middleware
        async def cors_middleware(request: web.Request, handler):
            allow_origin = self.resolve_cors_origin(request.headers.get("Origin"))
            if request.method == "OPTIONS":
                response: web.StreamResponse = web.Response(status=204)
            else:
                try:
                    response = await handler(request)
                except web.HTTPException as ex:
                    response = ex

            if allow_origin:
                response.headers["Access-Control-Allow-Origin"] = allow_origin
                response.headers["Vary"] = "Origin"
                response.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
                response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
                response.headers["Access-Control-Max-Age"] = "86400"
            return response

        middlewares = [cors_middleware] if self.cors_allow_origins else []
        app = web.Application(middlewares=middlewares)
        app.router.add_get("/", self.dashboard_handler)
        app.router.add_get("/favicon-vpulse.svg", self.favicon_handler)
        app.router.add_get("/favicon.ico", self.favicon_ico_handler)
        favicon_dir = self.base_dir / "favicon"
        if favicon_dir.is_dir():
            app.router.add_static("/favicon/", path=str(favicon_dir), show_index=False)
        app.router.add_get("/meta", self.meta_handler)
        app.router.add_get("/launch-configs", self.launch_configs_handler)
        app.router.add_post("/launch-configs", self.launch_config_upsert_handler)
        app.router.add_delete("/launch-configs/{name}", self.launch_config_delete_handler)
        app.router.add_get("/wallet-configs", self.monitored_wallets_handler)
        app.router.add_post("/wallet-configs", self.monitored_wallet_add_handler)
        app.router.add_delete("/wallet-configs/{wallet}", self.monitored_wallet_delete_handler)
        app.router.add_post("/wallet-recalc", self.wallet_recalc_handler)
        app.router.add_get("/runtime/db-batch-size", self.runtime_db_batch_size_get_handler)
        app.router.add_post("/runtime/db-batch-size", self.runtime_db_batch_size_set_handler)
        app.router.add_get("/runtime/pause", self.runtime_pause_get_handler)
        app.router.add_post("/runtime/pause", self.runtime_pause_set_handler)
        app.router.add_post("/runtime/heartbeat", self.runtime_heartbeat_handler)
        app.router.add_post("/scan-range", self.scan_range_handler)
        app.router.add_get("/scan-jobs/{job_id}", self.scan_job_detail_handler)
        app.router.add_post("/scan-jobs/{job_id}/cancel", self.scan_job_cancel_handler)
        app.router.add_get("/health", self.health_handler)
        app.router.add_get("/mywallets", self.wallets_handler)
        app.router.add_get("/mywallets/{addr}", self.wallet_detail_handler)
        app.router.add_get("/minutes", self.minutes_handler)
        app.router.add_get("/leaderboard", self.leaderboard_handler)
        app.router.add_get("/event-delays", self.event_delays_handler)
        app.router.add_get("/project-tax", self.project_tax_handler)
        return app

    async def run(self) -> None:
        await self.price_service.start()

        if self.is_realtime_role or self.is_backfill_role:
            worker_count = self.cfg.receipt_workers
            if self.role == "realtime":
                worker_count = self.cfg.receipt_workers_realtime
            elif self.role == "backfill":
                worker_count = self.cfg.receipt_workers_backfill
            for _ in range(max(1, worker_count)):
                self.tasks.append(asyncio.create_task(self.consumer_loop()))
        if self.consume_events_from_bus:
            self.tasks.append(asyncio.create_task(self.bus_writer_loop()))
        elif self.is_writer_role:
            self.tasks.append(asyncio.create_task(self.flush_loop()))
        if self.is_realtime_role:
            self.tasks.append(asyncio.create_task(self.ws_loop()))
        if self.is_backfill_role:
            self.tasks.append(asyncio.create_task(self.backfill_loop()))
        if self.role in {"realtime", "backfill"}:
            self.tasks.append(asyncio.create_task(self.role_heartbeat_loop()))
            self.tasks.append(asyncio.create_task(self.launch_config_watch_loop()))
            self.tasks.append(asyncio.create_task(self.my_wallet_watch_loop()))
        if self.role == "backfill":
            self.tasks.append(asyncio.create_task(self.scan_job_dispatch_loop()))

        runner: Optional[web.AppRunner] = None
        if self.enable_api:
            app = await self.create_api_app()
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host=self.cfg.api_host, port=self.cfg.api_port)
            await site.start()

        while not self.stop_event.is_set():
            await asyncio.sleep(1)

        if runner is not None:
            await runner.cleanup()

    async def shutdown(self) -> None:
        self.stop_event.set()
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        await self.price_service.stop()
        if self.is_writer_role and not self.consume_events_from_bus:
            await self.flush_once(force=True)


async def main_async(config_path: str, role: str) -> None:
    cfg = load_config(config_path)
    async with VirtualsBot(cfg, role=role) as bot:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _on_stop() -> None:
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _on_stop)

        run_task = asyncio.create_task(bot.run())
        wait_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            {run_task, wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for p in pending:
            p.cancel()
        for d in done:
            if d is run_task and d.exception():
                raise d.exception()
        await bot.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Virtuals-Launch-Hunter v1.0 split-role runtime"
    )
    parser.add_argument(
        "--config",
        default="./config.json",
        help="config file path (default: ./config.json)",
    )
    parser.add_argument(
        "--role",
        default="all",
        choices=["all", "writer", "realtime", "backfill"],
        help="run role: all | writer | realtime | backfill",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args.config, args.role))
    except KeyboardInterrupt:
        pass
    except InvalidOperation as e:
        raise SystemExit(f"Decimal calculation error: {e}") from e


if __name__ == "__main__":
    main()
