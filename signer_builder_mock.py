import argparse
import asyncio
import json
import signal
import time
from typing import Any, Dict

from aiohttp import web


class BuilderMock:
    def __init__(self):
        self.started_at = int(time.time())

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response(
            {"ok": True, "service": "vlh-builder-mock", "startedAt": self.started_at}
        )

    async def build_handler(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("json body must be object")
        except Exception as e:
            return web.json_response({"error": f"invalid json body: {e}"}, status=400)

        # For quick end-to-end testing:
        # - if request provides tx, echo it to signer
        # - otherwise just accept intent (no unsigned tx)
        tx = payload.get("tx")
        if isinstance(tx, dict) and tx.get("to") and tx.get("data") is not None:
            return web.json_response({"ok": True, "tx": tx})

        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
        return web.json_response(
            {
                "ok": True,
                "accepted": True,
                "note": "mock builder accepted intent (no tx generated)",
                "intent": {
                    "trade_id": intent.get("trade_id"),
                    "action": intent.get("action"),
                    "token_address": intent.get("token_address"),
                },
            }
        )

    async def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/health", self.health_handler)
        app.router.add_post("/build", self.build_handler)
        return app


async def run(host: str, port: int) -> None:
    svc = BuilderMock()
    app = await svc.create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_stop() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_stop)
        except NotImplementedError:
            pass

    await stop_event.wait()
    await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="VLH signer builder mock service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19091)
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port))


if __name__ == "__main__":
    main()
