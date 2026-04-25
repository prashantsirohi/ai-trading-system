"""FastAPI backend bootstrap for the React execution console.

This module owns app construction (CORS, API-key middleware, router wiring)
and the CLI entry point. Endpoint logic lives in
``ai_trading_system.ui.execution_api.routes.*`` and request models live in
``ai_trading_system.ui.execution_api.schemas.*``.
"""

from __future__ import annotations

import argparse

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ai_trading_system.ui.execution_api.routes import ALL_ROUTERS
from ai_trading_system.ui.execution_api.routes._deps import (
    API_KEY_HEADER,
    DEFAULT_PROJECT_ROOT,
    configured_api_key,
)


# Re-exported for backwards compatibility with callers that imported these
# constants from ``app`` directly (notably the deprecation shim chain).
__all__ = [
    "API_KEY_HEADER",
    "DEFAULT_PROJECT_ROOT",
    "app",
    "build_parser",
    "create_app",
    "main",
]


def create_app() -> FastAPI:
    app = FastAPI(title="AI Trading Execution API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*", API_KEY_HEADER],
    )

    @app.middleware("http")
    async def api_key_auth(request: Request, call_next):
        # Allow CORS preflight requests (OPTIONS without API key).
        if request.method == "OPTIONS":
            return await call_next(request)
        if request.url.path.startswith("/api"):
            api_key = configured_api_key()
            if api_key is None:
                return JSONResponse(
                    status_code=500,
                    content={"detail": "Execution API key is not configured"},
                )
            key = (request.headers.get(API_KEY_HEADER) or "").strip()
            if key != api_key:
                return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
        return await call_next(request)

    for router in ALL_ROUTERS:
        app.include_router(router)

    return app


app = create_app()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the FastAPI execution backend")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    uvicorn.run(
        "ui.execution_api.app:app", host=args.host, port=args.port, reload=False
    )


if __name__ == "__main__":  # pragma: no cover
    main()
