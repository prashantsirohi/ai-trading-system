"""Serve the isolated Phase 4A read-only API."""

from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from ai_trading_system.interfaces.api.app import create_app
from ai_trading_system.interfaces.api.config import ApiSettings, SourceProfile


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the Phase 4A read-only API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--reload", action="store_true", help="Local development only")
    parser.add_argument("--fixture-profile", choices=[item.value for item in SourceProfile], default="operator_read_only")
    parser.add_argument("--copied-control-plane", type=Path)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    profile = SourceProfile(args.fixture_profile)
    if args.reload and profile is not SourceProfile.SMALL_FIXTURE:
        raise SystemExit("--reload is allowed only with small_fixture")
    settings = ApiSettings.from_env()
    settings = ApiSettings(
        source_profile=profile,
        copied_control_plane=args.copied_control_plane,
        auth_enabled=settings.auth_enabled,
        local_dev_mode=settings.local_dev_mode,
        api_key=settings.api_key,
        host=args.host,
        port=args.port,
        default_page_size=settings.default_page_size,
        max_page_size=settings.max_page_size,
        rate_limit_per_minute=settings.rate_limit_per_minute,
        cache_enabled=settings.cache_enabled,
        cache_ttl_seconds=settings.cache_ttl_seconds,
        include_openapi=settings.include_openapi,
        max_response_rows=settings.max_response_rows,
        cors_allowed_origins=settings.cors_allowed_origins,
    )
    uvicorn.run(create_app(settings=settings), host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
