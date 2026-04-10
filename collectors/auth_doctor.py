"""Small auth self-check utility for Dhan token/bootstrap debugging."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from collectors.token_manager import DhanTokenManager
from core.env import load_project_env


def run_doctor(*, project_root: Path, probe_refresh: bool) -> dict:
    load_project_env(project_root)
    manager = DhanTokenManager(env_path=str(project_root / ".env"))

    payload: dict[str, object] = {
        "client_id_present": bool(manager.client_id),
        "access_token_present": bool(manager.access_token),
        "pin_present": bool(manager.pin),
        "totp_present": bool(__import__("os").getenv("DHAN_TOTP")),
        "is_token_expired": manager.is_token_expired() if manager.client_id else True,
        "is_token_expiring_soon": manager.is_token_expiring_soon() if manager.client_id else True,
    }

    try:
        payload["profile_check"] = manager.get_profile()
    except Exception as exc:  # pragma: no cover - defensive guard around network call
        payload["profile_check"] = {"status": "error", "message": str(exc)}

    if probe_refresh:
        refresh_result = manager.ensure_valid_token(hours_before_expiry=1)
        payload["ensure_valid_token"] = {
            "status": "success" if refresh_result else "error",
            "token_preview": f"{refresh_result[:12]}..." if refresh_result else None,
        }

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Check Dhan auth bootstrap and optional token refresh.")
    parser.add_argument("--probe-refresh", action="store_true", help="Attempt ensure_valid_token() after the basic checks.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    report = run_doctor(project_root=project_root, probe_refresh=bool(args.probe_refresh))
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
