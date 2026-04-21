"""Operational preflight checks for pipeline and publish test runs."""

from __future__ import annotations

import importlib.util
import os
import socket
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List
from core.env import load_project_env
from ai_trading_system.platform.db.paths import get_domain_paths


@dataclass
class PreflightCheck:
    """Represents one operational readiness check."""

    name: str
    status: str
    severity: str
    message: str


class PreflightChecker:
    """Runs local readiness checks before live or canary pipeline execution."""

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root)
        load_project_env(self.project_root)

    def run(self, stage_names: Iterable[str], params: Dict) -> Dict[str, object]:
        checks: List[PreflightCheck] = [self._check_duckdb_writable(params.get("data_domain", "operational"))]
        stage_names = list(stage_names)
        publish_requested = "publish" in stage_names and not bool(params.get("local_publish", False))

        if not bool(params.get("smoke", False)):
            checks.append(self._check_env_line_endings())
            if any(stage in stage_names for stage in ("ingest", "features")):
                checks.extend(self._check_dhan())
            if publish_requested or bool(params.get("publish_test", False)):
                checks.extend(self._check_telegram())
                checks.extend(self._check_google_sheets())
                if bool(params.get("preflight_publish_network_checks", True)):
                    checks.extend(self._check_publish_network())

        blocking = [check for check in checks if check.severity == "critical" and check.status == "failed"]
        return {
            "checks": [asdict(check) for check in checks],
            "status": "passed" if not blocking else "failed",
            "blocking_failures": [asdict(check) for check in blocking],
        }

    def _check_duckdb_writable(self, data_domain: str) -> PreflightCheck:
        db_path = get_domain_paths(
            project_root=self.project_root,
            data_domain=data_domain,
        ).ohlcv_db_path
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            with db_path.parent.joinpath(".write_test").open("w", encoding="utf-8") as handle:
                handle.write("ok")
            db_path.parent.joinpath(".write_test").unlink(missing_ok=True)
            return PreflightCheck("duckdb_writable", "passed", "critical", f"Writable: {db_path}")
        except Exception as exc:
            return PreflightCheck("duckdb_writable", "failed", "critical", str(exc))

    def _check_dhan(self) -> List[PreflightCheck]:
        required = ["DHAN_API_KEY", "DHAN_CLIENT_ID"]
        optional_auth = ["DHAN_ACCESS_TOKEN", "DHAN_REFRESH_TOKEN", "DHAN_TOTP"]
        checks = [self._env_required(name, "critical") for name in required]
        has_any_auth = any(os.getenv(name) for name in optional_auth)
        checks.append(
            PreflightCheck(
                "dhan_auth_material",
                "passed" if has_any_auth else "failed",
                "critical",
                "Found at least one Dhan auth token or refresh secret."
                if has_any_auth
                else "Missing DHAN_ACCESS_TOKEN / DHAN_REFRESH_TOKEN / DHAN_TOTP.",
            )
        )
        return checks

    def _check_env_line_endings(self) -> PreflightCheck:
        env_path = self.project_root / ".env"
        if not env_path.exists():
            return PreflightCheck("env_line_endings", "passed", "high", ".env not present.")
        raw = env_path.read_bytes()
        has_crlf = b"\r\n" in raw
        return PreflightCheck(
            "env_line_endings",
            "failed" if has_crlf else "passed",
            "high",
            "Detected CRLF line endings in .env; shell-sourced live runs may corrupt credentials."
            if has_crlf
            else ".env uses Unix line endings.",
        )

    def _check_telegram(self) -> List[PreflightCheck]:
        checks = [
            self._env_required("TELEGRAM_BOT_TOKEN", "critical"),
            self._env_required("TELEGRAM_CHAT_ID", "critical"),
        ]
        checks.append(self._module_check("telegram", "high"))
        return checks

    def _check_google_sheets(self) -> List[PreflightCheck]:
        checks = [
            self._env_required("GOOGLE_SPREADSHEET_ID", "critical"),
            self._module_check("gspread", "high"),
        ]
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
        token_path = os.getenv("GOOGLE_TOKEN_PATH")
        default_cred = self.project_root / "client_secret.json"
        default_token = self.project_root / "token.json"
        exists = any(
            Path(path).exists()
            for path in [credentials_path, token_path]
            if path
        ) or default_cred.exists() or default_token.exists()
        checks.append(
            PreflightCheck(
                "google_credentials_present",
                "passed" if exists else "failed",
                "critical",
                "Google Sheets credentials or token file found."
                if exists
                else "Missing Google Sheets credentials/token file.",
            )
        )
        return checks

    def _check_publish_network(self) -> List[PreflightCheck]:
        return [
            self._dns_check("telegram_dns_api", "api.telegram.org", "critical"),
            self._dns_check("google_dns_oauth2", "oauth2.googleapis.com", "critical"),
            self._dns_check("google_dns_sheets", "sheets.googleapis.com", "critical"),
        ]

    def _dns_check(self, name: str, host: str, severity: str) -> PreflightCheck:
        try:
            socket.getaddrinfo(host, 443)
            return PreflightCheck(name, "passed", severity, f"{host} resolves.")
        except Exception as exc:
            return PreflightCheck(name, "failed", severity, f"DNS resolve failed for {host}: {exc}")

    def _env_required(self, name: str, severity: str) -> PreflightCheck:
        value = os.getenv(name)
        return PreflightCheck(
            name.lower(),
            "passed" if value else "failed",
            severity,
            f"{name} is set." if value else f"{name} is not set.",
        )

    def _module_check(self, module_name: str, severity: str) -> PreflightCheck:
        present = importlib.util.find_spec(module_name) is not None
        return PreflightCheck(
            f"module_{module_name}",
            "passed" if present else "failed",
            severity,
            f"Module {module_name} is available." if present else f"Module {module_name} is not installed.",
        )
