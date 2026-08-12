"""Official BSE cash-market bhavcopy collector."""

from __future__ import annotations

import io
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.logging.logger import logger


_DEFAULT_BSE_EQ_DIR = str(get_domain_paths().raw_dir / "BSE_EQ")


class BSECollector:
    """Download and cache official BSE equity bhavcopies across format eras."""

    _EXPECTED_COLUMN_SETS = (
        frozenset({"SC_CODE", "OPEN", "HIGH", "LOW", "CLOSE", "NO_OF_SHRS"}),
        frozenset({"FinInstrmId", "OpnPric", "HghPric", "LwPric", "ClsPric", "TtlTradgVol"}),
    )

    def __init__(self, data_dir: str = _DEFAULT_BSE_EQ_DIR):
        self.data_dir = data_dir
        self.session = requests.Session()
        retries = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                ),
                "Referer": "https://www.bseindia.com/",
                "Accept": "text/csv,application/zip,application/octet-stream,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def _candidate_bhavcopy_urls(self, trade_date: str) -> list[str]:
        dt = datetime.fromisoformat(trade_date)
        yyyymmdd = dt.strftime("%Y%m%d")
        ddmmyyyy = dt.strftime("%d%m%Y")
        ddmmyy = dt.strftime("%d%m%y")
        base = "https://www.bseindia.com/download/BhavCopy/Equity"
        udiff = f"{base}/BhavCopy_BSE_CM_0_0_0_{yyyymmdd}_F_0000.CSV"
        standardized = f"{base}/BSE_EQ_BHAVCOPY_{ddmmyyyy}_T0.ZIP"
        legacy = f"{base}/EQ{ddmmyy}_CSV.ZIP"
        # UDiFF is the current canonical cash-market file. Prefer the legacy
        # archive before UDiFF for old sessions to avoid an avoidable HTML/404
        # round trip during large historical rebuilds.
        if dt.date() < date(2024, 7, 8):
            return [legacy, udiff, standardized]
        return [udiff, standardized, legacy]

    def _local_bhavcopy_path(self, trade_date: str) -> Path:
        dt = datetime.fromisoformat(trade_date)
        return Path(self.data_dir) / f"bse_canonical_{dt.strftime('%Y%m%d')}.csv"

    @classmethod
    def _has_expected_schema(cls, frame: pd.DataFrame) -> bool:
        columns = {str(column).replace("\ufeff", "").strip() for column in frame.columns}
        return any(required.issubset(columns) for required in cls._EXPECTED_COLUMN_SETS)

    def _read_bhavcopy_response(self, response: requests.Response) -> pd.DataFrame:
        content = response.content
        if not content or content.lstrip().lower().startswith((b"<!doctype html", b"<html")):
            return pd.DataFrame()
        try:
            if content[:2] == b"PK" or ".zip" in response.url.lower():
                with zipfile.ZipFile(io.BytesIO(content)) as archive:
                    csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                    if not csv_names:
                        return pd.DataFrame()
                    with archive.open(csv_names[0]) as source:
                        frame = pd.read_csv(source)
            else:
                frame = pd.read_csv(io.BytesIO(content))
        except (ValueError, OSError, zipfile.BadZipFile, pd.errors.ParserError, pd.errors.EmptyDataError):
            return pd.DataFrame()
        if frame.empty or not self._has_expected_schema(frame):
            return pd.DataFrame()
        return frame

    def get_bhavcopy(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """Return the official BSE cash bhavcopy for ``trade_date``."""
        if trade_date is None:
            trade_date = self._get_previous_business_day()
        local_path = self._local_bhavcopy_path(trade_date)
        if local_path.exists():
            try:
                cached = pd.read_csv(local_path)
                if not cached.empty and self._has_expected_schema(cached):
                    logger.debug("Loaded BSE bhavcopy from canonical cache for %s", trade_date)
                    return cached
            except (OSError, ValueError, pd.errors.ParserError, pd.errors.EmptyDataError):
                logger.warning("Ignoring invalid BSE bhavcopy cache: %s", local_path)

        for url in self._candidate_bhavcopy_urls(trade_date):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                frame = self._read_bhavcopy_response(response)
                if frame.empty:
                    continue
                local_path.parent.mkdir(parents=True, exist_ok=True)
                frame.to_csv(local_path, index=False)
                return frame
            except requests.RequestException as exc:
                logger.warning("BSE bhavcopy candidate failed for %s: %s", url, exc)
        return pd.DataFrame()

    @staticmethod
    def _get_previous_business_day() -> str:
        current = datetime.now()
        for _ in range(7):
            current -= timedelta(days=1)
            if current.weekday() < 5:
                return current.strftime("%Y-%m-%d")
        return current.strftime("%Y-%m-%d")
