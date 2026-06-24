import os
import logging
import math
import re
import random
import threading
import time
from pathlib import Path
from typing import Callable, Optional, List, Dict, Any, Union

import pandas as pd
from ai_trading_system.platform.utils.runtime_config import GoogleSheetsRuntimeConfig
from ai_trading_system.platform.utils.env import load_project_env

load_project_env(__file__)

try:
    import gspread
    from gspread import Spreadsheet, Worksheet
    from google.oauth2.service_account import Credentials
    from google.oauth2.credentials import Credentials as OAuthCredentials
    from google.auth.transport.requests import Request

    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False
    Spreadsheet = Worksheet = object

logger = logging.getLogger(__name__)

_DATE_SHEET_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RETRYABLE_STATUS_CODES = {429, 500, 503}
_RETRYABLE_ERROR_PATTERNS = (
    "resource_exhausted",
    "quota exceeded",
    "rate limit",
    "rate_limit",
    "timeout",
    "timed out",
    "temporarily unavailable",
)
_HELPER_PREFIXES = ("_DATA_", "_RAW_")


class GoogleSheetsTransientError(RuntimeError):
    """Raised when a Google Sheets API error is retryable/transient."""


class GoogleSheetsQuotaLimitedError(GoogleSheetsTransientError):
    """Raised when Google Sheets quota/cooldown is the likely failure cause."""


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(int(os.getenv(name, str(default))), minimum)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(float(os.getenv(name, str(default))), minimum)
    except (TypeError, ValueError):
        return default


def is_google_sheets_retryable_error(exc: BaseException) -> bool:
    return _google_sheets_error_kind(exc) is not None


def is_google_sheets_quota_error(exc: BaseException) -> bool:
    return _google_sheets_error_kind(exc) == "quota"


def _google_sheets_error_kind(exc: BaseException) -> str | None:
    status = getattr(getattr(exc, "resp", None), "status", None) or getattr(exc, "status_code", None)
    try:
        status_int = int(status) if status is not None else None
    except (TypeError, ValueError):
        status_int = None
    text = str(exc).lower()
    if status_int in _RETRYABLE_STATUS_CODES:
        if status_int == 429:
            return "quota"
        return "transient"
    if "resource_exhausted" in text or "quota exceeded" in text or "rate limit" in text or "rate_limit" in text:
        return "quota"
    if any(pattern in text for pattern in _RETRYABLE_ERROR_PATTERNS):
        return "transient"
    return None


def _to_cell(value: Any) -> Any:
    """Coerce a DataFrame cell into a value Google Sheets will treat correctly.

    Numerics stay numeric so number/percentage/date formats applied to the
    column actually render. NaN / None become empty strings so they show
    blank instead of "nan". Everything else is stringified.
    """
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, (int, float)):
        return value
    if pd.isna(value):
        return ""
    return str(value)


class GoogleSheetsManager:
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    _write_lock = threading.Lock()
    _last_write_at = 0.0

    def __init__(
        self,
        credentials_path: Optional[Union[str, Path]] = None,
        spreadsheet_id: Optional[str] = None,
        token_path: Optional[Union[str, Path]] = None,
    ):
        self._base_dir = Path(__file__).resolve().parents[5]
        runtime = GoogleSheetsRuntimeConfig.from_env(self._base_dir)

        if credentials_path:
            self.credentials_path = credentials_path
        else:
            self.credentials_path = runtime.credentials_path

        if token_path:
            self.token_path = token_path
        else:
            self.token_path = runtime.token_path

        self.spreadsheet_id = spreadsheet_id or runtime.spreadsheet_id
        self.client = None
        self.spreadsheet = None
        self.last_error: Optional[str] = None
        self.max_retries = _env_int("GOOGLE_SHEETS_MAX_RETRIES", 5, minimum=0)
        self.max_backoff_seconds = _env_float("GOOGLE_SHEETS_MAX_BACKOFF_SECONDS", 64.0, minimum=1.0)
        self.write_interval_seconds = _env_float("GOOGLE_SHEETS_WRITE_INTERVAL_SECONDS", 1.2, minimum=0.0)
        self.requests_attempted = 0
        self.rows_written = 0
        self.quota_limited = False
        self.retry_recommended_after_seconds: Optional[float] = None
        self.last_retryable_error: Optional[str] = None
        self._authenticate()

    def _set_error(self, message: str) -> None:
        self.last_error = message

    def _execute_with_backoff(self, call: Callable[[], Any], *, is_write: bool = False) -> Any:
        """Execute a Google API call with rate limiting and retry backoff."""
        attempts = max(1, self.max_retries + 1)
        last_exc: BaseException | None = None
        for attempt in range(attempts):
            if is_write:
                self._wait_for_write_slot()
            self.requests_attempted += 1
            try:
                result = call()
                if is_write:
                    with self._write_lock:
                        type(self)._last_write_at = time.monotonic()
                return result
            except Exception as exc:
                kind = _google_sheets_error_kind(exc)
                if kind is None:
                    raise
                last_exc = exc
                self.last_retryable_error = str(exc)
                if kind == "quota":
                    self.quota_limited = True
                if attempt >= attempts - 1:
                    break
                delay = min(2**attempt, self.max_backoff_seconds)
                delay = min(delay + random.uniform(0, min(1.0, delay * 0.25)), self.max_backoff_seconds)
                self.retry_recommended_after_seconds = delay
                time.sleep(delay)
        message = str(last_exc or "Google Sheets retryable error")
        self._set_error(message)
        if self.quota_limited:
            raise GoogleSheetsQuotaLimitedError(message) from last_exc
        raise GoogleSheetsTransientError(message) from last_exc

    def _wait_for_write_slot(self) -> None:
        if self.write_interval_seconds <= 0:
            return
        with self._write_lock:
            elapsed = time.monotonic() - type(self)._last_write_at
            wait_seconds = self.write_interval_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def quota_metadata(self) -> Dict[str, Any]:
        return {
            "google_sheets_quota_limited": bool(self.quota_limited),
            "retry_recommended_after_seconds": self.retry_recommended_after_seconds,
            "sheets_requests_attempted": int(self.requests_attempted),
            "sheets_rows_written": int(self.rows_written),
            "google_sheets_error": self.last_retryable_error or self.last_error,
        }

    def _authenticate(self):
        if not GOOGLE_AVAILABLE:
            message = "Google libraries not installed"
            self._set_error(message)
            logger.warning(message)
            return

        creds = None
        token_error: Optional[str] = None

        if Path(self.token_path).exists():
            try:
                creds = OAuthCredentials.from_authorized_user_file(
                    self.token_path, self.SCOPES
                )
                if creds and creds.valid:
                    self.client = gspread.authorize(creds)
                    logger.info("Authenticated via token.json")
                    self.last_error = None
                    return
                elif creds and creds.refresh_token:
                    creds.refresh(Request())
                    with open(self.token_path, "w") as f:
                        f.write(creds.to_json())
                    self.client = gspread.authorize(creds)
                    logger.info("Token refreshed and authenticated")
                    self.last_error = None
                    return
            except Exception as e:
                message = f"Token auth failed: {e}"
                token_error = message
                self._set_error(message)
                logger.warning(message)

        if Path(self.credentials_path).exists():
            with open(self.credentials_path, "r") as f:
                content = f.read()

            if '"type": "service_account"' in content:
                try:
                    credentials = Credentials.from_service_account_file(
                        self.credentials_path, scopes=self.SCOPES
                    )
                    self.client = gspread.authorize(credentials)
                    logger.info("Authenticated via service account")
                    self.last_error = None
                    return
                except Exception as e:
                    message = f"Service account auth failed: {e}"
                    self._set_error(message)
                    logger.warning(message)

            elif '"installed"' in content or '"web"' in content:
                logger.warning("OAuth2 credentials need re-authentication")
                logger.info("Run: python oauth_flow.py")

                if not creds or not creds.valid:
                    message = "OAuth2 credentials need re-authentication"
                    self._set_error(message)
                    logger.warning(message)
                    logger.info("Please generate OAuth2 token via OAuth flow")
                    return

                self.client = gspread.authorize(creds)
                logger.info("Authenticated via OAuth2")
                self.last_error = None
                return

        if token_error:
            self._set_error(token_error)
            logger.info("Falling back stopped because token-based authentication was present but refresh/auth failed.")
            return

        message = f"Credentials file not found: {self.credentials_path}"
        self._set_error(message)
        logger.warning(message)
        logger.info("Please set up Google Sheets authentication")

    def open_spreadsheet(
        self, spreadsheet_id: Optional[str] = None
    ) -> Optional[Spreadsheet]:
        if not self.client:
            message = self.last_error or "Not authenticated"
            self._set_error(message)
            logger.error(message)
            return None

        sheet_id = spreadsheet_id or self.spreadsheet_id
        if not sheet_id:
            message = "No spreadsheet ID provided"
            self._set_error(message)
            logger.error(message)
            return None

        try:
            self.spreadsheet = self._execute_with_backoff(lambda: self.client.open_by_key(sheet_id))
            logger.info(f"Opened spreadsheet: {self.spreadsheet.title}")
            self.last_error = None
            return self.spreadsheet
        except Exception as e:
            message = f"Failed to open spreadsheet: {e}"
            self._set_error(message)
            logger.error(message)
            return None

    def get_worksheet(self, sheet_name: str) -> Optional[Worksheet]:
        if not self.spreadsheet:
            self.open_spreadsheet()

        if not self.spreadsheet:
            return None

        try:
            return self._execute_with_backoff(lambda: self.spreadsheet.worksheet(sheet_name))
        except Exception:
            message = f"Worksheet '{sheet_name}' not found"
            self._set_error(message)
            logger.error(message)
            return None

    def read_worksheet(self, sheet_name: str = "Sheet1") -> Optional[pd.DataFrame]:
        worksheet = self.get_worksheet(sheet_name)
        if not worksheet:
            return None

        try:
            data = self._execute_with_backoff(lambda: worksheet.get_all_records())
            df = pd.DataFrame(data)
            logger.info(f"Read {len(df)} rows from '{sheet_name}'")
            self.last_error = None
            return df
        except Exception as e:
            message = f"Failed to read worksheet: {e}"
            self._set_error(message)
            logger.error(message)
            return None

    def write_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        include_index: bool = False,
        include_header: bool = True,
        start_cell: str = "A1",
        clear_sheet: bool = False,
    ) -> bool:
        if not self.spreadsheet:
            self.open_spreadsheet()

        worksheet = self.get_worksheet(sheet_name)
        if not worksheet and self.spreadsheet:
            worksheet = self.spreadsheet.add_worksheet(sheet_name, 1000, 26)
            logger.info(f"Created worksheet: {sheet_name}")

        if not worksheet:
            return False

        try:
            if clear_sheet:
                self._execute_with_backoff(lambda: worksheet.clear(), is_write=True)

            data = []
            if include_header:
                data.append(df.columns.tolist())

            if include_index:
                data.extend(
                    [str(idx)] + [_to_cell(v) for v in row.tolist()]
                    for idx, row in df.iterrows()
                )
            else:
                data.extend([_to_cell(v) for v in row.tolist()] for _, row in df.iterrows())

            if data:
                self.update_worksheet_values(worksheet, data, range_name=start_cell)

            logger.info(f"Wrote {len(df)} rows to '{sheet_name}'")
            self.last_error = None
            return True
        except Exception as e:
            message = f"Failed to write to worksheet: {e}"
            self._set_error(message)
            logger.error(message)
            return False

    def append_rows(
        self,
        df: pd.DataFrame,
        sheet_name: str = "Sheet1",
        include_header: bool = False,
    ) -> bool:
        worksheet = self.get_worksheet(sheet_name)
        if not worksheet:
            return self.write_dataframe(df, sheet_name, include_header=False)

        try:
            data = []
            if include_header:
                data.append(df.columns.tolist())
            data.extend([_to_cell(v) for v in row.tolist()] for _, row in df.iterrows())

            self._execute_with_backoff(lambda: worksheet.append_rows(data), is_write=True)
            logger.info(f"Appended {len(df)} rows to '{sheet_name}'")
            self.last_error = None
            self.rows_written += len(data)
            return True
        except Exception as e:
            message = f"Failed to append rows: {e}"
            self._set_error(message)
            logger.error(message)
            return False

    def clear_worksheet(self, sheet_name: str = "Sheet1") -> bool:
        worksheet = self.get_worksheet(sheet_name)
        if not worksheet:
            return False

        try:
            self._execute_with_backoff(lambda: worksheet.clear(), is_write=True)
            logger.info(f"Cleared worksheet '{sheet_name}'")
            self.last_error = None
            return True
        except Exception as e:
            message = f"Failed to clear worksheet: {e}"
            self._set_error(message)
            logger.error(message)
            return False

    def list_worksheets(self) -> List[str]:
        if not self.spreadsheet:
            self.open_spreadsheet()

        if not self.spreadsheet:
            return []

        return [ws.title for ws in self._execute_with_backoff(lambda: self.spreadsheet.worksheets())]

    def delete_worksheets(self, titles: List[str]) -> Dict[str, Any]:
        """Delete worksheets by title, returning a non-throwing summary."""
        if not self.spreadsheet:
            self.open_spreadsheet()
        if not self.spreadsheet:
            return {"deleted": [], "failed": list(titles), "error": self.last_error or "spreadsheet unavailable"}

        requested = {title.lower() for title in titles}
        deleted: List[str] = []
        failed: List[str] = []
        for worksheet in list(self._execute_with_backoff(lambda: self.spreadsheet.worksheets())):
            if worksheet.title.lower() not in requested:
                continue
            try:
                self._execute_with_backoff(lambda worksheet=worksheet: self.spreadsheet.del_worksheet(worksheet), is_write=True)
                deleted.append(worksheet.title)
            except Exception as e:
                failed.append(worksheet.title)
                self._set_error(f"Failed deleting worksheet '{worksheet.title}': {e}")
                logger.warning(self.last_error)
        return {"deleted": deleted, "failed": failed}

    def prune_date_named_worksheets(self, *, keep: int = 0) -> Dict[str, Any]:
        """Delete date-named report tabs, keeping the newest ``keep`` by title."""
        if not self.spreadsheet:
            self.open_spreadsheet()
        if not self.spreadsheet:
            return {"deleted": [], "failed": [], "error": self.last_error or "spreadsheet unavailable"}
        worksheets = self._execute_with_backoff(lambda: self.spreadsheet.worksheets())
        date_tabs = sorted([ws.title for ws in worksheets if _DATE_SHEET_RE.fullmatch(ws.title)], reverse=True)
        to_delete = date_tabs[max(0, keep):]
        return self.delete_worksheets(to_delete)

    def reorder_worksheets(self, ordered_titles: List[str]) -> bool:
        """Best-effort reorder of known worksheets into the operator tab order."""
        if not self.spreadsheet:
            self.open_spreadsheet()
        if not self.spreadsheet:
            return False
        try:
            by_title = {ws.title: ws for ws in self._execute_with_backoff(lambda: self.spreadsheet.worksheets())}
            requests: List[Dict[str, Any]] = []
            for index, title in enumerate(ordered_titles):
                worksheet = by_title.get(title)
                if worksheet is None:
                    continue
                requests.append(
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": int(worksheet.id), "index": index},
                            "fields": "index",
                        }
                    }
                )
            if requests:
                self.batch_update({"requests": requests})
            self.last_error = None
            return True
        except Exception as e:
            message = f"Failed reordering worksheets: {e}"
            self._set_error(message)
            logger.warning(message)
            return False

    # Number-format pattern strings used by ``apply_number_formats``. These
    # match Google Sheets' format mini-language so callers can stay agnostic
    # of the underlying API.
    FORMAT_DATE = {"type": "DATE", "pattern": "yyyy-mm-dd"}
    FORMAT_INT = {"type": "NUMBER", "pattern": "0"}
    FORMAT_DECIMAL_2 = {"type": "NUMBER", "pattern": "0.00"}
    FORMAT_DECIMAL_4 = {"type": "NUMBER", "pattern": "0.0000"}
    FORMAT_PERCENT_1 = {"type": "PERCENT", "pattern": "0.0%"}

    def apply_number_formats(
        self,
        sheet_name: str,
        column_formats: Dict[str, Dict[str, str]],
        *,
        header_row: int = 1,
    ) -> bool:
        """Apply per-column number formats to a worksheet.

        ``column_formats`` maps header name → format spec (see ``FORMAT_*``
        class constants). Columns missing from the sheet are skipped silently
        so callers don't need to gate by schema.
        """
        worksheet = self.get_worksheet(sheet_name)
        if worksheet is None:
            return False
        try:
            header_values = self._execute_with_backoff(lambda: worksheet.row_values(header_row))
        except Exception as e:
            self._set_error(f"Failed reading header row: {e}")
            logger.warning(self.last_error)
            return False
        if not header_values:
            return True

        def _col_letter(idx_zero: int) -> str:
            n = idx_zero
            letters = ""
            while True:
                letters = chr(ord("A") + (n % 26)) + letters
                n = n // 26 - 1
                if n < 0:
                    return letters

        try:
            for header_name, fmt in column_formats.items():
                if header_name not in header_values:
                    continue
                col_idx = header_values.index(header_name)
                letter = _col_letter(col_idx)
                range_ref = f"{letter}{header_row + 1}:{letter}"
                self._execute_with_backoff(lambda range_ref=range_ref, fmt=fmt: worksheet.format(range_ref, {"numberFormat": fmt}), is_write=True)
            self.last_error = None
            return True
        except Exception as e:
            message = f"Failed applying number formats to '{sheet_name}': {e}"
            self._set_error(message)
            logger.warning(message)
            return False

    def replace_line_charts(
        self,
        sheet_name: str,
        *,
        chart_specs: List[Dict[str, Any]],
    ) -> bool:
        """Best-effort replacement of line charts on a worksheet."""
        worksheet = self.get_worksheet(sheet_name)
        if worksheet is None or not self.spreadsheet:
            return False
        try:
            sheet_id = worksheet.id
            metadata = self.fetch_sheet_metadata()
            existing = []
            for sheet in metadata.get("sheets", []):
                if sheet.get("properties", {}).get("sheetId") != sheet_id:
                    continue
                for chart in sheet.get("charts", []):
                    existing.append({"deleteEmbeddedObject": {"objectId": chart["chartId"]}})
            requests: List[Dict[str, Any]] = existing
            for spec in chart_specs:
                requests.append(_line_chart_request(sheet_id=sheet_id, **spec))
            if requests:
                self.batch_update({"requests": requests})
            self.last_error = None
            return True
        except Exception as e:
            message = f"Failed replacing charts on '{sheet_name}': {e}"
            self._set_error(message)
            logger.warning(message)
            return False

    def get_or_create_sheet(
        self, title: str, rows: int = 1000, cols: int = 26
    ) -> Optional[Worksheet]:
        if not self.spreadsheet:
            self.open_spreadsheet()

        if not self.spreadsheet:
            return None

        title_lower = title.lower()
        for ws in self._execute_with_backoff(lambda: self.spreadsheet.worksheets()):
            if ws.title.lower() == title_lower:
                logger.info(f"Found existing worksheet: {ws.title}")
                return ws

        try:
            worksheet = self._execute_with_backoff(lambda: self.spreadsheet.add_worksheet(title, rows, cols), is_write=True)
            logger.info(f"Created worksheet: {title}")
            self.last_error = None
            return worksheet
        except Exception as e:
            message = f"Failed to create worksheet: {e}"
            self._set_error(message)
            logger.error(message)
            return None

    def update_worksheet_values(
        self,
        worksheet: Worksheet,
        values: List[List[Any]],
        *,
        range_name: str = "A1",
    ) -> None:
        normalized = [[_to_cell(value) for value in row] for row in values]
        self._execute_with_backoff(lambda: worksheet.update(normalized, range_name=range_name, raw=False), is_write=True)
        self.rows_written += len(normalized)

    def batch_update(self, body: Dict[str, Any]) -> Any:
        if not self.spreadsheet:
            self.open_spreadsheet()
        if not self.spreadsheet:
            return None
        return self._execute_with_backoff(lambda: self.spreadsheet.batch_update(body), is_write=True)

    def fetch_sheet_metadata(self) -> Dict[str, Any]:
        if not self.spreadsheet:
            self.open_spreadsheet()
        if not self.spreadsheet:
            return {}
        return self._execute_with_backoff(lambda: self.spreadsheet.fetch_sheet_metadata())

    def write_hidden_data_sheet(
        self,
        sheet_name: str,
        dataframe: pd.DataFrame,
        max_rows: int,
        max_cols: int,
    ) -> bool:
        if not sheet_name.startswith(_HELPER_PREFIXES):
            raise ValueError("Hidden helper sheet names must start with _DATA_ or _RAW_")
        frame = dataframe.copy() if isinstance(dataframe, pd.DataFrame) else pd.DataFrame()
        frame = frame.iloc[:max_rows, :max_cols].fillna("")
        sheet = self.get_or_create_sheet(sheet_name, rows=max(max_rows + 5, len(frame) + 5), cols=max(max_cols, len(frame.columns), 1))
        if sheet is None:
            return False
        if not self.write_dataframe(frame, sheet_name=sheet_name, include_header=True, clear_sheet=True):
            return False
        return self.hide_worksheet(sheet_name)

    def hide_worksheet(self, sheet_name: str) -> bool:
        return self._set_hidden(sheet_name, hidden=True)

    def unhide_worksheet(self, sheet_name: str) -> bool:
        return self._set_hidden(sheet_name, hidden=False)

    def _set_hidden(self, sheet_name: str, *, hidden: bool) -> bool:
        if not sheet_name.startswith(_HELPER_PREFIXES):
            raise ValueError("Hidden helper sheet names must start with _DATA_ or _RAW_")
        worksheet = self.get_worksheet(sheet_name)
        if worksheet is None:
            return False
        requests = [
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": int(worksheet.id), "hidden": hidden},
                    "fields": "hidden",
                }
            }
        ]
        self.batch_update({"requests": requests})
        return True


def _dimension_range(sheet_id: int, start_row: int, end_row: int, col: int) -> Dict[str, int]:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }


def _line_chart_request(
    *,
    sheet_id: int,
    title: str,
    start_row: int,
    end_row: int,
    x_col: int,
    y_cols: List[int],
    anchor_row: int,
    anchor_col: int,
) -> Dict[str, Any]:
    return {
        "addChart": {
            "chart": {
                "spec": {
                    "title": title,
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "Date"},
                            {"position": "LEFT_AXIS", "title": "Value"},
                        ],
                        "domains": [
                            {"domain": {"sourceRange": {"sources": [_dimension_range(sheet_id, start_row, end_row, x_col)]}}}
                        ],
                        "series": [
                            {"series": {"sourceRange": {"sources": [_dimension_range(sheet_id, start_row, end_row, col)]}}}
                            for col in y_cols
                        ],
                    },
                },
                "position": {
                    "overlayPosition": {
                        "anchorCell": {"sheetId": sheet_id, "rowIndex": anchor_row, "columnIndex": anchor_col},
                        "offsetXPixels": 0,
                        "offsetYPixels": 0,
                        "widthPixels": 620,
                        "heightPixels": 300,
                    }
                },
            }
        }
    }


class PortfolioSheets(GoogleSheetsManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.positions_sheet = "Positions"
        self.trades_sheet = "Trades"
        self.performance_sheet = "Performance"

    def write_positions(self, positions: List[Dict]) -> bool:
        if not positions:
            return False
        df = pd.DataFrame(positions)
        if "timestamp" not in df.columns:
            df["timestamp"] = pd.Timestamp.now()
        return self.write_dataframe(df, self.positions_sheet, clear_sheet=True)

    def append_trade(self, trade: Dict) -> bool:
        return self.append_rows(pd.DataFrame([trade]), self.trades_sheet)

    def write_performance(self, metrics: Dict) -> bool:
        df = pd.DataFrame([metrics])
        df["date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        return self.write_dataframe(df, self.performance_sheet)


class SectorReportSheets(GoogleSheetsManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sector_sheet = "Sector Strength"
        self.stock_sheet = "Stock Signals"
        self.summary_sheet = "Summary"

    def write_sector_report(
        self, sector_data: pd.DataFrame, timestamp: Optional[pd.Timestamp] = None
    ) -> bool:
        df = sector_data.copy()
        df["report_date"] = (timestamp or pd.Timestamp.now()).strftime("%Y-%m-%d")
        cols = ["report_date"] + [c for c in df.columns if c != "report_date"]
        return self.append_rows(df[cols], self.sector_sheet)

    def write_stock_signals(
        self, signals: List[Dict], timestamp: Optional[pd.Timestamp] = None
    ) -> bool:
        df = pd.DataFrame(signals)
        df["timestamp"] = (timestamp or pd.Timestamp.now()).strftime("%Y-%m-%d %H:%M")
        return self.write_dataframe(df, self.stock_sheet, clear_sheet=True)


def create_sheets_manager(
    credentials_path: Optional[str] = None, spreadsheet_id: Optional[str] = None
) -> GoogleSheetsManager:
    return GoogleSheetsManager(
        credentials_path=credentials_path, spreadsheet_id=spreadsheet_id
    )


if __name__ == "__main__":
    SPREADSHEET_ID = "1_hyFH_RpMXlKlCQZuDkkt-Eh0CxFtqV_zNtk-_W3KUQ"

    manager = GoogleSheetsManager(spreadsheet_id=SPREADSHEET_ID)

    if manager.client:
        print(
            f"Connected! Spreadsheet: {manager.spreadsheet.title if manager.spreadsheet else 'N/A'}"
        )
        print(f"Worksheets: {manager.list_worksheets()}")

        df = manager.read_worksheet("Sheet1")
        if df is not None:
            print(f"\nSheet1 data ({len(df)} rows):")
            print(df.head(10).to_string())
    else:
        print("\nNot connected. Options:")
        print("1. For OAuth2: Generate token via OAuth flow")
        print(
            "2. For Service Account: Use client_secret.json with service_account type"
        )
        print("\nSet GOOGLE_SPREADSHEET_ID in .env for your spreadsheet")
