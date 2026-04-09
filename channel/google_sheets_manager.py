import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

import pandas as pd
from core.runtime_config import GoogleSheetsRuntimeConfig
from utils.env import load_project_env

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


class GoogleSheetsManager:
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(
        self,
        credentials_path: Optional[Union[str, Path]] = None,
        spreadsheet_id: Optional[str] = None,
        token_path: Optional[Union[str, Path]] = None,
    ):
        self._base_dir = Path(__file__).parent.parent
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
        self._authenticate()

    def _set_error(self, message: str) -> None:
        self.last_error = message

    def _authenticate(self):
        if not GOOGLE_AVAILABLE:
            message = "Google libraries not installed"
            self._set_error(message)
            logger.warning(message)
            return

        creds = None

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
            self.spreadsheet = self.client.open_by_key(sheet_id)
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
            return self.spreadsheet.worksheet(sheet_name)
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
            data = worksheet.get_all_records()
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
                worksheet.clear()

            data = []
            if include_header:
                data.append(df.columns.tolist())

            if include_index:
                data.extend(
                    [str(idx)] + [str(v) for v in row.tolist()]
                    for idx, row in df.iterrows()
                )
            else:
                data.extend([str(v) for v in row.tolist()] for _, row in df.iterrows())

            if data:
                worksheet.update(data, range_name=start_cell)

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
            data.extend([str(v) for v in row.tolist()] for _, row in df.iterrows())

            worksheet.append_rows(data)
            logger.info(f"Appended {len(df)} rows to '{sheet_name}'")
            self.last_error = None
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
            worksheet.clear()
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

        return [ws.title for ws in self.spreadsheet.worksheets()]

    def get_or_create_sheet(
        self, title: str, rows: int = 1000, cols: int = 26
    ) -> Optional[Worksheet]:
        if not self.spreadsheet:
            self.open_spreadsheet()

        if not self.spreadsheet:
            return None

        title_lower = title.lower()
        for ws in self.spreadsheet.worksheets():
            if ws.title.lower() == title_lower:
                logger.info(f"Found existing worksheet: {ws.title}")
                return ws

        try:
            worksheet = self.spreadsheet.add_worksheet(title, rows, cols)
            logger.info(f"Created worksheet: {title}")
            self.last_error = None
            return worksheet
        except Exception as e:
            message = f"Failed to create worksheet: {e}"
            self._set_error(message)
            logger.error(message)
            return None


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
