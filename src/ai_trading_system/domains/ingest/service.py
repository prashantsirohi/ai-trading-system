"""Service-layer orchestration for the ingest stage."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Optional

import pandas as pd

from ai_trading_system.domains.ingest.repository import fetch_catalog_close_frame, fetch_catalog_summary
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.pipeline.contracts import DataQualityCriticalError, StageArtifact, StageContext, StageResult
from ai_trading_system.domains.ingest.symbol_master import SymbolMaster


class IngestOrchestrationService:
    """Run the ingest workflow while preserving stage artifacts and summaries."""

    def __init__(self, operation: Optional[Callable[[StageContext], Dict]] = None):
        self.operation = operation

    def run(self, context: StageContext) -> StageResult:
        metadata = self.run_default(context)
        artifact_path = context.write_json("ingest_summary.json", metadata)
        artifact = StageArtifact.from_file(
            "ingest_summary",
            artifact_path,
            row_count=metadata.get("catalog_rows"),
            metadata=metadata,
            attempt_number=context.attempt_number,
        )
        return StageResult(artifacts=[artifact], metadata=metadata)

    def run_default(self, context: StageContext) -> Dict:
        if self.operation is not None:
            result = self.operation(context)
        else:
            from ai_trading_system.domains.ingest import daily_update_runner

            result = daily_update_runner.run(
                symbols_only=True,
                features_only=False,
                batch_size=int(context.params.get("batch_size", 700)),
                bulk=bool(context.params.get("bulk", False)),
                nse_primary=bool(context.params.get("nse_primary", True)),
                symbol_limit=context.params.get("symbol_limit"),
                canary_mode=bool(context.params.get("canary_mode", False)),
                canary_symbol_limit=context.params.get("canary_symbol_limit"),
                data_domain=context.params.get("data_domain", "operational"),
                run_id=context.run_id,
                stale_missing_symbol_grace_days=int(context.params.get("stale_missing_symbol_grace_days", 3)),
                nse_allow_yfinance_fallback=bool(context.params.get("nse_allow_yfinance_fallback", False)),
            )

        catalog_rows, symbol_count, latest_ts = fetch_catalog_summary(context.db_path)

        payload = dict(result or {})
        latest_catalog_date = None
        if latest_ts is not None:
            latest_catalog_date = pd.Timestamp(latest_ts).date().isoformat()
        target_end_date = str(payload.get("target_end_date") or context.run_date)
        payload.update(
            {
                "catalog_rows": int(catalog_rows),
                "symbol_count": int(symbol_count),
                "latest_timestamp": str(latest_ts) if latest_ts is not None else None,
                "freshness_status": self.classify_freshness_status(
                    target_end_date=target_end_date,
                    latest_available_date=latest_catalog_date,
                ),
            }
        )
        payload.update(self.run_bhavcopy_validation(context, payload))
        payload.update(self.run_delivery_collection(context, payload))
        payload["downstream_skip_eligible"] = self.is_downstream_skip_eligible(payload)
        payload["downstream_input_fingerprint"] = self.build_downstream_input_fingerprint(payload)
        return payload

    @staticmethod
    def is_downstream_skip_eligible(payload: Dict) -> bool:
        if "rows_written" not in payload and "updated_symbols" not in payload:
            return False
        updated_symbols = payload.get("updated_symbols")
        rows_written = int(payload.get("rows_written", 0) or 0)
        return rows_written == 0 and not bool(updated_symbols)

    @staticmethod
    def build_downstream_input_fingerprint(payload: Dict) -> str:
        updated_symbols = payload.get("updated_symbols")
        if isinstance(updated_symbols, list):
            normalized_symbols = sorted({str(symbol).strip() for symbol in updated_symbols if str(symbol).strip()})
        else:
            normalized_symbols = []
        trust_summary = payload.get("trust_summary") if isinstance(payload.get("trust_summary"), dict) else {}
        fingerprint_payload = {
            "catalog_rows": int(payload.get("catalog_rows", 0) or 0),
            "symbol_count": int(payload.get("symbol_count", 0) or 0),
            "latest_timestamp": payload.get("latest_timestamp"),
            "rows_written": int(payload.get("rows_written", 0) or 0),
            "benchmark_rows_written": int(payload.get("benchmark_rows_written", 0) or 0),
            "updated_symbols": normalized_symbols,
            "unresolved_date_count": int(payload.get("unresolved_date_count", 0) or 0),
            "unresolved_symbol_count": int(payload.get("unresolved_symbol_count", 0) or 0),
            "validation_counts": payload.get("validation_counts") if isinstance(payload.get("validation_counts"), dict) else {},
            "trust_summary": {
                "status": trust_summary.get("status"),
                "fallback_ratio_latest": trust_summary.get("fallback_ratio_latest"),
                "unknown_ratio_latest": trust_summary.get("unknown_ratio_latest"),
                "active_quarantine_symbol_count": trust_summary.get("active_quarantine_symbol_count"),
                "active_quarantine_ratio": trust_summary.get("active_quarantine_ratio"),
            },
        }
        encoded = json.dumps(fingerprint_payload, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def classify_freshness_status(target_end_date: str, latest_available_date: str | None) -> str:
        if latest_available_date is None:
            return "stale"
        if str(latest_available_date) == str(target_end_date):
            return "fresh"
        return "delayed"

    def run_bhavcopy_validation(self, context: StageContext, ingest_payload: Dict) -> Dict:
        if not bool(context.params.get("validate_bhavcopy_after_ingest", False)):
            return {"bhavcopy_validation_status": "skipped", "bhavcopy_validation_reason": "disabled"}

        validation_date = str(context.params.get("bhavcopy_validation_date") or context.run_date)
        min_coverage = float(context.params.get("bhavcopy_min_coverage", 0.9))
        max_mismatch_ratio = float(context.params.get("bhavcopy_max_mismatch_ratio", 0.05))
        tolerance_pct = float(context.params.get("bhavcopy_close_tolerance_pct", 0.01))
        required = bool(context.params.get("bhavcopy_validation_required", True))

        catalog_df = self.load_catalog_close_frame(context=context, validation_date=validation_date)
        scope_symbols = self.resolve_validation_scope_symbols(ingest_payload, catalog_df)
        bhavcopy_df, bhavcopy_source = self.load_reference_close_frame(
            context=context,
            validation_date=validation_date,
            symbol_ids=sorted(scope_symbols),
        )

        if scope_symbols:
            bhavcopy_scope = bhavcopy_df[bhavcopy_df["symbol_id"].isin(scope_symbols)].copy()
            catalog_scope = catalog_df[catalog_df["symbol_id"].isin(scope_symbols)].copy()
        else:
            bhavcopy_scope = bhavcopy_df.copy()
            catalog_scope = catalog_df.copy()

        if bhavcopy_scope.empty:
            message = (
                f"Bhavcopy validation failed: no bhavcopy rows found for scope on {validation_date} "
                f"(source={bhavcopy_source})."
            )
            if required:
                raise DataQualityCriticalError(message)
            logger.warning(message)
            return {
                "bhavcopy_validation_status": "skipped",
                "bhavcopy_validation_reason": "empty_bhavcopy_scope",
                "bhavcopy_validation_date": validation_date,
                "bhavcopy_validation_source": bhavcopy_source,
            }

        if catalog_scope.empty:
            message = f"Bhavcopy validation failed: no catalog rows found for {validation_date}."
            if required:
                raise DataQualityCriticalError(message)
            logger.warning(message)
            return {
                "bhavcopy_validation_status": "skipped",
                "bhavcopy_validation_reason": "empty_catalog_scope",
                "bhavcopy_validation_date": validation_date,
                "bhavcopy_validation_source": bhavcopy_source,
            }

        merged = catalog_scope.merge(bhavcopy_scope, on="symbol_id", how="inner")
        expected_rows = int(len(bhavcopy_scope))

        mismatch_rows = 0
        mismatch_sample: list[dict[str, object]] = []
        if not merged.empty:
            merged = merged.copy(deep=True)
            merged.loc[:, "close_catalog"] = pd.to_numeric(merged["close_catalog"], errors="coerce")
            merged.loc[:, "close_bhavcopy"] = pd.to_numeric(merged["close_bhavcopy"], errors="coerce")
            merged = merged.dropna(subset=["close_catalog", "close_bhavcopy"])
        compared_rows = int(len(merged))
        coverage_ratio = (compared_rows / expected_rows) if expected_rows else 0.0
        if not merged.empty:
            ref_abs = merged["close_bhavcopy"].abs().replace(0, pd.NA)
            merged.loc[:, "abs_pct_diff"] = (merged["close_catalog"] - merged["close_bhavcopy"]).abs() / ref_abs
            merged.loc[:, "abs_pct_diff"] = merged["abs_pct_diff"].fillna(0.0)
            mismatch_mask = (
                merged["close_catalog"].round(4) != merged["close_bhavcopy"].round(4)
            ) & (merged["abs_pct_diff"] >= tolerance_pct)
            mismatches = merged[mismatch_mask].copy()
            mismatch_rows = int(len(mismatches))
            if mismatch_rows:
                mismatch_sample = (
                    mismatches.sort_values("symbol_id")
                    .head(20)[["symbol_id", "close_catalog", "close_bhavcopy", "abs_pct_diff"]]
                    .to_dict("records")
                )

        mismatch_ratio = (mismatch_rows / compared_rows) if compared_rows else 1.0
        missing_in_catalog = sorted(set(bhavcopy_scope["symbol_id"]) - set(catalog_scope["symbol_id"]))
        missing_in_bhavcopy = sorted(set(catalog_scope["symbol_id"]) - set(bhavcopy_scope["symbol_id"]))

        summary = {
            "bhavcopy_validation_status": "passed",
            "bhavcopy_validation_date": validation_date,
            "bhavcopy_validation_source": bhavcopy_source,
            "bhavcopy_validation_scope_symbols": int(len(scope_symbols)),
            "bhavcopy_validation_expected_rows": expected_rows,
            "bhavcopy_validation_compared_rows": compared_rows,
            "bhavcopy_validation_coverage_ratio": round(float(coverage_ratio), 6),
            "bhavcopy_validation_mismatch_rows": mismatch_rows,
            "bhavcopy_validation_mismatch_ratio": round(float(mismatch_ratio), 6),
            "bhavcopy_validation_close_tolerance_pct": float(tolerance_pct),
            "bhavcopy_validation_min_coverage": float(min_coverage),
            "bhavcopy_validation_max_mismatch_ratio": float(max_mismatch_ratio),
            "bhavcopy_validation_missing_in_catalog_count": len(missing_in_catalog),
            "bhavcopy_validation_missing_in_bhavcopy_count": len(missing_in_bhavcopy),
            "bhavcopy_validation_missing_in_catalog_sample": missing_in_catalog[:25],
            "bhavcopy_validation_missing_in_bhavcopy_sample": missing_in_bhavcopy[:25],
            "bhavcopy_validation_mismatch_sample": mismatch_sample,
        }

        validation_failed = (coverage_ratio < min_coverage) or (mismatch_ratio > max_mismatch_ratio)
        if validation_failed:
            summary["bhavcopy_validation_status"] = "failed"
            message = (
                "Bhavcopy validation gate blocked ingest stage: "
                f"date={validation_date} coverage={coverage_ratio:.2%} "
                f"(min={min_coverage:.2%}) mismatch={mismatch_ratio:.2%} "
                f"(max={max_mismatch_ratio:.2%}) source={bhavcopy_source}"
            )
            if required:
                raise DataQualityCriticalError(message)
            logger.warning(message)

        return summary

    def resolve_validation_scope_symbols(self, ingest_payload: Dict, catalog_df: pd.DataFrame) -> set[str]:
        updated_symbols = ingest_payload.get("updated_symbols")
        if isinstance(updated_symbols, list) and updated_symbols:
            return {str(symbol).strip() for symbol in updated_symbols if str(symbol).strip()}
        return set(catalog_df["symbol_id"].astype(str).tolist())

    def load_catalog_close_frame(self, context: StageContext, validation_date: str) -> pd.DataFrame:
        return fetch_catalog_close_frame(context.db_path, validation_date)

    def load_reference_close_frame(
        self,
        *,
        context: StageContext,
        validation_date: str,
        symbol_ids: list[str],
    ) -> tuple[pd.DataFrame, str]:
        source_mode = str(context.params.get("bhavcopy_validation_source", "bhavcopy") or "bhavcopy").strip().lower()
        if source_mode not in {"auto", "bhavcopy", "yfinance"}:
            raise DataQualityCriticalError(
                f"Invalid bhavcopy_validation_source '{source_mode}'. Expected auto|bhavcopy|yfinance."
            )

        if source_mode in {"auto", "bhavcopy"}:
            bhavcopy_df, bhavcopy_source = self.load_bhavcopy_close_frame(
                context=context,
                validation_date=validation_date,
            )
            if not bhavcopy_df.empty or source_mode == "bhavcopy":
                return bhavcopy_df, bhavcopy_source

        if source_mode in {"auto", "yfinance"}:
            yfinance_df, yfinance_source = self.load_yfinance_close_frame(
                validation_date=validation_date,
                symbol_ids=symbol_ids,
            )
            if not yfinance_df.empty or source_mode == "yfinance":
                return yfinance_df, yfinance_source

        return pd.DataFrame(columns=["symbol_id", "close_bhavcopy"]), "reference_unavailable"

    def load_bhavcopy_close_frame(self, context: StageContext, validation_date: str) -> tuple[pd.DataFrame, str]:
        source_path = context.params.get("bhavcopy_validation_csv")
        if source_path:
            csv_path = Path(str(source_path))
            if not csv_path.is_absolute():
                csv_path = context.project_root / csv_path
            if not csv_path.exists():
                raise DataQualityCriticalError(f"Bhavcopy validation file not found: {csv_path}")
            raw_df = pd.read_csv(csv_path)
            source_label = str(csv_path)
        else:
            from ai_trading_system.domains.ingest.providers.nse import NSECollector

            raw_dir = context.project_root / "data" / "raw" / "NSE_EQ"
            raw_dir.mkdir(parents=True, exist_ok=True)
            collector = NSECollector(data_dir=str(raw_dir))
            raw_df = collector.get_bhavcopy(validation_date)
            try:
                trade_dt = date.fromisoformat(validation_date)
            except ValueError as exc:
                raise DataQualityCriticalError(
                    f"Invalid bhavcopy validation date '{validation_date}'. Expected YYYY-MM-DD."
                ) from exc
            cached_file = raw_dir / f"nse_{trade_dt.strftime('%d%b%Y').upper()}.csv"
            if not raw_df.empty and not cached_file.exists():
                raw_df.to_csv(cached_file, index=False)
            source_label = f"nse_bhavcopy:{validation_date}"

        if raw_df is None or raw_df.empty:
            return pd.DataFrame(columns=["symbol_id", "close_bhavcopy"]), source_label

        frame = raw_df.copy(deep=True)
        frame.columns = [
            str(column).replace("\ufeff", "").strip().upper().replace(" ", "_")
            for column in frame.columns
        ]
        symbol_col = "SYMBOL" if "SYMBOL" in frame.columns else None
        close_col = None
        for candidate in ("CLOSE_PRICE", "CLOSE", "CLOSEPRICE"):
            if candidate in frame.columns:
                close_col = candidate
                break
        series_col = "SERIES" if "SERIES" in frame.columns else None
        if not symbol_col or not close_col:
            raise DataQualityCriticalError(
                f"Bhavcopy validation failed: expected SYMBOL/CLOSE columns not found in source {source_label}."
            )
        if series_col:
            frame = frame[frame[series_col].astype(str).str.strip().str.upper().eq("EQ")]
        isin_col = "ISIN" if "ISIN" in frame.columns else None
        symbol_master = SymbolMaster.from_masterdb(
            str(
                ensure_domain_layout(
                    project_root=context.project_root,
                    data_domain=context.params.get("data_domain", "operational"),
                ).master_db_path
            )
        )
        symbol_map: dict[str, dict[str, str]] = {}
        isin_map: dict[str, dict[str, str]] = {}
        if not symbol_master.frame.empty:
            for row in symbol_master.frame.itertuples(index=False):
                symbol = str(getattr(row, "symbol", "") or "").strip().upper()
                canonical = str(getattr(row, "canonical_symbol", "") or "").strip().upper()
                isin = str(getattr(row, "isin", "") or "").strip().upper()
                if not canonical:
                    continue
                record = {"symbol_id": canonical, "isin": isin}
                if symbol:
                    symbol_map[symbol] = record
                symbol_map[canonical] = record
                if isin:
                    isin_map[isin] = record

        keep_cols = [symbol_col, close_col]
        if isin_col:
            keep_cols.append(isin_col)
        frame = frame[keep_cols].copy()
        frame.columns = ["symbol_raw", "close_bhavcopy"] + (["isin"] if isin_col else [])
        frame.loc[:, "symbol_raw"] = frame["symbol_raw"].astype(str).str.strip().str.upper()
        if "isin" in frame.columns:
            frame.loc[:, "isin"] = frame["isin"].astype(str).str.strip().str.upper()
        else:
            frame.loc[:, "isin"] = ""
        frame.loc[:, "symbol_id"] = frame.apply(
            lambda row: str(
                (
                    isin_map.get(str(row.get("isin", "")))
                    or symbol_map.get(str(row.get("symbol_raw", "")))
                    or {"symbol_id": str(row.get("symbol_raw", ""))}
                ).get("symbol_id", "")
            ),
            axis=1,
        )
        frame.loc[:, "close_bhavcopy"] = pd.to_numeric(frame["close_bhavcopy"], errors="coerce")
        frame = frame[(frame["symbol_id"].astype(str).str.strip() != "") & frame["close_bhavcopy"].notna()]
        return frame.drop_duplicates("symbol_id", keep="last"), source_label

    def load_yfinance_close_frame(self, *, validation_date: str, symbol_ids: list[str]) -> tuple[pd.DataFrame, str]:
        if not symbol_ids:
            return pd.DataFrame(columns=["symbol_id", "close_bhavcopy"]), f"yfinance:{validation_date}"

        try:
            trade_dt = date.fromisoformat(validation_date)
        except ValueError as exc:
            raise DataQualityCriticalError(
                f"Invalid bhavcopy validation date '{validation_date}'. Expected YYYY-MM-DD."
            ) from exc

        try:
            import yfinance as yf
        except Exception as exc:
            raise DataQualityCriticalError(f"yfinance import failed during validation fallback: {exc}") from exc

        start_date = trade_dt.isoformat()
        end_date = (trade_dt + timedelta(days=2)).isoformat()
        rows: list[dict[str, object]] = []
        batch_size = 100

        for start_idx in range(0, len(symbol_ids), batch_size):
            batch_symbols = [
                str(symbol).strip().upper()
                for symbol in symbol_ids[start_idx : start_idx + batch_size]
                if str(symbol).strip()
            ]
            if not batch_symbols:
                continue
            tickers = [f"{symbol}.NS" for symbol in batch_symbols]
            downloaded = yf.download(
                tickers,
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=True,
                group_by="column",
            )
            if downloaded is None or downloaded.empty:
                continue

            for symbol in batch_symbols:
                ticker = f"{symbol}.NS"
                close_series = None
                if isinstance(downloaded.columns, pd.MultiIndex):
                    if "Close" in downloaded.columns.get_level_values(0):
                        try:
                            close_series = downloaded["Close"][ticker]
                        except KeyError:
                            close_series = None
                else:
                    close_series = downloaded["Close"] if "Close" in downloaded.columns else None

                if close_series is None:
                    continue
                close_values = pd.Series(close_series).dropna()
                if close_values.empty:
                    continue
                close_idx = pd.to_datetime(close_values.index, errors="coerce")
                if getattr(close_idx, "tz", None) is not None:
                    close_idx = close_idx.tz_convert(None)
                close_df = pd.DataFrame(
                    {
                        "trade_date": close_idx.date.astype(str),
                        "close": pd.to_numeric(close_values.values, errors="coerce"),
                    }
                ).dropna(subset=["close"])
                match = close_df[close_df["trade_date"] == validation_date]
                if match.empty:
                    continue
                rows.append(
                    {
                        "symbol_id": symbol,
                        "close_bhavcopy": float(match.iloc[-1]["close"]),
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["symbol_id", "close_bhavcopy"]), f"yfinance:{validation_date}"
        frame = pd.DataFrame(rows)
        return frame.drop_duplicates("symbol_id", keep="last"), f"yfinance:{validation_date}"

    def run_delivery_collection(self, context: StageContext, ingest_payload: Dict) -> Dict:
        include_delivery = bool(context.params.get("include_delivery", True))
        if not include_delivery:
            return {
                "delivery_status": "skipped",
                "delivery_reason": "disabled",
            }

        try:
            from ai_trading_system.domains.ingest.delivery import DeliveryCollector

            collector = DeliveryCollector(
                ohlcv_db_path=str(context.db_path),
                data_domain=context.params.get("data_domain", "operational"),
            )
            to_date = str(ingest_payload.get("target_end_date") or context.run_date)
            last_delivery_date = collector.get_last_delivery_date()

            if last_delivery_date:
                from_date = (datetime.fromisoformat(last_delivery_date) + timedelta(days=1)).date().isoformat()
            else:
                backfill_days = int(context.params.get("delivery_backfill_days", 30))
                from_date = (datetime.fromisoformat(to_date) - timedelta(days=backfill_days)).date().isoformat()

            if from_date > to_date:
                return {
                    "delivery_status": "skipped",
                    "delivery_reason": "up_to_date",
                    "delivery_from_date": from_date,
                    "delivery_to_date": to_date,
                    "delivery_last_date": last_delivery_date,
                    "delivery_rows_ingested": 0,
                    "delivery_feature_rows": 0,
                }

            workers = max(1, int(context.params.get("delivery_workers", 4)))
            updated_symbols = ingest_payload.get("updated_symbols")
            symbols: list[str] | None
            if isinstance(updated_symbols, list):
                symbols = sorted({str(symbol) for symbol in updated_symbols if symbol})
            else:
                symbols = None

            rows_ingested = int(
                collector.fetch_range(
                    from_date=from_date,
                    to_date=to_date,
                    n_workers=workers,
                    symbols=symbols,
                )
                or 0
            )
            feature_rows = 0
            if bool(context.params.get("delivery_compute_features", True)) and rows_ingested > 0:
                feature_rows = int(collector.compute_delivery_features(exchange="NSE") or 0)

            return {
                "delivery_status": "completed",
                "delivery_from_date": from_date,
                "delivery_to_date": to_date,
                "delivery_last_date": collector.get_last_delivery_date(),
                "delivery_rows_ingested": rows_ingested,
                "delivery_feature_rows": feature_rows,
            }
        except Exception as exc:
            if bool(context.params.get("delivery_required", False)):
                raise
            logger.warning("Delivery collection failed during ingest stage: %s", exc)
            return {
                "delivery_status": "failed",
                "delivery_error": str(exc),
                "delivery_rows_ingested": 0,
                "delivery_feature_rows": 0,
            }
