"""Versioned point-in-time trade and portfolio analysis."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from statistics import fmean, median
from typing import Any

from .analytics import score_components
from .config import JournalAnalyticsConfig
from .enrichment import JournalMarketDataReader
from .identity import canonical_json, stable_id
from .store import TradeJournalStore, rows_as_dicts, utc_now


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _bounded_score(value: float) -> Decimal:
    return Decimal(str(max(0.0, min(100.0, value))))


def _json(value: Any) -> str:
    return canonical_json(value)


def _money(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


class JournalAnalysisEngine:
    def __init__(
        self,
        store: TradeJournalStore,
        market_data: JournalMarketDataReader,
        config: JournalAnalyticsConfig | None = None,
    ):
        self.store = store
        self.market_data = market_data
        self.config = config or JournalAnalyticsConfig()

    def run(self, account: str) -> dict[str, Any]:
        proposed_actions = self._sync_corporate_action_proposals(account)
        source = self._load_source(account)
        fills = source["fills"]
        context_keys = {
            (str(fill["symbol"]), str(fill["exchange"]), fill["trade_date"])
            for fill in fills
        }
        contexts = self.market_data.prior_session_contexts(context_keys)
        episode_by_fill = source["episode_by_fill"]
        outcome_requests: dict[str, tuple[str, str, date, float, date | None]] = {}
        for fill in fills:
            if fill["side"] != "buy":
                continue
            episode = episode_by_fill.get(fill["fill_id"])
            end_date = episode["closed_at"].date() if episode and episode.get("closed_at") else None
            outcome_requests[fill["fill_id"]] = (
                fill["symbol"], fill["exchange"], fill["trade_date"],
                float(fill["price"]), end_date,
            )
        outcomes = self.market_data.forward_outcomes(outcome_requests)
        valuation = self._build_valuation_series(source)
        input_payload = {
            "logic_version": self.config.logic_version,
            "fills": [(fill["fill_id"], fill["economics_hash"]) for fill in fills],
            "context_hashes": sorted(
                context.get("source_snapshot", {}).get("content_hash")
                for context in contexts.values()
            ),
            "outcomes": outcomes,
            "valuation_source_hash": valuation["source_hash"],
            "reconstruction_run_id": source["reconstruction_run_id"],
        }
        input_hash = hashlib.sha256(_json(input_payload).encode()).hexdigest()
        run_id = stable_id(
            "arun", account, "point_in_time_analysis", input_hash, self.config.logic_version
        )
        with self.store.reader() as conn:
            existing = conn.execute(
                "SELECT status FROM journal_analysis_run WHERE analysis_run_id=?", [run_id]
            ).fetchone()
        if existing and existing[0] == "COMPLETED":
            return {
                "analysis_run_id": run_id, "status": "NO_OP",
                "contexts": len(fills), "valuation_sessions": len(valuation["series"]),
                "corporate_action_proposals": proposed_actions,
            }
        result = self._persist(
            account=account, run_id=run_id, input_hash=input_hash, source=source,
            contexts=contexts, outcomes=outcomes, valuation=valuation,
        )
        result["corporate_action_proposals"] = proposed_actions
        return result

    def _sync_corporate_action_proposals(self, account: str) -> int:
        with self.store.reader() as conn:
            rows = conn.execute(
                """SELECT DISTINCT i.instrument_id,i.primary_isin,f.trade_date
                   FROM journal_fill f JOIN instrument_identity i USING(instrument_id)
                   WHERE f.account_ref=? AND i.primary_isin IS NOT NULL""",
                [account],
            ).fetchall()
        if not rows:
            return 0
        instrument_by_isin = {str(row[1]): str(row[0]) for row in rows}
        from_date = min(row[2] for row in rows)
        proposals = self.market_data.corporate_action_proposals(
            isins=set(instrument_by_isin), from_date=from_date, to_date=date.today()
        )
        if not proposals:
            return 0
        inserted = 0
        now = utc_now()
        with self.store.writer() as conn:
            for proposal in proposals:
                instrument_id = instrument_by_isin[proposal["isin"]]
                exists = conn.execute(
                    """SELECT 1 FROM corporate_action_event WHERE instrument_id=?
                       AND action_type=? AND effective_date=?""",
                    [instrument_id, proposal["action_type"], proposal["effective_date"]],
                ).fetchone()
                if exists:
                    continue
                action_id = stable_id(
                    "corp_action", instrument_id, proposal["action_type"],
                    proposal["effective_date"], proposal["source_ref"],
                )
                conn.execute(
                    "INSERT INTO corporate_action_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [action_id, instrument_id, proposal["action_type"],
                     proposal["effective_date"], proposal["quantity_factor"],
                     proposal["cost_factor"], proposal["source"], proposal["source_ref"],
                     "PROPOSED", None, None, _json({
                         "content_hash": proposal["content_hash"],
                         "symbol": proposal["symbol"],
                         "parsed_ratio": proposal["parsed_ratio"],
                         "source_table": "_corporate_actions",
                     }), now],
                )
                inserted += 1
        return inserted

    def _load_source(self, account: str) -> dict[str, Any]:
        with self.store.reader() as conn:
            latest = conn.execute(
                """SELECT analysis_run_id FROM journal_latest_analysis
                   WHERE account_ref=? AND analysis_type='reconstruction' AND status='COMPLETED'""",
                [account],
            ).fetchone()
            if latest is None:
                raise ValueError("a successful reconstruction is required before analysis")
            reconstruction_run_id = latest[0]
            fills = rows_as_dicts(conn.execute(
                """SELECT * FROM journal_fill WHERE account_ref=? AND instrument_id IS NOT NULL
                   ORDER BY executed_at,fill_id""", [account]
            ))
            positions = rows_as_dicts(conn.execute(
                "SELECT * FROM portfolio_reconstruction WHERE analysis_run_id=? ORDER BY instrument_id",
                [reconstruction_run_id],
            ))
            episodes = rows_as_dicts(conn.execute(
                "SELECT * FROM trade_episode WHERE analysis_run_id=? ORDER BY opened_at,episode_id",
                [reconstruction_run_id],
            ))
            links = rows_as_dicts(conn.execute(
                "SELECT * FROM episode_fill_link WHERE analysis_run_id=?",
                [reconstruction_run_id],
            ))
            disposals = rows_as_dicts(conn.execute(
                "SELECT * FROM lot_disposal WHERE analysis_run_id=?",
                [reconstruction_run_id],
            ))
            aliases = rows_as_dicts(conn.execute(
                """SELECT instrument_id,symbol,exchange,valid_from,valid_to,created_at
                   FROM instrument_alias ORDER BY instrument_id,valid_from,created_at"""
            ))
            openings = rows_as_dicts(conn.execute(
                """SELECT * FROM opening_position WHERE account_ref=? AND review_status='APPROVED'
                   ORDER BY effective_at,opening_position_id""", [account]
            ))
            actions = rows_as_dicts(conn.execute(
                """SELECT * FROM corporate_action_event WHERE review_status='APPROVED'
                   ORDER BY effective_date,action_id"""
            ))
            annotations = rows_as_dicts(conn.execute(
                """SELECT a.* FROM journal_annotation a
                   QUALIFY ROW_NUMBER() OVER(PARTITION BY episode_id ORDER BY revision DESC)=1"""
            ))
        episodes_by_id = {row["episode_id"]: row for row in episodes}
        episode_by_fill = {
            row["fill_id"]: episodes_by_id[row["episode_id"]]
            for row in links if row["episode_id"] in episodes_by_id
        }
        link_type_by_fill = {row["fill_id"]: row["link_type"] for row in links}
        return {
            "reconstruction_run_id": reconstruction_run_id,
            "fills": fills, "positions": positions, "episodes": episodes,
            "links": links, "episode_by_fill": episode_by_fill,
            "link_type_by_fill": link_type_by_fill, "disposals": disposals,
            "aliases": aliases, "openings": openings, "actions": actions,
            "annotations": {row["episode_id"]: row for row in annotations},
        }

    def _persist(
        self, *, account: str, run_id: str, input_hash: str,
        source: dict[str, Any],
        contexts: dict[tuple[str, str, date], dict[str, Any]],
        outcomes: dict[str, dict[str, Any]], valuation: dict[str, Any],
    ) -> dict[str, Any]:
        now = utc_now()
        context_rows: list[list[Any]] = []
        evaluation_rows: list[list[Any]] = []
        latest_context_by_instrument: dict[str, dict[str, Any]] = {}
        disposal_by_sell: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for disposal in source["disposals"]:
            disposal_by_sell[disposal["sell_fill_id"]].append(disposal)
        for fill in source["fills"]:
            context = contexts[(fill["symbol"], fill["exchange"], fill["trade_date"])]
            latest_context_by_instrument[fill["instrument_id"]] = context
            context_rows.append([
                stable_id("ctx", run_id, fill["fill_id"]), run_id, fill["fill_id"],
                context.get("cutoff_session"), "PREVIOUS_COMPLETED_SESSION",
                _json(context.get("metrics", {})), _json(context.get("source_snapshot", {})),
                context["trust_status"], self.config.logic_version, now,
            ])
            link_type = source["link_type_by_fill"].get(fill["fill_id"])
            if fill["side"] == "buy":
                components = self._entry_components(context.get("metrics", {}))
                scored = score_components(
                    components, self.config,
                    weights=dict(self.config.entry_component_weights),
                )
                outcome = outcomes.get(fill["fill_id"], {"trust_status": "UNAVAILABLE"})
                evaluation_type = "ENTRY_PROCESS" if link_type == "INITIAL_ENTRY" else "ADD_PROCESS"
                classification = self._entry_classification(context.get("metrics", {}))
            else:
                components = self._exit_components(context.get("metrics", {}))
                scored = score_components(
                    components, self.config,
                    weights=dict(self.config.exit_component_weights),
                )
                realised = sum(
                    (_decimal(row["realised_gross_pnl"]) for row in disposal_by_sell[fill["fill_id"]]),
                    Decimal("0"),
                )
                outcome = {
                    "realised_gross_fifo_pnl": _money(realised),
                    "scope": "gross", "trust_status": fill["trust_status"],
                }
                evaluation_type = "EXIT_PROCESS"
                classification = self._exit_classification(context.get("metrics", {}))
            payload = {
                "process_components": scored["components"],
                "component_contributions": scored["contributions"],
                "coverage": scored["coverage"],
                "ex_post_outcome": outcome,
                "inferred_reason": classification,
                "user_annotation_separate": True,
            }
            episode = source["episode_by_fill"].get(fill["fill_id"])
            evaluation_rows.append([
                stable_id("eval", run_id, fill["fill_id"], evaluation_type), run_id,
                episode["episode_id"] if episode else None, fill["fill_id"], evaluation_type,
                _decimal(scored["score"]) if scored["score"] is not None else None,
                scored["status"], _json(payload), classification,
                "HIGH" if context["trust_status"] == "TRUSTED" else "LOW",
                self.config.logic_version, now,
            ])
        portfolio_metrics = self._portfolio_metrics(
            source=source, valuation=valuation,
            latest_context_by_instrument=latest_context_by_instrument,
        )
        with self.store.writer() as conn:
            conn.execute(
                "INSERT INTO journal_analysis_run VALUES (?,?,?,?,?,?,?,?,?,?)",
                [run_id, account, "point_in_time_analysis", "RUNNING",
                 self.config.logic_version, input_hash, None, now, None, None],
            )
            if context_rows:
                conn.executemany(
                    "INSERT INTO trade_context VALUES (?,?,?,?,?,?,?,?,?,?)", context_rows
                )
            if evaluation_rows:
                conn.executemany(
                    "INSERT INTO trade_evaluation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    evaluation_rows,
                )
            if valuation["valuation_rows"]:
                conn.executemany(
                    "INSERT INTO portfolio_valuation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        [stable_id("valuation", run_id, row["instrument_id"], row["date"]),
                         run_id, account, row["instrument_id"], row["date"], row["quantity"],
                         row["close"], row["market_value"], row["price_source"],
                         row["trust_status"], _json(row["source_snapshot"]), now]
                        for row in valuation["valuation_rows"]
                    ],
                )
            risk_rows = []
            for point in valuation["series"]:
                risk_id = stable_id("risk", run_id, point["date"])
                risk_rows.append([
                    risk_id, run_id, account, point["date"], "holdings_only",
                    _json(point), self.config.logic_version, now,
                ])
            if risk_rows:
                conn.executemany(
                    "INSERT INTO portfolio_risk_snapshot VALUES (?,?,?,?,?,?,?,?)", risk_rows
                )
            as_of = valuation["series"][-1]["date"] if valuation["series"] else date.today()
            conn.execute(
                "INSERT INTO portfolio_evaluation VALUES (?,?,?,?,?,?,?,?,?)",
                [stable_id("peval", run_id), run_id, account, as_of, "holdings_only",
                 _json(portfolio_metrics), portfolio_metrics["trust_status"],
                 self.config.logic_version, now],
            )
            self._persist_policy_breaches(
                conn, run_id=run_id, valuation=valuation, metrics=portfolio_metrics, now=now
            )
            conn.execute(
                "UPDATE journal_analysis_run SET status='COMPLETED',completed_at=? WHERE analysis_run_id=?",
                [now, run_id],
            )
        return {
            "analysis_run_id": run_id, "status": "COMPLETED",
            "contexts": len(context_rows), "evaluations": len(evaluation_rows),
            "valuation_sessions": len(valuation["series"]), "metrics": portfolio_metrics,
        }

    def _build_valuation_series(self, source: dict[str, Any]) -> dict[str, Any]:
        aliases_by_instrument: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for alias in source["aliases"]:
            aliases_by_instrument[alias["instrument_id"]].append(alias)
        reconstruction_trust = {
            row["instrument_id"]: row["trust_status"] for row in source["positions"]
        }
        events: list[tuple[date, int, str, str, Decimal]] = []
        for opening in source["openings"]:
            events.append((
                opening["effective_at"].date(), 10, "quantity", opening["instrument_id"],
                _decimal(opening["quantity"]),
            ))
        instruments = {fill["instrument_id"] for fill in source["fills"]}
        instruments.update(opening["instrument_id"] for opening in source["openings"])
        for action in source["actions"]:
            if action["instrument_id"] in instruments:
                events.append((
                    action["effective_date"], 15, "factor", action["instrument_id"],
                    _decimal(action["quantity_factor"] or 1),
                ))
        linked_fill_ids = {row["fill_id"] for row in source["links"]}
        for fill in source["fills"]:
            if fill["fill_id"] not in linked_fill_ids:
                # A blocking inventory deficit is deliberately not converted into a short.
                # Only fills accepted by the lot matcher may change valuation quantities.
                continue
            quantity = _decimal(fill["quantity"])
            events.append((
                fill["trade_date"], 20, "quantity", fill["instrument_id"],
                quantity if fill["side"] == "buy" else -quantity,
            ))
        if not events:
            return {"valuation_rows": [], "series": [], "source_hash": "empty"}
        events.sort(key=lambda row: (row[0], row[1], row[3]))
        start_date = events[0][0]
        end_date = date.today()
        bars: list[dict[str, Any]] = []
        aliases_by_exchange: dict[str, set[str]] = defaultdict(set)
        for alias in source["aliases"]:
            aliases_by_exchange[alias["exchange"]].add(alias["symbol"])
        for exchange, symbols in aliases_by_exchange.items():
            bars.extend(self.market_data.load_bars(
                symbols=symbols, exchange=exchange, from_date=start_date, to_date=end_date
            ))
        bars_by_symbol_date = {
            (row["symbol"], row["exchange"], row["date"]): row for row in bars
        }
        sessions = sorted({row["date"] for row in bars})
        quantities: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        event_index = 0
        valuation_rows: list[dict[str, Any]] = []
        series: list[dict[str, Any]] = []
        return_index = Decimal("1")
        peak_index = Decimal("1")
        previous_quantities: dict[str, Decimal] = {}
        previous_prices: dict[str, Decimal] = {}
        for session in sessions:
            while event_index < len(events) and events[event_index][0] <= session:
                _, _, kind, instrument_id, value = events[event_index]
                quantities[instrument_id] = (
                    quantities[instrument_id] * value
                    if kind == "factor" else quantities[instrument_id] + value
                )
                event_index += 1
            position_values: dict[str, Decimal] = {}
            current_prices: dict[str, Decimal] = {}
            incomplete: list[str] = []
            for instrument_id, quantity in quantities.items():
                if quantity == 0:
                    continue
                candidates = sorted(
                    (
                        alias for alias in aliases_by_instrument.get(instrument_id, [])
                        if alias["valid_from"] is None or alias["valid_from"] <= session
                    ),
                    key=lambda alias: (alias["valid_from"] or date.min, alias["created_at"]),
                    reverse=True,
                )
                bar = next((
                    bars_by_symbol_date.get((alias["symbol"], alias["exchange"], session))
                    for alias in candidates
                    if bars_by_symbol_date.get((alias["symbol"], alias["exchange"], session))
                ), None)
                close = _decimal(bar["close"]) if bar and bar["close"] is not None else None
                market_value = quantity * close if close is not None else None
                trust_status = bar["trust_status"] if bar else "UNAVAILABLE"
                if close is not None and market_value is not None and trust_status == "TRUSTED":
                    position_values[instrument_id] = market_value
                    current_prices[instrument_id] = close
                    if reconstruction_trust.get(instrument_id) != "TRUSTED":
                        incomplete.append(instrument_id)
                else:
                    incomplete.append(instrument_id)
                valuation_rows.append({
                    "instrument_id": instrument_id, "date": session, "quantity": quantity,
                    "close": close, "market_value": market_value,
                    "price_source": "_catalog_adjusted_close" if bar else None,
                    "trust_status": trust_status,
                    "source_snapshot": {
                        "table": "_catalog", "symbol": bar["symbol"] if bar else None,
                        "exchange": bar["exchange"] if bar else None,
                        "provider": bar["provider"] if bar else None,
                        "ingestion_version": bar["ingestion_version"] if bar else None,
                        "adjusted_prices": bar["adjusted_prices"] if bar else None,
                    },
                })
            total = sum(position_values.values(), Decimal("0"))
            previous_value = sum(
                (
                    quantity * previous_prices[instrument_id]
                    for instrument_id, quantity in previous_quantities.items()
                    if quantity > 0 and instrument_id in previous_prices
                ),
                Decimal("0"),
            )
            price_pnl_by_instrument = {
                instrument_id: quantity * (current_prices[instrument_id] - previous_prices[instrument_id])
                for instrument_id, quantity in previous_quantities.items()
                if quantity > 0 and instrument_id in previous_prices and instrument_id in current_prices
            }
            daily_price_pnl = sum(price_pnl_by_instrument.values(), Decimal("0"))
            if previous_value > 0:
                return_index *= Decimal("1") + daily_price_pnl / previous_value
            peak_index = max(peak_index, return_index)
            drawdown = return_index / peak_index - Decimal("1") if peak_index else Decimal("0")
            weights = [value / total for value in position_values.values()] if total else []
            active_count = sum(quantity != 0 for quantity in quantities.values())
            complete_count = max(0, active_count - len(set(incomplete)))
            series.append({
                "date": session, "market_value": _money(total),
                "known_position_count": complete_count,
                "missing_position_count": len(set(incomplete)),
                "coverage": _money(
                    Decimal(complete_count) / Decimal(active_count)
                ) if active_count else None,
                "top_1_weight": _money(max(weights)) if weights else None,
                "top_5_weight": _money(sum(sorted(weights, reverse=True)[:5], Decimal("0"))) if weights else None,
                "drawdown": _money(drawdown),
                "holdings_return_index": _money(return_index),
                "daily_price_pnl": _money(daily_price_pnl),
                "price_pnl_by_instrument": {
                    key: _money(value) for key, value in price_pnl_by_instrument.items()
                },
                "trust_status": "TRUSTED" if not incomplete else "PARTIAL",
                "position_values": {key: _money(value) for key, value in position_values.items()},
                "missing_instruments": sorted(set(incomplete)),
            })
            previous_quantities = dict(quantities)
            previous_prices = current_prices
        source_hash = hashlib.sha256(_json([
            (row["symbol"], row["date"], row["close"], row["validation_status"], row["ingestion_version"])
            for row in bars
        ]).encode()).hexdigest()
        return {"valuation_rows": valuation_rows, "series": series, "source_hash": source_hash}

    def _portfolio_metrics(
        self, *, source: dict[str, Any], valuation: dict[str, Any],
        latest_context_by_instrument: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        disposals = source["disposals"]
        realised_values = [_decimal(row["realised_gross_pnl"]) for row in disposals]
        gross_profit = sum((value for value in realised_values if value > 0), Decimal("0"))
        gross_loss = abs(sum((value for value in realised_values if value < 0), Decimal("0")))
        realised = sum(realised_values, Decimal("0"))
        closed_episodes = [row for row in source["episodes"] if row["status"] == "CLOSED"]
        holding_days = [int(row["holding_days"]) for row in disposals]
        latest = valuation["series"][-1] if valuation["series"] else None
        latest_values = {
            key: _decimal(value) for key, value in (latest or {}).get("position_values", {}).items()
            if value is not None
        }
        total_value = sum(latest_values.values(), Decimal("0"))
        fifo_cost = sum(
            (_decimal(row["fifo_cost"] or 0) for row in source["positions"] if row["trust_status"] == "TRUSTED"),
            Decimal("0"),
        )
        unrealised = total_value - fifo_cost if total_value else None
        notional = sum(
            (_decimal(row["quantity"]) * _decimal(row["price"]) for row in source["fills"]),
            Decimal("0"),
        )
        average_value = (
            fmean(float(_decimal(point["market_value"])) for point in valuation["series"] if point["market_value"] is not None)
            if valuation["series"] else 0.0
        )
        dimension_values: dict[str, dict[str, Decimal]] = {
            "sector": defaultdict(lambda: Decimal("0")),
            "stage": defaultdict(lambda: Decimal("0")),
            "pattern": defaultdict(lambda: Decimal("0")),
        }
        for instrument_id, value in latest_values.items():
            metrics = latest_context_by_instrument.get(instrument_id, {}).get("metrics", {})
            dimension_values["sector"][str(metrics.get("sector") or "UNKNOWN")] += value
            dimension_values["stage"][str(metrics.get("stage_label") or "UNKNOWN")] += value
            dimension_values["pattern"][str(metrics.get("pattern_state") or "UNKNOWN")] += value
        exposures = {
            dimension: {
                key: _money(value / total_value) if total_value else None
                for key, value in sorted(values.items())
            }
            for dimension, values in dimension_values.items()
        }
        open_episodes = [row for row in source["episodes"] if row["status"] == "OPEN"]
        positions_by_instrument = {
            row["instrument_id"]: row for row in source["positions"]
        }
        known_stop_risk = Decimal("0")
        known_stops = 0
        for episode in open_episodes:
            stop = source["annotations"].get(
                episode["episode_id"], {}
            ).get("intended_stop")
            position = positions_by_instrument.get(episode["instrument_id"])
            market_value = latest_values.get(episode["instrument_id"])
            if stop is None or position is None or market_value is None:
                continue
            quantity = _decimal(position["quantity"])
            if quantity <= 0:
                continue
            current_price = market_value / quantity
            known_stop_risk += max(current_price - _decimal(stop), Decimal("0")) * quantity
            known_stops += 1
        max_drawdown_point = min(
            valuation["series"], key=lambda point: _decimal(point["drawdown"] or 0),
            default=None,
        )
        peak_point = None
        if max_drawdown_point:
            candidates = [point for point in valuation["series"] if point["date"] <= max_drawdown_point["date"]]
            peak_point = max(
                candidates,
                key=lambda point: _decimal(point["holdings_return_index"] or 0),
                default=None,
            )
        attribution: dict[str, str] = {}
        if peak_point and max_drawdown_point:
            attributed: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
            for point in valuation["series"]:
                if peak_point["date"] < point["date"] <= max_drawdown_point["date"]:
                    for instrument_id, value in point.get("price_pnl_by_instrument", {}).items():
                        attributed[instrument_id] += _decimal(value or 0)
            attribution = {
                instrument_id: _money(value) or "0"
                for instrument_id, value in attributed.items()
            }
        cohort_pnl: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        for episode in closed_episodes:
            cohort_pnl[str(episode["opened_at"].year)] += _decimal(episode["realised_gross_pnl"] or 0)
        return {
            "scope": "holdings_only",
            "realised_gross_fifo_pnl": _money(realised),
            "unrealised_pnl": _money(unrealised),
            "market_value": _money(total_value),
            "profit_factor": _money(gross_profit / gross_loss) if gross_loss else None,
            "expectancy_per_closed_episode": _money(realised / Decimal(len(closed_episodes))) if closed_episodes else None,
            "turnover": _money(notional / _decimal(average_value)) if average_value else None,
            "average_holding_days": fmean(holding_days) if holding_days else None,
            "median_holding_days": median(holding_days) if holding_days else None,
            "position_count": len(latest_values),
            "top_1_weight": latest.get("top_1_weight") if latest else None,
            "top_5_weight": latest.get("top_5_weight") if latest else None,
            "stop_coverage": _money(Decimal(known_stops) / Decimal(len(open_episodes))) if open_episodes else None,
            "known_stop_heat": _money(known_stop_risk / total_value) if total_value else None,
            "exposures": exposures,
            "max_drawdown": max_drawdown_point.get("drawdown") if max_drawdown_point else None,
            "max_drawdown_date": max_drawdown_point.get("date") if max_drawdown_point else None,
            "drawdown_attribution": attribution,
            "cohort_realised_pnl": {key: _money(value) for key, value in sorted(cohort_pnl.items())},
            "trust_status": latest.get("trust_status", "UNAVAILABLE") if latest else "UNAVAILABLE",
            "limitations": ["NO_CASH_LEDGER", "NO_CHARGES_OR_TAXES", "HOLDINGS_ONLY_DRAWDOWN"],
        }

    def _persist_policy_breaches(
        self, conn: Any, *, run_id: str, valuation: dict[str, Any],
        metrics: dict[str, Any], now: datetime,
    ) -> None:
        if not valuation["series"]:
            return
        latest = valuation["series"][-1]
        risk_id = stable_id("risk", run_id, latest["date"])
        breaches: list[tuple[str, Decimal, Decimal, dict[str, Any]]] = []
        top_weight = _decimal(latest["top_1_weight"] or 0)
        if top_weight > self.config.max_position_weight:
            breaches.append(("MAX_POSITION_WEIGHT", top_weight, self.config.max_position_weight, {"scope": "holdings_only"}))
        sector_weights = metrics.get("exposures", {}).get("sector", {})
        for sector, raw_weight in sector_weights.items():
            weight = _decimal(raw_weight or 0)
            if sector != "UNKNOWN" and weight > self.config.max_sector_weight:
                breaches.append(("MAX_SECTOR_WEIGHT", weight, self.config.max_sector_weight, {"sector": sector, "scope": "holdings_only"}))
        for rule, observed, threshold, evidence in breaches:
            conn.execute(
                "INSERT INTO portfolio_policy_breach VALUES (?,?,?,?,?,?,?,?)",
                [stable_id("breach", risk_id, rule, evidence), risk_id,
                 self.config.logic_version, rule, observed, threshold, _json(evidence), now],
            )

    @staticmethod
    def _entry_components(metrics: dict[str, Any]) -> dict[str, Decimal | None]:
        close, sma50, sma200 = metrics.get("close"), metrics.get("sma_50"), metrics.get("sma_200")
        trend = None
        if close is not None and sma50 is not None and sma200 is not None:
            trend = Decimal("100") if close > sma50 > sma200 else (Decimal("65") if close > sma50 else Decimal("20"))
        rsi = metrics.get("rsi_14")
        momentum = _bounded_score(100 - abs(float(rsi) - 60) * 2.5) if rsi is not None else None
        adx = metrics.get("adx_14")
        strength = _bounded_score(float(adx) * 3) if adx is not None else None
        volume_z = metrics.get("volume_zscore_20")
        volume = _bounded_score(50 + float(volume_z) * 20) if volume_z is not None else None
        delivery = metrics.get("delivery_intensity")
        delivery_score = _bounded_score(float(delivery) * 60) if delivery is not None else None
        rs = metrics.get("stock_rs_63")
        relative_strength = _bounded_score(50 + float(rs) * 200) if rs is not None else None
        stage_label = str(metrics.get("stage_label") or "").upper()
        stage = Decimal("100") if "2" in stage_label else (Decimal("55") if "1" in stage_label else None)
        pattern_score = metrics.get("pattern_score")
        pattern = _bounded_score(float(pattern_score)) if pattern_score is not None else None
        regime = str(metrics.get("regime") or "")
        sector_stage = str(metrics.get("sector_stage") or "")
        sector_regime = (
            Decimal("100") if regime == "RISK_ON" and "2" in sector_stage
            else Decimal("60") if regime == "TRANSITION" else Decimal("20") if regime == "RISK_OFF" else None
        )
        return {
            "trend": trend, "momentum": momentum, "trend_strength": strength,
            "volume": volume, "delivery": delivery_score,
            "relative_strength": relative_strength, "stage": stage,
            "pattern": pattern, "sector_regime": sector_regime,
        }

    @staticmethod
    def _exit_components(metrics: dict[str, Any]) -> dict[str, Decimal | None]:
        close, sma20 = metrics.get("close"), metrics.get("sma_20")
        trend_break = (
            Decimal("100") if close is not None and sma20 is not None and close < sma20
            else Decimal("25") if close is not None and sma20 is not None else None
        )
        rsi = metrics.get("rsi_14")
        momentum = _bounded_score((50 - float(rsi)) * 4) if rsi is not None else None
        volume_z = metrics.get("volume_zscore_20")
        volume = _bounded_score(50 + abs(float(volume_z)) * 15) if volume_z is not None else None
        stage_label = str(metrics.get("stage_label") or "").upper()
        stage = Decimal("90") if "4" in stage_label else Decimal("70") if "3" in stage_label else Decimal("20") if stage_label else None
        pattern_state = str(metrics.get("pattern_state") or "").lower()
        pattern = Decimal("100") if "invalid" in pattern_state or "failed" in pattern_state else Decimal("25") if pattern_state else None
        regime = str(metrics.get("regime") or "")
        regime_score = Decimal("100") if regime == "RISK_OFF" else Decimal("50") if regime == "TRANSITION" else Decimal("10") if regime else None
        return {
            "trend_break": trend_break, "momentum_deterioration": momentum,
            "volume_confirmation": volume, "stage_deterioration": stage,
            "pattern_invalidation": pattern, "regime_risk": regime_score,
        }

    @staticmethod
    def _entry_classification(metrics: dict[str, Any]) -> str:
        if metrics.get("rsi_14") is not None and float(metrics["rsi_14"]) >= 75:
            return "extended_momentum_entry"
        if "confirm" in str(metrics.get("pattern_state") or "").lower():
            return "pattern_confirmed_entry"
        if "2" in str(metrics.get("stage_label") or ""):
            return "stage2_entry"
        if metrics.get("volume_zscore_20") is not None and float(metrics["volume_zscore_20"]) >= 2:
            return "volume_expansion_entry"
        return "unclassified_entry"

    @staticmethod
    def _exit_classification(metrics: dict[str, Any]) -> str:
        close, sma20 = metrics.get("close"), metrics.get("sma_20")
        if close is not None and sma20 is not None and close < sma20:
            return "trend_break_exit"
        if metrics.get("rsi_14") is not None and float(metrics["rsi_14"]) < 40:
            return "momentum_deterioration_exit"
        if "invalid" in str(metrics.get("pattern_state") or "").lower():
            return "pattern_invalidation_exit"
        return "unclassified_exit"
