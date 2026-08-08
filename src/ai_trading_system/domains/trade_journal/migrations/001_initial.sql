CREATE TABLE IF NOT EXISTS journal_schema(schema_name VARCHAR PRIMARY KEY, schema_version VARCHAR NOT NULL, applied_at TIMESTAMP NOT NULL);
INSERT INTO journal_schema VALUES ('trade_journal','001',CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS journal_import_file(
 import_id VARCHAR PRIMARY KEY, file_sha256 VARCHAR NOT NULL, broker VARCHAR NOT NULL,
 account_ref VARCHAR NOT NULL, file_type VARCHAR NOT NULL, import_mode VARCHAR NOT NULL,
 detected_format VARCHAR, declared_period VARCHAR, detected_from DATE, detected_to DATE,
 as_of_date DATE, captured_at TIMESTAMP, market_state VARCHAR, status VARCHAR NOT NULL,
 row_count BIGINT NOT NULL DEFAULT 0, normalized_count BIGINT NOT NULL DEFAULT 0,
 canonical_snapshot BOOLEAN, supersedes_import_id VARCHAR, error_summary VARCHAR,
 metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, completed_at TIMESTAMP,
 UNIQUE(broker, account_ref, file_type, file_sha256));
CREATE TABLE IF NOT EXISTS journal_import_run(
 import_run_id VARCHAR PRIMARY KEY, import_id VARCHAR NOT NULL, status VARCHAR NOT NULL,
 logic_version VARCHAR NOT NULL, code_version VARCHAR, task_id VARCHAR,
 started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP, error_summary VARCHAR);
CREATE TABLE IF NOT EXISTS journal_raw_row(
 raw_row_id VARCHAR PRIMARY KEY, import_id VARCHAR NOT NULL, sheet_name VARCHAR NOT NULL,
 row_number BIGINT NOT NULL, row_hash VARCHAR NOT NULL, raw_json VARCHAR NOT NULL,
 created_at TIMESTAMP NOT NULL, UNIQUE(import_id, sheet_name, row_number));
CREATE TABLE IF NOT EXISTS journal_dq_issue(
 issue_id VARCHAR PRIMARY KEY, import_id VARCHAR, analysis_run_id VARCHAR, account_ref VARCHAR,
 severity VARCHAR NOT NULL, issue_type VARCHAR NOT NULL, entity_type VARCHAR,
 entity_id VARCHAR, source_row_number BIGINT, evidence_json VARCHAR NOT NULL,
 lifecycle_status VARCHAR NOT NULL DEFAULT 'OPEN', resolution_json VARCHAR,
 created_at TIMESTAMP NOT NULL, resolved_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS instrument_identity(
 instrument_id VARCHAR PRIMARY KEY, primary_isin VARCHAR, created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS instrument_alias(
 alias_id VARCHAR PRIMARY KEY, instrument_id VARCHAR NOT NULL, symbol VARCHAR NOT NULL,
 isin VARCHAR, exchange VARCHAR NOT NULL, segment VARCHAR, series VARCHAR,
 valid_from DATE, valid_to DATE, source_import_id VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS identity_resolution(
 resolution_id VARCHAR PRIMARY KEY, source_import_id VARCHAR NOT NULL, source_row_number BIGINT,
 instrument_id VARCHAR, method VARCHAR NOT NULL, confidence VARCHAR NOT NULL,
 evidence_json VARCHAR NOT NULL, review_status VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS corporate_action_event(
 action_id VARCHAR PRIMARY KEY, instrument_id VARCHAR NOT NULL, action_type VARCHAR NOT NULL,
 effective_date DATE NOT NULL, quantity_factor DECIMAL(38,8), cost_factor DECIMAL(38,8),
 source VARCHAR NOT NULL, source_ref VARCHAR, review_status VARCHAR NOT NULL,
 reviewed_by VARCHAR, reviewed_at TIMESTAMP, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS journal_adjustment_request(
 adjustment_id VARCHAR PRIMARY KEY, account_ref VARCHAR NOT NULL, instrument_id VARCHAR,
 adjustment_type VARCHAR NOT NULL, effective_at TIMESTAMP NOT NULL,
 quantity DECIMAL(38,8), amount DECIMAL(38,8), status VARCHAR NOT NULL,
 reason VARCHAR NOT NULL, evidence_json VARCHAR NOT NULL, proposed_at TIMESTAMP NOT NULL,
 reviewed_by VARCHAR, reviewed_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS journal_fill(
 fill_id VARCHAR PRIMARY KEY, import_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 instrument_id VARCHAR, symbol VARCHAR NOT NULL, isin VARCHAR, exchange VARCHAR NOT NULL,
 segment VARCHAR, series VARCHAR, trade_date DATE NOT NULL, executed_at TIMESTAMP NOT NULL,
 side VARCHAR NOT NULL, auction BOOLEAN NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 price DECIMAL(38,8) NOT NULL, trade_id VARCHAR NOT NULL, order_id VARCHAR NOT NULL,
 economics_hash VARCHAR NOT NULL, trust_status VARCHAR NOT NULL, raw_row_id VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS journal_order(
 order_key VARCHAR PRIMARY KEY, account_ref VARCHAR NOT NULL, instrument_id VARCHAR,
 symbol VARCHAR NOT NULL, exchange VARCHAR NOT NULL, trade_date DATE NOT NULL,
 order_id VARCHAR NOT NULL, side VARCHAR NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 vwap DECIMAL(38,8) NOT NULL, fill_count BIGINT NOT NULL, first_fill_at TIMESTAMP NOT NULL,
 last_fill_at TIMESTAMP NOT NULL, analysis_run_id VARCHAR NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS opening_position(
 opening_position_id VARCHAR PRIMARY KEY, account_ref VARCHAR NOT NULL, instrument_id VARCHAR NOT NULL,
 effective_at TIMESTAMP NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 total_cost DECIMAL(38,8) NOT NULL, provenance VARCHAR NOT NULL,
 source_import_id VARCHAR, review_status VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS opening_lot AS SELECT * FROM opening_position WITH NO DATA;
CREATE TABLE IF NOT EXISTS portfolio_event(
 event_id VARCHAR PRIMARY KEY, account_ref VARCHAR NOT NULL, instrument_id VARCHAR NOT NULL,
 event_type VARCHAR NOT NULL, effective_at TIMESTAMP NOT NULL, quantity DECIMAL(38,8),
 price DECIMAL(38,8), amount DECIMAL(38,8), source_id VARCHAR NOT NULL,
 precedence BIGINT NOT NULL, trust_status VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL,
 created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS position_lot(
 lot_id VARCHAR NOT NULL, analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 instrument_id VARCHAR NOT NULL, opened_at TIMESTAMP NOT NULL,
 original_quantity DECIMAL(38,8) NOT NULL, remaining_quantity DECIMAL(38,8) NOT NULL,
 unit_cost DECIMAL(38,8) NOT NULL, source_event_id VARCHAR NOT NULL,
 generated_at TIMESTAMP NOT NULL, PRIMARY KEY(lot_id,analysis_run_id));
CREATE TABLE IF NOT EXISTS lot_disposal(
 disposal_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL, lot_id VARCHAR NOT NULL,
 sell_fill_id VARCHAR NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 proceeds DECIMAL(38,8) NOT NULL, cost DECIMAL(38,8) NOT NULL,
 realised_gross_pnl DECIMAL(38,8) NOT NULL, holding_days BIGINT NOT NULL,
 disposed_at TIMESTAMP NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS weighted_average_position(
 analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL, instrument_id VARCHAR NOT NULL,
 as_of_at TIMESTAMP NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 average_cost DECIMAL(38,8), total_cost DECIMAL(38,8), trust_status VARCHAR NOT NULL,
 generated_at TIMESTAMP NOT NULL, PRIMARY KEY(analysis_run_id,instrument_id,as_of_at));
CREATE TABLE IF NOT EXISTS trade_episode(
 episode_id VARCHAR NOT NULL, analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 instrument_id VARCHAR NOT NULL, opened_at TIMESTAMP NOT NULL, closed_at TIMESTAMP,
 status VARCHAR NOT NULL, realised_gross_pnl DECIMAL(38,8), trust_status VARCHAR NOT NULL,
 generated_at TIMESTAMP NOT NULL, PRIMARY KEY(episode_id,analysis_run_id));
CREATE TABLE IF NOT EXISTS episode_fill_link(
 episode_id VARCHAR NOT NULL, analysis_run_id VARCHAR NOT NULL, fill_id VARCHAR NOT NULL,
 link_type VARCHAR NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 PRIMARY KEY(episode_id,analysis_run_id,fill_id));

CREATE TABLE IF NOT EXISTS portfolio_snapshot(
 snapshot_id VARCHAR PRIMARY KEY, import_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 as_of_date DATE NOT NULL, captured_at TIMESTAMP, market_state VARCHAR NOT NULL,
 import_mode VARCHAR NOT NULL, trust_status VARCHAR NOT NULL,
 reported_invested DECIMAL(38,8), reported_current_value DECIMAL(38,8),
 reported_pnl DECIMAL(38,8), canonical BOOLEAN NOT NULL, supersedes_snapshot_id VARCHAR,
 created_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_snapshot_position(
 snapshot_id VARCHAR NOT NULL, instrument_id VARCHAR, instrument VARCHAR NOT NULL,
 quantity DECIMAL(38,8) NOT NULL, average_cost DECIMAL(38,8) NOT NULL,
 ltp DECIMAL(38,8) NOT NULL, invested DECIMAL(38,8) NOT NULL,
 current_value DECIMAL(38,8) NOT NULL, pnl DECIMAL(38,8) NOT NULL,
 net_change_pct DECIMAL(38,8), day_change_pct DECIMAL(38,8), raw_json VARCHAR NOT NULL,
 PRIMARY KEY(snapshot_id,instrument));
CREATE TABLE IF NOT EXISTS portfolio_reconstruction(
 analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL, instrument_id VARCHAR NOT NULL,
 as_of_at TIMESTAMP NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 fifo_cost DECIMAL(38,8), weighted_average_cost DECIMAL(38,8),
 trust_status VARCHAR NOT NULL, source_snapshot_json VARCHAR NOT NULL,
 generated_at TIMESTAMP NOT NULL, PRIMARY KEY(analysis_run_id,instrument_id,as_of_at));
CREATE TABLE IF NOT EXISTS portfolio_reconciliation(
 reconciliation_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL,
 snapshot_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL, as_of_at TIMESTAMP NOT NULL,
 status VARCHAR NOT NULL, matched_count BIGINT NOT NULL, issue_count BIGINT NOT NULL,
 trust_status VARCHAR NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_reconciliation_item(
 reconciliation_id VARCHAR NOT NULL, instrument_id VARCHAR, instrument VARCHAR NOT NULL,
 classification VARCHAR NOT NULL, broker_quantity DECIMAL(38,8), ledger_quantity DECIMAL(38,8),
 broker_cost DECIMAL(38,8), fifo_cost DECIMAL(38,8), weighted_average_cost DECIMAL(38,8),
 quantity_difference DECIMAL(38,8), cost_difference DECIMAL(38,8), evidence_json VARCHAR NOT NULL,
 PRIMARY KEY(reconciliation_id,instrument));

CREATE TABLE IF NOT EXISTS journal_analysis_run(
 analysis_run_id VARCHAR PRIMARY KEY, account_ref VARCHAR NOT NULL, analysis_type VARCHAR NOT NULL,
 status VARCHAR NOT NULL, logic_version VARCHAR NOT NULL, input_hash VARCHAR NOT NULL,
 task_id VARCHAR, started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP, error_summary VARCHAR);
CREATE TABLE IF NOT EXISTS journal_task_request(
 journal_run_id VARCHAR PRIMARY KEY, action VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 snapshot_id VARCHAR, status VARCHAR NOT NULL, operator_task_id VARCHAR,
 requested_at TIMESTAMP NOT NULL, started_at TIMESTAMP, completed_at TIMESTAMP,
 result_json VARCHAR, error_summary VARCHAR);
CREATE TABLE IF NOT EXISTS trade_context(
 context_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL, fill_id VARCHAR NOT NULL,
 cutoff_session DATE, context_type VARCHAR NOT NULL, metrics_json VARCHAR NOT NULL,
 source_snapshot_json VARCHAR NOT NULL, trust_status VARCHAR NOT NULL,
 logic_version VARCHAR NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS trade_evaluation(
 evaluation_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL, episode_id VARCHAR,
 fill_id VARCHAR, evaluation_type VARCHAR NOT NULL, score DECIMAL(38,8),
 score_status VARCHAR NOT NULL, components_json VARCHAR NOT NULL,
 classification VARCHAR, confidence VARCHAR NOT NULL, logic_version VARCHAR NOT NULL,
 generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_evaluation(
 evaluation_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 as_of_date DATE NOT NULL, scope_label VARCHAR NOT NULL, metrics_json VARCHAR NOT NULL,
 trust_status VARCHAR NOT NULL, logic_version VARCHAR NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS journal_annotation(
 annotation_id VARCHAR PRIMARY KEY, episode_id VARCHAR NOT NULL, revision BIGINT NOT NULL,
 thesis VARCHAR, setup VARCHAR, intended_stop DECIMAL(38,8), target DECIMAL(38,8),
 exit_reason VARCHAR, lesson VARCHAR, tags_json VARCHAR NOT NULL,
 author VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, UNIQUE(episode_id,revision));
CREATE TABLE IF NOT EXISTS portfolio_valuation(
 valuation_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 instrument_id VARCHAR NOT NULL, valuation_date DATE NOT NULL, quantity DECIMAL(38,8) NOT NULL,
 close_price DECIMAL(38,8), market_value DECIMAL(38,8), price_source VARCHAR,
 trust_status VARCHAR NOT NULL, source_snapshot_json VARCHAR NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_risk_snapshot(
 risk_snapshot_id VARCHAR PRIMARY KEY, analysis_run_id VARCHAR NOT NULL, account_ref VARCHAR NOT NULL,
 as_of_date DATE NOT NULL, scope_label VARCHAR NOT NULL, metrics_json VARCHAR NOT NULL,
 logic_version VARCHAR NOT NULL, generated_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS portfolio_policy_breach(
 breach_id VARCHAR PRIMARY KEY, risk_snapshot_id VARCHAR NOT NULL, policy_version VARCHAR NOT NULL,
 rule_code VARCHAR NOT NULL, observed_value DECIMAL(38,8), threshold_value DECIMAL(38,8),
 evidence_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL);

CREATE OR REPLACE VIEW journal_latest_analysis AS
 SELECT * FROM journal_analysis_run QUALIFY ROW_NUMBER() OVER(PARTITION BY account_ref,analysis_type ORDER BY completed_at DESC NULLS LAST,started_at DESC)=1;
CREATE OR REPLACE VIEW journal_current_positions AS
 SELECT p.* FROM portfolio_reconstruction p JOIN journal_latest_analysis a USING(analysis_run_id) WHERE a.analysis_type='reconstruction' AND a.status='COMPLETED';
