CREATE OR REPLACE VIEW journal_latest_analysis AS
 SELECT * FROM journal_analysis_run
 WHERE status = 'COMPLETED'
 QUALIFY ROW_NUMBER() OVER(
   PARTITION BY account_ref, analysis_type
   ORDER BY completed_at DESC NULLS LAST, started_at DESC
 ) = 1;

CREATE OR REPLACE VIEW journal_current_positions AS
 SELECT p.*
 FROM portfolio_reconstruction p
 JOIN journal_latest_analysis a USING(analysis_run_id)
 WHERE a.analysis_type = 'reconstruction';

UPDATE journal_schema
SET schema_version = '002', applied_at = CURRENT_TIMESTAMP
WHERE schema_name = 'trade_journal';
