-- Migration: Add factor monitoring to drift_metric table
-- Add metric_name values for factor correlation and turnover tracking
-- This table already exists, we just need to ensure it can store factor metrics

-- No schema change needed - drift_metric has metadata_json column
-- This migration is a placeholder for documentation

SELECT 1; -- Placeholder migration