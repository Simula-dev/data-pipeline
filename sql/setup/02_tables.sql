-- ==========================================================================
-- 02 -- Tables and views for RDS PostgreSQL
--
-- Run after 01_schemas.sql against the `data_pipeline` database.
-- ==========================================================================

-- Raw landing table (jsonb instead of Redshift SUPER)
CREATE TABLE IF NOT EXISTS raw.landing (
    load_id      TEXT        NOT NULL,
    source       TEXT        NOT NULL,
    file_path    TEXT        NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data         JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_landing_source ON raw.landing (source);
CREATE INDEX IF NOT EXISTS idx_landing_ingested ON raw.landing (ingested_at);
CREATE INDEX IF NOT EXISTS idx_landing_load ON raw.landing (load_id);

COMMENT ON TABLE raw.landing IS 'Unified landing table for all ingested sources';

GRANT SELECT, INSERT ON raw.landing TO pipeline_loader;
GRANT SELECT           ON raw.landing TO transformer;

-- Audit view
CREATE OR REPLACE VIEW raw.landing_audit AS
SELECT
    source,
    DATE(ingested_at)                AS load_date,
    COUNT(*)                         AS rows_loaded,
    COUNT(DISTINCT load_id)          AS load_count,
    COUNT(DISTINCT file_path)        AS file_count,
    MAX(ingested_at)                 AS last_load_at
FROM raw.landing
GROUP BY 1, 2
ORDER BY load_date DESC, source;

GRANT SELECT ON raw.landing_audit TO transformer;

-- ML predictions output table
CREATE TABLE IF NOT EXISTS marts.ml_predictions (
    load_id      TEXT        NOT NULL,
    predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    prediction   JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ml_predictions_at ON marts.ml_predictions (predicted_at);

COMMENT ON TABLE marts.ml_predictions IS 'Batch transform inference outputs';

GRANT SELECT, INSERT ON marts.ml_predictions TO transformer;
GRANT SELECT          ON marts.ml_predictions TO pipeline_loader;

-- Placeholder views for ML
CREATE OR REPLACE VIEW marts.ml_inference_input AS
SELECT NULL::TEXT AS placeholder_column WHERE FALSE;

CREATE OR REPLACE VIEW marts.ml_training_data AS
SELECT NULL::TEXT AS placeholder_column, NULL::TEXT AS target WHERE FALSE;

GRANT SELECT ON marts.ml_inference_input TO transformer;
GRANT SELECT ON marts.ml_training_data   TO transformer;
