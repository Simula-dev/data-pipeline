-- ==========================================================================
-- 02 \u2014 Raw landing table + ML tables
--
-- Run after 01_schemas.sql against the `data_pipeline` database.
-- ==========================================================================

\c data_pipeline

-- ---------- Raw landing table ----------
-- Single SUPER-typed landing table for all ingest sources. dbt staging
-- models split this by source and parse source-specific shapes.
CREATE TABLE IF NOT EXISTS raw.landing (
    load_id      VARCHAR(255) NOT NULL,
    source       VARCHAR(255) NOT NULL,
    file_path    VARCHAR(1024) NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT GETDATE(),
    data         SUPER        NOT NULL
)
DISTSTYLE KEY
DISTKEY (source)
SORTKEY (ingested_at, source);

COMMENT ON TABLE raw.landing IS 'Unified landing table for all ingested sources';

GRANT SELECT, INSERT ON raw.landing TO GROUP pipeline_loader;
GRANT SELECT           ON raw.landing TO GROUP transformer;

-- ---------- Audit view ----------
CREATE OR REPLACE VIEW raw.landing_audit AS
SELECT
    source,
    DATE(ingested_at)         AS load_date,
    COUNT(*)                  AS rows_loaded,
    COUNT(DISTINCT load_id)   AS load_count,
    COUNT(DISTINCT file_path) AS file_count,
    MAX(ingested_at)          AS last_load_at
FROM raw.landing
GROUP BY 1, 2
ORDER BY load_date DESC, source;

GRANT SELECT ON raw.landing_audit TO GROUP transformer;

-- ---------- ML predictions output table ----------
-- Populated by the ml_load Lambda after Batch Transform completes.
-- SUPER prediction column so the pipeline works for any task type.
CREATE TABLE IF NOT EXISTS marts.ml_predictions (
    load_id      VARCHAR(255) NOT NULL,
    predicted_at TIMESTAMPTZ  NOT NULL DEFAULT GETDATE(),
    prediction   SUPER        NOT NULL
)
DISTSTYLE AUTO
SORTKEY (predicted_at);

COMMENT ON TABLE marts.ml_predictions IS 'Batch transform inference outputs';

GRANT SELECT, INSERT ON marts.ml_predictions TO GROUP transformer;
GRANT SELECT          ON marts.ml_predictions TO GROUP pipeline_loader;

-- ---------- Placeholder views for ML input/training ----------
-- Replace with real dbt marts once your model is defined.
CREATE OR REPLACE VIEW marts.ml_inference_input AS
SELECT
    NULL::VARCHAR AS placeholder_column
WHERE FALSE;

CREATE OR REPLACE VIEW marts.ml_training_data AS
SELECT
    NULL::VARCHAR AS placeholder_column,
    NULL::VARCHAR AS target
WHERE FALSE;

GRANT SELECT ON marts.ml_inference_input TO GROUP transformer;
GRANT SELECT ON marts.ml_training_data   TO GROUP transformer;
