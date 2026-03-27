-- ==========================================================================
-- 05 \u2014 ML inference tables and views
--
-- Run AFTER 01-04. Creates the contract for the ML step:
--   * MARTS.ML_INFERENCE_INPUT  \u2014 rows the ML step will score (populated by dbt)
--   * MARTS.ML_PREDICTIONS      \u2014 output rows from SageMaker Batch Transform
--   * MARTS.ML_TRAINING_DATA    \u2014 labeled rows for training (populated by dbt)
--
-- The pipeline itself never writes to ML_INFERENCE_INPUT or ML_TRAINING_DATA;
-- those are dbt mart tables owned by your model definitions.
-- ==========================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_PIPELINE;

-- ---------- Predictions output table ----------
-- Populated by the ml_load Lambda after Batch Transform completes.
-- Single VARIANT `prediction` column so the pipeline works for any task
-- type (classification label, regression value, or multi-output).
CREATE TABLE IF NOT EXISTS MARTS.ML_PREDICTIONS (
    load_id      STRING       NOT NULL COMMENT 'Step Functions execution / load id',
    predicted_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    prediction   VARIANT      NOT NULL COMMENT 'Raw prediction value(s) from batch transform'
)
CLUSTER BY (DATE(predicted_at))
COMMENT = 'Batch transform inference outputs';

GRANT SELECT, INSERT ON TABLE MARTS.ML_PREDICTIONS TO ROLE TRANSFORMER;
GRANT SELECT          ON TABLE MARTS.ML_PREDICTIONS TO ROLE PIPELINE_LOADER;

-- ---------- Placeholder view for inference input ----------
-- Replace with a real dbt mart under dbt/models/marts/ml_inference_input.sql.
-- Must expose the same columns (minus target) that ML_TRAINING_DATA uses.
CREATE OR REPLACE VIEW MARTS.ML_INFERENCE_INPUT AS
SELECT
    NULL::STRING AS placeholder_column
WHERE FALSE  -- returns zero rows until replaced by your dbt model
;

GRANT SELECT ON VIEW MARTS.ML_INFERENCE_INPUT TO ROLE TRANSFORMER;

-- ---------- Placeholder view for training data ----------
-- Replace with a real dbt mart. Must contain a `target_column` (from config.yaml)
-- and N feature columns.
CREATE OR REPLACE VIEW MARTS.ML_TRAINING_DATA AS
SELECT
    NULL::STRING AS placeholder_column,
    NULL::STRING AS target
WHERE FALSE
;

GRANT SELECT ON VIEW MARTS.ML_TRAINING_DATA TO ROLE TRANSFORMER;
