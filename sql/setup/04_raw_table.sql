-- ==========================================================================
-- 04 — Raw landing table
--
-- Single VARIANT-typed landing table for all ingest sources. dbt staging
-- models split this by source and parse source-specific shapes.
-- ==========================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_PIPELINE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS LANDING (
    load_id      STRING       NOT NULL COMMENT 'Lambda request id for this load',
    source       STRING       NOT NULL COMMENT 'Logical source name from ingest step',
    file_path    STRING       NOT NULL COMMENT 'S3 object key (METADATA$FILENAME)',
    ingested_at  TIMESTAMP_TZ NOT NULL COMMENT 'Load timestamp',
    data         VARIANT      NOT NULL COMMENT 'Full JSON record'
)
CLUSTER BY (source, DATE(ingested_at))
COMMENT = 'Unified landing table for all ingested sources';

-- Grants
GRANT SELECT, INSERT ON TABLE LANDING TO ROLE PIPELINE_LOADER;
GRANT SELECT           ON TABLE LANDING TO ROLE TRANSFORMER;

-- Optional: simple audit view for monitoring recent loads
CREATE OR REPLACE VIEW LANDING_AUDIT AS
SELECT
    source,
    DATE(ingested_at)         AS load_date,
    COUNT(*)                  AS rows_loaded,
    COUNT(DISTINCT load_id)   AS load_count,
    COUNT(DISTINCT file_path) AS file_count,
    MAX(ingested_at)          AS last_load_at
FROM LANDING
GROUP BY 1, 2
ORDER BY load_date DESC, source;

GRANT SELECT ON VIEW LANDING_AUDIT TO ROLE TRANSFORMER;
