-- ==========================================================================
-- 03 — NDJSON file format and external S3 stage
--
-- Run AFTER 02_storage_integration.sql and after the trust policy
-- has been updated with Snowflake's IAM user and external ID.
-- ==========================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DATA_PIPELINE;
USE SCHEMA RAW;

-- ---------- File format: newline-delimited JSON ----------
-- The ingest Lambda writes one JSON object per line.
CREATE OR REPLACE FILE FORMAT NDJSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE
    COMPRESSION = AUTO
    COMMENT = 'Newline-delimited JSON produced by the ingest Lambda';

-- ---------- External stage ----------
-- Points at the raw bucket via the storage integration from step 02.
CREATE OR REPLACE STAGE S3_RAW_STAGE
    STORAGE_INTEGRATION = S3_RAW_INTEGRATION
    URL = 's3://<PASTE RawBucketName HERE>/'
    FILE_FORMAT = NDJSON_FORMAT
    COMMENT = 'External stage on the raw S3 bucket';

GRANT USAGE ON FILE FORMAT NDJSON_FORMAT TO ROLE PIPELINE_LOADER;
GRANT USAGE ON FILE FORMAT NDJSON_FORMAT TO ROLE TRANSFORMER;
GRANT USAGE ON STAGE       S3_RAW_STAGE  TO ROLE PIPELINE_LOADER;
GRANT USAGE ON STAGE       S3_RAW_STAGE  TO ROLE TRANSFORMER;

-- Sanity check: list the stage root. Should return S3 objects if any exist.
LIST @S3_RAW_STAGE;
