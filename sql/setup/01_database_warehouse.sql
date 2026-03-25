-- ==========================================================================
-- 01 — Database, warehouse, roles, and schemas
-- Run as ACCOUNTADMIN or a role with equivalent privileges.
-- ==========================================================================

USE ROLE ACCOUNTADMIN;

-- ---------- Warehouse ----------
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         COMMENT = 'Data pipeline transformation warehouse';

-- ---------- Database ----------
CREATE DATABASE IF NOT EXISTS DATA_PIPELINE
    COMMENT = 'Data pipeline: raw -> staging -> marts';

USE DATABASE DATA_PIPELINE;

-- ---------- Schemas ----------
CREATE SCHEMA IF NOT EXISTS RAW          COMMENT = 'Landing zone for ingested data (VARIANT)';
CREATE SCHEMA IF NOT EXISTS STAGING      COMMENT = 'dbt staging models';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE COMMENT = 'dbt intermediate models';
CREATE SCHEMA IF NOT EXISTS MARTS        COMMENT = 'dbt mart tables (analytics-ready)';
CREATE SCHEMA IF NOT EXISTS ML           COMMENT = 'SageMaker inference outputs';

-- ---------- Roles ----------
-- Loader: used by the Lambda that does COPY INTO
CREATE ROLE IF NOT EXISTS PIPELINE_LOADER
    COMMENT = 'Used by load Lambda for COPY INTO RAW.LANDING';

-- Transformer: used by dbt on Fargate
CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Used by dbt to read RAW and write STAGING/INTERMEDIATE/MARTS';

-- ---------- Grant warehouse usage ----------
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE PIPELINE_LOADER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER;

-- ---------- Grant database usage ----------
GRANT USAGE ON DATABASE DATA_PIPELINE TO ROLE PIPELINE_LOADER;
GRANT USAGE ON DATABASE DATA_PIPELINE TO ROLE TRANSFORMER;

-- ---------- Loader permissions: only RAW schema writes ----------
GRANT USAGE, CREATE STAGE, CREATE FILE FORMAT, CREATE TABLE
    ON SCHEMA DATA_PIPELINE.RAW TO ROLE PIPELINE_LOADER;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA DATA_PIPELINE.RAW TO ROLE PIPELINE_LOADER;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA DATA_PIPELINE.RAW TO ROLE PIPELINE_LOADER;

-- ---------- Transformer permissions: read RAW, write downstream ----------
GRANT USAGE ON SCHEMA DATA_PIPELINE.RAW TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA DATA_PIPELINE.RAW TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DATA_PIPELINE.RAW TO ROLE TRANSFORMER;

GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA DATA_PIPELINE.STAGING TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA DATA_PIPELINE.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA DATA_PIPELINE.MARTS TO ROLE TRANSFORMER;

-- ---------- Create service user (password from SSM, rotate regularly) ----------
-- Replace '<CHANGE_ME>' with a strong random password, then store the same
-- value in AWS SSM under /data-pipeline/snowflake/password
CREATE USER IF NOT EXISTS PIPELINE_USER
    PASSWORD = '<CHANGE_ME>'
    DEFAULT_ROLE = PIPELINE_LOADER
    DEFAULT_WAREHOUSE = TRANSFORM_WH
    DEFAULT_NAMESPACE = DATA_PIPELINE.RAW
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service user for data pipeline load Lambda and dbt';

GRANT ROLE PIPELINE_LOADER TO USER PIPELINE_USER;
GRANT ROLE TRANSFORMER    TO USER PIPELINE_USER;
