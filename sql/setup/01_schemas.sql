-- ==========================================================================
-- 01 \u2014 Schemas and groups
--
-- Run against the `data_pipeline` database in Redshift Serverless after
-- `cdk deploy DataPipeline-Redshift` has completed.
--
-- Connect using the admin credentials from Secrets Manager:
--   Secrets Manager \u2192 data-pipeline/redshift/admin
--
-- The database itself is created by CDK via the Namespace resource.
-- This file only creates schemas, users, and grants inside it.
-- ==========================================================================

\c data_pipeline

-- ---------- Schemas ----------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ml;

COMMENT ON SCHEMA raw          IS 'Landing zone for ingested data (SUPER)';
COMMENT ON SCHEMA staging      IS 'dbt staging models';
COMMENT ON SCHEMA intermediate IS 'dbt intermediate models';
COMMENT ON SCHEMA marts        IS 'dbt mart tables (analytics-ready)';
COMMENT ON SCHEMA ml           IS 'SageMaker inference inputs / outputs';

-- ---------- Groups ----------
-- Redshift doesn't have roles like Snowflake; use groups.
CREATE GROUP pipeline_loader;
CREATE GROUP transformer;

-- Loader: writes to raw schema (used by the load Lambda via Redshift Data API)
GRANT USAGE, CREATE ON SCHEMA raw TO GROUP pipeline_loader;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw TO GROUP pipeline_loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT, INSERT ON TABLES TO GROUP pipeline_loader;

-- Transformer: reads raw, writes to downstream (used by dbt via Fargate)
GRANT USAGE ON SCHEMA raw TO GROUP transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO GROUP transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT ON TABLES TO GROUP transformer;

GRANT USAGE, CREATE ON SCHEMA staging      TO GROUP transformer;
GRANT USAGE, CREATE ON SCHEMA intermediate TO GROUP transformer;
GRANT USAGE, CREATE ON SCHEMA marts        TO GROUP transformer;
GRANT USAGE, CREATE ON SCHEMA ml           TO GROUP transformer;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging      GRANT ALL ON TABLES TO GROUP transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO GROUP transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts        GRANT ALL ON TABLES TO GROUP transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml           GRANT ALL ON TABLES TO GROUP transformer;

-- Marts tables also need to be readable by pipeline_loader for the quality gate
GRANT USAGE ON SCHEMA marts TO GROUP pipeline_loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT ON TABLES TO GROUP pipeline_loader;

-- ---------- Service users ----------
-- Admin is created by CDK; add both groups to it for simplicity during dev.
ALTER GROUP pipeline_loader ADD USER admin;
ALTER GROUP transformer ADD USER admin;
