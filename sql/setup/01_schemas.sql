-- ==========================================================================
-- 01 -- Schemas, roles, and grants for RDS PostgreSQL
--
-- Run against the `data_pipeline` database after `cdk deploy DataPipeline-RDS`.
-- Connect as the admin user (credentials in Secrets Manager: data-pipeline/rds/admin).
-- ==========================================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ml;

COMMENT ON SCHEMA raw          IS 'Landing zone for ingested data (jsonb)';
COMMENT ON SCHEMA staging      IS 'dbt staging models';
COMMENT ON SCHEMA intermediate IS 'dbt intermediate models';
COMMENT ON SCHEMA marts        IS 'dbt mart tables (analytics-ready)';
COMMENT ON SCHEMA ml           IS 'SageMaker inference inputs / outputs';

-- Roles
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_loader') THEN
        CREATE ROLE pipeline_loader;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'transformer') THEN
        CREATE ROLE transformer;
    END IF;
END $$;

-- Loader: writes to raw schema
GRANT USAGE, CREATE ON SCHEMA raw TO pipeline_loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT, INSERT ON TABLES TO pipeline_loader;

-- Transformer: reads raw, writes to downstream
GRANT USAGE ON SCHEMA raw TO transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT ON TABLES TO transformer;

GRANT USAGE, CREATE ON SCHEMA staging      TO transformer;
GRANT USAGE, CREATE ON SCHEMA intermediate TO transformer;
GRANT USAGE, CREATE ON SCHEMA marts        TO transformer;
GRANT USAGE, CREATE ON SCHEMA ml           TO transformer;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging      GRANT ALL ON TABLES TO transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts        GRANT ALL ON TABLES TO transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml           GRANT ALL ON TABLES TO transformer;

-- Marts need to be readable by pipeline_loader for the quality gate
GRANT USAGE ON SCHEMA marts TO pipeline_loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT ON TABLES TO pipeline_loader;

-- Admin user gets both roles
GRANT pipeline_loader TO admin;
GRANT transformer TO admin;
