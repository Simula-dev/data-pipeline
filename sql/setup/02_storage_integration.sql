-- ==========================================================================
-- 02 — Storage integration for S3 raw bucket
--
-- Prerequisites:
--   1. `cdk deploy DataPipeline-Ingestion` has been run
--   2. You have the output values:
--        SnowflakeIntegrationRoleArn  (e.g. arn:aws:iam::123456789012:role/data-pipeline-snowflake-integration)
--        RawBucketName                (e.g. data-pipeline-raw-123456789012)
--
-- Replace the placeholders below before running.
-- ==========================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION S3_RAW_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<PASTE SnowflakeIntegrationRoleArn HERE>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<PASTE RawBucketName HERE>/raw/', 's3://<PASTE RawBucketName HERE>/bulk/')
    COMMENT = 'Data pipeline raw S3 bucket \u2014 ingest + bulk zones';

-- After creation, run this to get the Snowflake-side IAM user + external id:
DESC INTEGRATION S3_RAW_INTEGRATION;

-- Copy the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the output,
-- then update the trust policy on data-pipeline-snowflake-integration role:
--
-- aws iam update-assume-role-policy \
--     --role-name data-pipeline-snowflake-integration \
--     --policy-document file://trust-policy.json
--
-- Where trust-policy.json is:
-- {
--   "Version": "2012-10-17",
--   "Statement": [{
--     "Effect": "Allow",
--     "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" },
--     "Action": "sts:AssumeRole",
--     "Condition": {
--       "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>" }
--     }
--   }]
-- }

GRANT USAGE ON INTEGRATION S3_RAW_INTEGRATION TO ROLE PIPELINE_LOADER;
