"""Unit tests for S3RawWriter using moto to mock S3."""

import json
import re

import boto3
import pytest
from moto import mock_aws

from s3_writer import S3RawWriter


BUCKET = "test-raw-bucket"


@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


def test_write_records_creates_ndjson_object(s3_bucket):
    writer = S3RawWriter(bucket=BUCKET, s3_client=s3_bucket)
    records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    result = writer.write_records("test_source", iter(records), run_id="abc123")

    assert result["recordCount"] == 2
    assert result["source"] == "test_source"
    assert result["s3Bucket"] == BUCKET
    assert result["s3Key"].startswith("raw/test_source/")
    assert result["s3Key"].endswith(".ndjson")
    assert "contentHash" in result

    # Verify the object exists and contains valid NDJSON
    obj = s3_bucket.get_object(Bucket=BUCKET, Key=result["s3Key"])
    body = obj["Body"].read().decode()
    lines = [json.loads(line) for line in body.strip().split("\n")]
    assert lines == records


def test_write_zero_records_skips_upload(s3_bucket):
    writer = S3RawWriter(bucket=BUCKET, s3_client=s3_bucket)
    result = writer.write_records("empty_source", iter([]))

    assert result["recordCount"] == 0
    assert result["s3Key"] is None

    # Nothing should have been written
    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)
    assert objects.get("Contents", []) == []


def test_s3_key_is_date_partitioned(s3_bucket):
    writer = S3RawWriter(bucket=BUCKET, s3_client=s3_bucket)
    result = writer.write_records("my_src", iter([{"x": 1}]))

    # Matches raw/<src>/YYYY/MM/DD/HHMMSS_<hash>[_<runid>].ndjson
    pattern = r"^raw/my_src/\d{4}/\d{2}/\d{2}/\d{6}_[a-f0-9]{16}\.ndjson$"
    assert re.match(pattern, result["s3Key"]), f"Unexpected key: {result['s3Key']}"


def test_metadata_is_set_on_object(s3_bucket):
    writer = S3RawWriter(bucket=BUCKET, s3_client=s3_bucket)
    result = writer.write_records("meta_src", iter([{"a": 1}, {"a": 2}, {"a": 3}]))

    obj = s3_bucket.head_object(Bucket=BUCKET, Key=result["s3Key"])
    assert obj["Metadata"]["source"] == "meta_src"
    assert obj["Metadata"]["record_count"] == "3"
    assert "content_hash" in obj["Metadata"]
    assert "ingested_at" in obj["Metadata"]
    assert obj["ContentType"] == "application/x-ndjson"
