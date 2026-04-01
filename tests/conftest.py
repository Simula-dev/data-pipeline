"""
pytest configuration:
  1. Set AWS env vars so module-level boto3 clients can import cleanly.
     (In Lambda runtime AWS_REGION is auto-set; in the test env we fake it.)
  2. Put the ingest Lambda source dir on sys.path so tests can `import config`
     directly \u2014 mirroring how Lambda itself imports modules.
"""

import os
import sys
from pathlib import Path

# Must run before any test module imports boto3 at module level
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

INGEST_LAMBDA_DIR = Path(__file__).resolve().parents[1] / "lambdas" / "ingest"
sys.path.insert(0, str(INGEST_LAMBDA_DIR))
