"""
pytest configuration: put the ingest Lambda source dir on sys.path
so tests can import config, http_client, s3_writer, logger directly
\u2014 the same way Lambda runtime imports them.
"""

import sys
from pathlib import Path

INGEST_LAMBDA_DIR = Path(__file__).resolve().parents[1] / "lambdas" / "ingest"
sys.path.insert(0, str(INGEST_LAMBDA_DIR))
