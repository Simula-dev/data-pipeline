#!/usr/bin/env python3
"""
Helper: download a Kaggle dataset locally, then upload it to the
staging S3 bucket where DataSync will pick it up.

Prerequisites:
    pip install kaggle boto3
    # Place your Kaggle API token at ~/.kaggle/kaggle.json
    # (download from https://www.kaggle.com/settings  \u2192 Create New API Token)

Usage:
    python scripts/upload_kaggle.py \\
        --dataset zynicide/wine-reviews \\
        --bucket data-pipeline-staging-123456789012 \\
        --prefix kaggle/wine-reviews
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dataset",
        required=True,
        help="Kaggle dataset slug, e.g. 'zynicide/wine-reviews'",
    )
    p.add_argument(
        "--bucket",
        required=True,
        help="Staging S3 bucket name (from CDK output)",
    )
    p.add_argument(
        "--prefix",
        default=None,
        help="S3 key prefix (defaults to 'kaggle/<dataset-name>')",
    )
    p.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "us-east-1"),
    )
    p.add_argument(
        "--competition",
        action="store_true",
        help="Treat as a competition dataset (uses `kaggle competitions download`)",
    )
    return p.parse_args()


def download_kaggle(dataset: str, dest_dir: Path, is_competition: bool) -> None:
    """Download and unzip a Kaggle dataset into dest_dir."""
    cmd = (
        ["kaggle", "competitions", "download", "-c", dataset]
        if is_competition
        else ["kaggle", "datasets", "download", "-d", dataset]
    )
    cmd.extend(["-p", str(dest_dir), "--unzip"])

    print(f"$ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(
            f"Kaggle download failed (exit {result.returncode}). "
            "Check that `kaggle` CLI is installed and ~/.kaggle/kaggle.json exists."
        )


def upload_directory(local_dir: Path, bucket: str, prefix: str, region: str) -> int:
    """Upload every file under local_dir to s3://bucket/prefix/... Returns file count."""
    s3 = boto3.client("s3", region_name=region)
    count = 0

    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(local_dir).as_posix()
        key = f"{prefix.rstrip('/')}/{relative}"
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  uploading {relative} ({size_mb:.1f} MB) \u2192 s3://{bucket}/{key}")
        s3.upload_file(str(path), bucket, key)
        count += 1

    return count


def main() -> None:
    args = parse_args()
    prefix = args.prefix or f"kaggle/{args.dataset.split('/')[-1]}"

    with tempfile.TemporaryDirectory(prefix="kaggle_") as tmp:
        tmp_path = Path(tmp)
        print(f"Downloading {args.dataset} to {tmp_path}...")
        download_kaggle(args.dataset, tmp_path, args.competition)

        print(f"\nUploading to s3://{args.bucket}/{prefix}/ ...")
        count = upload_directory(tmp_path, args.bucket, prefix, args.region)

    print(f"\nDone. Uploaded {count} file(s). DataSync task will pick up on next run.")
    print(f"Trigger manually with:")
    print(
        f"  aws datasync start-task-execution "
        f"--task-arn <task-arn-from-cdk-output>"
    )


if __name__ == "__main__":
    main()
