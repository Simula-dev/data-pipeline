"""
Local bundling for Lambda assets.

Used instead of Docker bundling when the Lambda's dependencies are pure
Python (no C extensions). CDK tries the local bundler first; if it
succeeds, Docker is never invoked.

This is a significant DX win on Windows where Docker Desktop + WSL2
has repeated friction (slow startup, permission issues, daemon crashes).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

import jsii
import aws_cdk as cdk


@jsii.implements(cdk.ILocalBundling)
class LocalPipBundling:
    """
    Runs `pip install -r requirements.txt -t <output>` on the host,
    then copies the Lambda source files alongside the installed packages.

    Only works for pure-Python dependencies (pg8000, requests, etc.).
    Falls back to Docker if pip fails or a compiled extension is needed.
    """

    def __init__(self, source_dir: str):
        self._source_dir = source_dir

    def try_bundle(self, output_dir: str, *, image, **kwargs) -> bool:  # type: ignore[override]
        req_file = os.path.join(self._source_dir, "requirements.txt")

        # Install dependencies from requirements.txt
        if os.path.exists(req_file):
            result = subprocess.run(
                [
                    sys.executable, "-m", "pip", "install",
                    "--no-cache-dir",
                    "-r", req_file,
                    "-t", output_dir,
                    "--quiet",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(f"Local bundling pip install failed: {result.stderr}")
                return False  # fall back to Docker

        # Copy all source files (skip requirements.txt and __pycache__)
        for item in os.listdir(self._source_dir):
            if item in ("__pycache__", ".pytest_cache", "requirements.txt"):
                continue
            src = os.path.join(self._source_dir, item)
            dst = os.path.join(output_dir, item)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
            elif os.path.isdir(src):
                shutil.copytree(src, dst, dirs_exist_ok=True)

        return True
