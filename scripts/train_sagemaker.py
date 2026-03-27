#!/usr/bin/env python3
"""
Launch a SageMaker training job using the built-in scikit-learn container
and the entry point at ml/train.py.

The training job reads CSV from s3://<raw_bucket>/ml/training/ (which you
first unload from Snowflake \u2014 see the pre-flight section at the bottom),
trains a GradientBoosting model, and writes the artifact to s3://.../ml/models/.

After training completes, the script registers the model in SageMaker
Model Registry under the group defined in ml/config.yaml.

Usage:
    python scripts/train_sagemaker.py \\
        --config ml/config.yaml \\
        --raw-bucket data-pipeline-raw-123456789012 \\
        --role-arn arn:aws:iam::123456789012:role/data-pipeline-sagemaker \\
        --region us-east-1

Pre-flight: before running this, unload your dbt training mart to S3:

    -- In Snowflake
    COPY INTO @RAW.S3_RAW_STAGE/ml/training/
    FROM MARTS.ML_TRAINING_DATA
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION=NONE)
    HEADER = TRUE
    OVERWRITE = TRUE
    SINGLE = TRUE;
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml required. Run: pip install pyyaml sagemaker boto3")
    sys.exit(1)

try:
    import boto3
    import sagemaker
    from sagemaker.sklearn.estimator import SKLearn
except ImportError:
    print("ERROR: sagemaker SDK required. Run: pip install sagemaker boto3")
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="ml/config.yaml", help="Path to training config YAML")
    p.add_argument("--raw-bucket", required=True, help="S3 bucket name (from CDK output)")
    p.add_argument("--role-arn", required=True, help="SageMaker execution role ARN")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--training-prefix", default="ml/training/",
                   help="S3 prefix containing training CSV files")
    p.add_argument("--model-prefix", default="ml/models/",
                   help="S3 prefix where the trained model artifact is uploaded")
    return p.parse_args()


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    print(f"Launching training job for target='{cfg['target_column']}' task={cfg['task_type']}")

    session = sagemaker.Session(boto_session=boto3.Session(region_name=args.region))

    estimator = SKLearn(
        entry_point="train.py",
        source_dir="ml",
        role=args.role_arn,
        instance_count=cfg.get("instance_count", 1),
        instance_type=cfg.get("instance_type", "ml.m5.large"),
        framework_version="1.2-1",
        py_version="py3",
        sagemaker_session=session,
        output_path=f"s3://{args.raw_bucket}/{args.model_prefix}",
        hyperparameters={
            "target-column": cfg["target_column"],
            "task-type": cfg["task_type"],
            "n-estimators": cfg.get("n_estimators", 100),
            "max-depth": cfg.get("max_depth", 3),
        },
        max_run=cfg.get("max_runtime_seconds", 3600),
    )

    training_input = f"s3://{args.raw_bucket}/{args.training_prefix}"
    job_name = f"data-pipeline-train-{int(time.time())}"

    print(f"Training input: {training_input}")
    print(f"Model output:   s3://{args.raw_bucket}/{args.model_prefix}")
    print(f"Job name:       {job_name}\n")

    estimator.fit({"train": training_input}, job_name=job_name, wait=True)
    print(f"\nTraining complete.")
    print(f"Model artifact: {estimator.model_data}")

    # --- Register the trained model in the Model Registry ---
    print(f"\nRegistering model under group: {cfg['model_package_group_name']}")
    model_package = estimator.register(
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.m5.large"],
        transform_instances=["ml.m5.large"],
        model_package_group_name=cfg["model_package_group_name"],
        approval_status=cfg.get("model_approval_status", "PendingManualApproval"),
    )
    print(f"Registered: {model_package.model_package_arn}")

    # --- Update SSM with the model name so the pipeline picks it up ---
    ssm = boto3.client("ssm", region_name=args.region)
    model_name = model_package.model_package_arn.split("/")[-1]
    ssm.put_parameter(
        Name="/data-pipeline/ml/model_name",
        Value=model_name,
        Type="String",
        Overwrite=True,
    )
    print(f"\nSSM /data-pipeline/ml/model_name updated \u2192 {model_name}")
    print(f"The next pipeline run will use this model for Batch Transform.")


if __name__ == "__main__":
    main()
