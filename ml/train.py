"""
SageMaker training + inference entry point.

This single file implements both:
  - Training (__main__ block) \u2014 runs inside the SageMaker training job
  - Inference (model_fn, input_fn, predict_fn, output_fn) \u2014 runs inside
    the SageMaker batch transform job

SageMaker's built-in scikit-learn container handles the framework install
and invokes these functions automatically following the naming convention.

Training input: CSV files under /opt/ml/input/data/train/
Model output:   /opt/ml/model/model.joblib (auto-uploaded to S3 by SageMaker)

Run locally:
    python train.py --input-dir ./local_data --model-dir ./local_model
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    GradientBoostingClassifier,
    GradientBoostingRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    r2_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("train")


# --------------------------------------------------------------------------- #
#  Training                                                                    #
# --------------------------------------------------------------------------- #

def train(args: argparse.Namespace) -> None:
    input_dir = Path(args.input_dir)
    model_dir = Path(args.model_dir)

    # SageMaker passes hyperparameters as environment variables or CLI flags
    target_col = args.target_column
    task_type = args.task_type  # 'classification' or 'regression'

    log.info(f"Reading training CSVs from {input_dir}")
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {input_dir}")

    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    log.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    if target_col not in df.columns:
        raise ValueError(
            f"Target column '{target_col}' not found. "
            f"Available: {list(df.columns)}"
        )

    y = df[target_col]
    X = df.drop(columns=[target_col])

    # Split by dtype
    numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = X.select_dtypes(exclude=[np.number]).columns.tolist()

    log.info(f"Numeric features: {numeric_cols}")
    log.info(f"Categorical features: {categorical_cols}")

    # Preprocessor
    numeric_pipeline = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
    ])
    categorical_pipeline = Pipeline([
        ("impute", SimpleImputer(strategy="most_frequent")),
        ("encode", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocessor = ColumnTransformer([
        ("num", numeric_pipeline, numeric_cols),
        ("cat", categorical_pipeline, categorical_cols),
    ])

    # Model
    if task_type == "classification":
        estimator = GradientBoostingClassifier(
            n_estimators=args.n_estimators,
            max_depth=args.max_depth,
            random_state=42,
        )
    elif task_type == "regression":
        estimator = GradientBoostingRegressor(
            n_estimators=args.n_estimators,
            max_depth=args.max_depth,
            random_state=42,
        )
    else:
        raise ValueError(f"task_type must be 'classification' or 'regression', got {task_type}")

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("estimator", estimator),
    ])

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42,
        stratify=y if task_type == "classification" else None,
    )

    log.info(f"Training on {len(X_train):,} rows ...")
    pipeline.fit(X_train, y_train)

    # Evaluate
    y_pred = pipeline.predict(X_test)
    metrics: dict[str, float] = {}
    if task_type == "classification":
        metrics["accuracy"] = float(accuracy_score(y_test, y_pred))
        metrics["f1_weighted"] = float(f1_score(y_test, y_pred, average="weighted"))
    else:
        metrics["mae"] = float(mean_absolute_error(y_test, y_pred))
        metrics["r2"] = float(r2_score(y_test, y_pred))

    log.info(f"Validation metrics: {metrics}")

    # Save artifacts
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "model.joblib"
    joblib.dump(pipeline, model_path)

    # Save feature order + target + task type alongside the model so inference
    # knows how to reconstruct the input frame
    metadata = {
        "target_column": target_col,
        "task_type": task_type,
        "feature_columns": list(X.columns),
        "numeric_features": numeric_cols,
        "categorical_features": categorical_cols,
        "metrics": metrics,
    }
    with open(model_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    log.info(f"Model saved to {model_path}")
    log.info(f"Metadata saved to {model_dir / 'metadata.json'}")


# --------------------------------------------------------------------------- #
#  Inference (called by SageMaker at batch-transform time)                    #
# --------------------------------------------------------------------------- #

def model_fn(model_dir: str):
    """Load the trained pipeline + metadata."""
    log.info(f"Loading model from {model_dir}")
    pipeline = joblib.load(Path(model_dir) / "model.joblib")
    with open(Path(model_dir) / "metadata.json") as f:
        metadata = json.load(f)
    return {"pipeline": pipeline, "metadata": metadata}


def input_fn(request_body, content_type: str = "text/csv"):
    """Parse a CSV batch into a DataFrame."""
    import io
    if content_type == "text/csv":
        return pd.read_csv(io.StringIO(request_body))
    if content_type == "application/json":
        return pd.DataFrame(json.loads(request_body))
    raise ValueError(f"Unsupported content type: {content_type}")


def predict_fn(input_data: pd.DataFrame, model) -> pd.DataFrame:
    """Run the model over the input batch and return predictions."""
    pipeline = model["pipeline"]
    feature_cols = model["metadata"]["feature_columns"]

    # Align columns to training order; missing columns become NaN (imputed)
    for col in feature_cols:
        if col not in input_data.columns:
            input_data[col] = np.nan
    X = input_data[feature_cols]

    predictions = pipeline.predict(X)
    return pd.DataFrame({"prediction": predictions})


def output_fn(prediction: pd.DataFrame, accept: str = "text/csv") -> str:
    """Serialize the prediction DataFrame back to CSV."""
    if accept == "text/csv":
        return prediction.to_csv(index=False)
    if accept == "application/json":
        return prediction.to_json(orient="records")
    raise ValueError(f"Unsupported accept type: {accept}")


# --------------------------------------------------------------------------- #
#  CLI entrypoint                                                              #
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default=os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train"))
    parser.add_argument("--model-dir", default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    parser.add_argument("--target-column", default=os.environ.get("TARGET_COLUMN", "target"))
    parser.add_argument("--task-type", default=os.environ.get("TASK_TYPE", "classification"),
                        choices=["classification", "regression"])
    parser.add_argument("--n-estimators", type=int, default=int(os.environ.get("N_ESTIMATORS", 100)))
    parser.add_argument("--max-depth", type=int, default=int(os.environ.get("MAX_DEPTH", 3)))
    return parser.parse_args()


if __name__ == "__main__":
    train(_parse_args())
