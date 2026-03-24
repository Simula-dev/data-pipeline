"""
SageMaker training script entry point.

Run locally:  python train.py
Run on SM:    passed automatically by SageMaker training job.

Data is read from /opt/ml/input/data/train/ (SageMaker convention).
Trained model artifact is written to /opt/ml/model/ for deployment.
"""

import os
import json
import argparse


def train(args):
    """
    Stub training loop.
    Replace with your actual model training logic
    (scikit-learn, XGBoost, PyTorch, etc.).
    """
    input_dir = args.input_dir
    model_dir = args.model_dir

    print(f"Reading training data from: {input_dir}")
    print(f"Writing model artifact to:  {model_dir}")

    # TODO: load data, train model, save artifact
    model_artifact = {"model": "placeholder", "version": "0.1"}

    os.makedirs(model_dir, exist_ok=True)
    with open(os.path.join(model_dir, "model.json"), "w") as f:
        json.dump(model_artifact, f)

    print("Training complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        default=os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train"),
    )
    parser.add_argument(
        "--model-dir",
        default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"),
    )
    args = parser.parse_args()
    train(args)
