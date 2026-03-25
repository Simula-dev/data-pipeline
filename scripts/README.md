# Scripts

Helper scripts for operating the data pipeline.

## `upload_kaggle.py`

Downloads a Kaggle dataset to a temp dir, then uploads every file to the
staging S3 bucket. DataSync picks it up on its next run and transfers it
into the raw bucket under `bulk/`.

### Setup

```bash
pip install kaggle boto3
```

Get a Kaggle API token at <https://www.kaggle.com/settings> → "Create New
API Token". Save the downloaded `kaggle.json` to:
- Linux/Mac: `~/.kaggle/kaggle.json`
- Windows: `C:\Users\<you>\.kaggle\kaggle.json`

### Usage

```bash
python scripts/upload_kaggle.py \
    --dataset zynicide/wine-reviews \
    --bucket data-pipeline-staging-123456789012 \
    --prefix kaggle/wine-reviews
```

Options:
- `--dataset` — Kaggle slug, e.g. `zynicide/wine-reviews`
- `--bucket` — Staging bucket (from CDK stack output)
- `--prefix` — S3 prefix (default: `kaggle/<dataset-name>`)
- `--region` — AWS region (default: `AWS_REGION` env var, fallback `us-east-1`)
- `--competition` — Treat as a competition dataset

### Trigger DataSync manually

```bash
aws datasync start-task-execution --task-arn <arn-from-cdk-output>
```

### Beginner-friendly Kaggle dataset suggestions

For a first ML model, pick something with clean CSV data and a clear target column:

| Dataset | Size | ML task | Difficulty |
|---|---|---|---|
| `zynicide/wine-reviews` | 50MB | Price regression / rating classification | Easy |
| `uciml/iris` | <1MB | Flower species classification | Very easy |
| `blastchar/telco-customer-churn` | 1MB | Churn binary classification | Easy |
| `c/titanic` | 1MB | Survival classification (classic tutorial) | Very easy |
| `rtatman/188-million-us-wildfires` | 800MB | Fire cause classification | Intermediate |
| `olistbr/brazilian-ecommerce` | 100MB | Delivery time prediction | Intermediate |
