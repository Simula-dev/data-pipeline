# ML Workflow

First-ML-model friendly: scikit-learn GradientBoosting with SageMaker's built-in
container. One `train.py` file handles both training and batch inference.

## File layout

| File | Purpose |
|---|---|
| `train.py` | Training logic (`__main__`) and batch-transform inference hooks (`model_fn`, `predict_fn`, `input_fn`, `output_fn`) |
| `requirements.txt` | Additional pip deps on top of the SKLearn base container (usually empty) |
| `config.yaml` | Training config: target column, task type, hyperparameters, instance type |
| `README.md` | This file |

## Training workflow

### 1. Create a training mart in dbt

Create `dbt/models/marts/ml_training_data.sql` with one row per example.
Include the target column and any feature columns.

```sql
{{ config(materialized='table', schema='marts') }}

SELECT
    -- Features
    star_count,
    fork_count,
    open_issue_count,
    language,
    DATEDIFF('day', repo_created_at, repo_updated_at) AS days_active,
    -- Target (example: is the repo "popular"?)
    CASE WHEN star_count > 10000 THEN 1 ELSE 0 END AS is_popular
FROM {{ ref('stg_github_repos') }}
WHERE star_count IS NOT NULL
```

Run `dbt build` to materialize `MARTS.ML_TRAINING_DATA`.

### 2. Unload training data to S3

The ml_export Lambda handles this for you during pipeline runs, but for initial training you can export manually. Use a temporary Lambda or script that connects to RDS and writes a CSV to S3:

```python
# Quick export via pg8000 (run from a Lambda or EC2 in the VPC)
rows = conn.run("SELECT * FROM marts.ml_training_data")
# Write to s3://your-bucket/ml/training/data.csv
```

### 3. Configure the training run

Edit `ml/config.yaml`:

```yaml
training_table: "marts.ml_training_data"
target_column: "is_popular"
task_type: "classification"
n_estimators: 100
max_depth: 3
```

### 4. Launch the SageMaker training job

```bash
# One-time: install the SageMaker SDK
pip install sagemaker boto3 pyyaml

# Kick off training
python scripts/train_sagemaker.py \
    --config ml/config.yaml \
    --raw-bucket data-pipeline-raw-123456789012 \
    --role-arn arn:aws:iam::123456789012:role/data-pipeline-sagemaker \
    --region us-east-1
```

The script will:
1. Launch a SageMaker training job using the built-in sklearn container
2. Wait for it to complete (~5-15 min for small data)
3. Register the trained model in the SageMaker Model Registry
4. Update SSM parameter `/data-pipeline/ml/model_name` with the new model

### 5. Enable the ML step in the pipeline

Trigger the pipeline with `"ml_enabled": true` in the input event:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:...:stateMachine:data-pipeline-orchestrator \
  --input '{"source_name": "...", "base_url": "...", "ml_enabled": true}'
```

## Inference workflow (inside the pipeline)

```
dbt writes marts.ml_inference_input
                │
                ▼
     ExportMLInput Lambda
     (SELECT from PostgreSQL, write CSV to S3)
                │
                ▼
     SageMaker Batch Transform
     (reads CSV, runs predict_fn, writes CSV)
                │
                ▼
     LoadMLPredictions Lambda
     (read CSV from S3, INSERT into marts.ml_predictions)
```

### Batch Transform vs real-time endpoint

**Batch Transform** (used here): pay-per-job, no idle cost. Ideal for scheduled
pipelines that score data in batches.

**Real-time endpoint**: always-on instance, ~$24/day minimum. Only worth it
if you need sub-second latency from external API callers.

## Debugging a failed training job

```bash
# List recent training jobs
aws sagemaker list-training-jobs --sort-by CreationTime --sort-order Descending --max-results 10

# Get details + CloudWatch log group for a specific job
aws sagemaker describe-training-job --training-job-name <job-name>

# Tail the training logs
aws logs tail /aws/sagemaker/TrainingJobs --follow
```

## Debugging a failed batch transform

```bash
aws sagemaker list-transform-jobs --sort-by CreationTime --sort-order Descending --max-results 10
aws logs tail /aws/sagemaker/TransformJobs --follow
```
