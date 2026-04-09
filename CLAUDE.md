# CLAUDE.md

**Purpose:** context for Claude Code (or any future Claude instance) working
in this repository. Exists so that a new session can become productive
within minutes without reconstructing project state from conversation history.

**Keep it updated.** At the end of any session that changes the architecture,
completes a major milestone, or discovers a new gotcha, update this file.
Authoritative sources:
- **Task state** → GitHub issues
- **Architecture state** → the code
- **Narrative layer tying them together** → this file

---

## Project identity

**What this is:** Production-quality AWS data pipeline built with CDK. Ingests
HTTP and bulk data → Redshift Serverless → dbt transformations → SageMaker
batch inference → data quality gate → notifications. Orchestrated by
Step Functions.

**What this is NOT:** a Unity project, a game, or any Heir or Harb content.
This repo is deliberately kept separate from the user's other work so it can
be used as a portfolio piece for data engineering contract work.

**Primary goal:** a defensible, production-grade portfolio demo for data /
analytics engineering. Every architectural decision should be justifiable
under "how would this look in a real client engagement?"

---

## Current status

**Last updated:** 2026-04-08

**Phase:** C (initial AWS deployment)

### Deployed stacks (5/8)

- `DataPipeline-Network` — VPC, 3 AZs (required for Redshift Serverless)
- `DataPipeline-Ingestion` — S3 raw bucket + 5 Lambdas
- `DataPipeline-Monitoring` — SNS + notify Lambda + CloudWatch alarms + dashboard
- `DataPipeline-SageMaker` — execution role + model package group
- `DataPipeline-DataSync` — staging bucket + DataSync S3→S3 task

### Pending stacks (3/8)

- `DataPipeline-Redshift` — **blocked on account enrollment (issue #2)**
- `DataPipeline-Compute` — blocked on Redshift
- `DataPipeline-Orchestration` — blocked on Compute

### Active blocker

AWS account `004947744611` is not enrolled in Redshift Serverless.
Confirmed via `aws redshift-serverless list-workgroups` which returns
`SubscriptionRequiredException`. Resolution path is in issue #2: start a
"Create workgroup" wizard in the Redshift console and click through to the
review step, then cancel before actually creating. The act of starting the
wizard triggers service enrollment on the account.

### Authoritative backlog

https://github.com/entity-black/data-pipeline/issues

Current open issues:

| # | Title | Labels |
|---|---|---|
| 2 | Phase C: Enroll AWS account in Redshift Serverless | `phase-c` `blocked` `infrastructure` `warehouse` |
| 5 | Phase C: Deploy Redshift + Compute + Orchestration stacks | `phase-c` `infrastructure` |
| 3 | Phase C: Run Redshift SQL setup scripts | `phase-c` `warehouse` |
| 4 | Phase C: Execute first end-to-end pipeline run | `phase-c` |
| 6 | Add real Kaggle dataset and production dbt models | `enhancement` `dbt` `warehouse` |
| 7 | Add Snowflake as secondary warehouse target (Path 2) | `enhancement` `warehouse` `infrastructure` |

---

## Architecture

### Pipeline flow

```
IngestData (Lambda, HTTP + S3 write)
   ↓
LoadToRedshift (Lambda via Data API, COPY via temp table)
   ↓
RunDbtTransformation (ECS Fargate, `dbt build`)
   ↓
MLEnabled? (Choice)
   ├─ true:  ExportMLInput → BatchTransform → LoadMLPredictions
   └─ false: skip to quality gate
   ↓
DataQualityGate (Lambda, JSON config, Redshift Data API)
   ↓
QualityPassed? (Choice)
   ├─ true:  NotifySuccess (Lambda → SNS + EMF metrics)
   └─ false: NotifyFailure (Lambda → SNS + EMF metrics)
```

### Stack dependency order

```
Network ──────┬─> Ingestion ──> Redshift ──> Compute ──> Orchestration
              │                 ↑
              │                 │
              └─────────────────┘
                 (Redshift uses Network VPC)

Independent:  DataSync (uses Ingestion raw bucket)
              SageMaker (uses Ingestion raw bucket)
              Monitoring
```

Why this shape: Network owns the VPC, Redshift owns the dbt security group
(so Redshift can grant 5439 ingress from it without circular deps), Compute
imports Redshift admin secret + SSM params by explicit cross-stack reference.

### Warehouse: Redshift Serverless (NOT Snowflake)

**Important historical note.** The project was originally designed for
Snowflake. You'll see Snowflake in the git history through commit `5a886cc`.
Commit `6429f3b` is the full refactor to Redshift Serverless — triggered
because Snowflake's signup flow rejects non-business emails and the user
couldn't register at the time. **The current codebase is 100% Redshift.**
Don't be confused by old Snowflake references in commit messages or issue
history.

The user now has a custom-domain email (`simulacrum@simu-dev.com` via
Cloudflare Email Routing) so Snowflake _can_ be added back later as a
multi-warehouse Path 2 addition. Issue #7 tracks that work.

### Redshift Data API, not JDBC

All 4 data Lambdas (`load`, `ml_export`, `ml_load`, `quality_gate`) use
`boto3.client('redshift-data')`. No JDBC, no C extensions, no Docker
bundling required. Huge simplification over the Snowflake era which needed
`snowflake-connector-python` bundled in Linux containers.

The pattern is `execute_statement` or `batch_execute_statement` → poll
`describe_statement` until `FINISHED` → `get_statement_result` for any
result set. See `lambdas/load/redshift_client.py` as the canonical
implementation (duplicated per-Lambda because they're packaged separately).

### dbt runs on ECS Fargate

- Image built via CDK `DockerImageAsset` from `dbt/Dockerfile`
- `python:3.12-slim` + `dbt-core==1.8.*` + `dbt-redshift==1.8.*` + `libpq-dev`
- Credentials injected at container start via ECS `secrets` integration
  (Redshift admin password from Secrets Manager + db name from SSM)
- Runs `dbt build --profiles-dir /app/dbt` on each invocation
- Step Functions invokes via `EcsRunTask` with `RUN_JOB` integration pattern

---

## Repository layout

```
data-pipeline/
├── app.py                          # CDK entry point (wires 8 stacks)
├── cdk.json                        # Account 004947744611, region us-east-1
│
├── cdk/stacks/
│   ├── network_stack.py            # VPC (3 AZs, NAT gateway)
│   ├── ingestion_stack.py          # S3 raw + 5 Lambdas (pure python)
│   ├── redshift_stack.py           # Serverless ns + wg + dbt SG + admin secret
│   ├── compute_stack.py            # ECS Fargate cluster + dbt task def
│   ├── datasync_stack.py           # DataSync task (with IAM race fix)
│   ├── sagemaker_stack.py          # execution role + model registry
│   ├── monitoring_stack.py         # SNS + notify Lambda + alarms + dashboard
│   └── stepfunctions_stack.py      # state machine
│
├── lambdas/
│   ├── ingest/        # HTTP ingestion (urllib3, pagination, auth, rate limit)
│   ├── load/          # Redshift COPY via temp table pattern
│   ├── ml_export/     # Redshift UNLOAD to S3 (for batch transform input)
│   ├── ml_load/       # Redshift COPY predictions back after batch transform
│   ├── quality_gate/  # JSON-config-driven checks via Data API
│   └── notify/        # Message format + EMF metrics + SNS publish
│
├── dbt/
│   ├── Dockerfile              # built by CDK DockerImageAsset
│   ├── profiles.yml            # env var sourced, Redshift ra3
│   ├── dbt_project.yml         # layer configs, +schema override
│   ├── macros/
│   │   ├── generate_schema_name.sql   # strips dbt default prefix
│   │   └── parse_landing_source.sql   # reusable SUPER source parser
│   └── models/
│       ├── sources.yml
│       ├── staging/            # stg_github_repos, stg_coingecko_markets (examples)
│       ├── intermediate/       # int_github_repos_latest (ephemeral)
│       └── marts/              # dim_repos, fct_daily_repo_metrics (incremental)
│
├── ml/
│   ├── train.py                # sklearn GradientBoosting + SM inference hooks
│   ├── config.yaml             # target col, task type, hyperparams
│   └── README.md               # full training workflow walkthrough
│
├── sql/setup/
│   ├── 01_schemas.sql          # schemas, groups, grants
│   ├── 02_tables.sql           # raw.landing (SUPER) + marts.ml_predictions
│   └── README.md               # how to run via Query Editor v2 or Data API
│
├── scripts/
│   ├── upload_kaggle.py        # Kaggle → staging bucket helper
│   ├── train_sagemaker.py      # launch SM training job + register model
│   └── build_dbt_image.{sh,ps1}  # manual ECR push for iteration
│
├── tests/                      # 58 pytest tests, fully offline
│   ├── conftest.py             # sets fake AWS creds so boto3 imports don't fail
│   ├── test_config.py
│   ├── test_http_client.py
│   ├── test_s3_writer.py
│   ├── test_load_handler.py
│   ├── test_quality_checks.py
│   └── test_notify_formatter.py
│
├── examples/                   # ready-to-use Step Functions input payloads
│   ├── event_jsonplaceholder.json
│   ├── event_github_repos.json
│   ├── event_coingecko.json
│   ├── event_load_direct.json
│   └── event_with_ml.json
│
├── .github/
│   ├── workflows/ci.yml        # test + synth + deploy (deploy gated on var)
│   └── pull_request_template.md
│
├── CLAUDE.md                   # THIS FILE
├── CONTRIBUTING.md             # branching strategy, commit style, PR workflow
├── SETUP.md                    # first-time local env + AWS setup
└── README.md
```

---

## Environment

**Local dev machine:** Windows 11 Enterprise

- Python 3.12 at `C:\Users\Entity\AppData\Local\Programs\Python\Python312\`
- Node 24 via winget/npm
- AWS CLI v2 at `C:\Program Files\Amazon\AWSCLIV2\`
- CDK CLI via `npm install -g aws-cdk`
- Docker Desktop with WSL2 backend
- GitHub CLI at `C:\Program Files\GitHub CLI\gh.exe`
- venv at `data-pipeline/.venv/`

**Git Bash path mangling gotcha:** when running `docker` commands manually
from Git Bash, Unix-style paths get converted to Windows paths before reaching
the Docker engine. Example: `-v /asset-input:/asset-input` becomes
`-v C:/Program Files/Git/asset-input:/asset-input`. CDK doesn't hit this
because it spawns docker via Node `child_process` directly. If you need to
manually test a bundling command, use `cmd.exe //c` or prepend
`MSYS_NO_PATHCONV=1`.

**Shell PATH gotcha:** binaries installed via `winget` in a running bash
session aren't on PATH until the shell restarts. Use absolute paths in
the session where an install just happened:

```bash
export PATH="/c/Program Files/Docker/Docker/resources/bin:/c/Program Files/Amazon/AWSCLIV2:/c/Program Files/GitHub CLI:$PATH"
```

**AWS account:** `004947744611` in `us-east-1`
**IAM user:** `data-pipeline-admin` (AdministratorAccess)
**GitHub user:** `entity-black`
**Repo:** https://github.com/entity-black/data-pipeline (private)

---

## Known gotchas and resolved bugs

These all appear in the git history. Don't re-discover them.

### 1. `log_event(..., event=event)` kwarg collision
The `log_event(logger, event, **fields)` function signature uses `event` as
a positional parameter name. Passing `event=event` triggers
`TypeError: log_event() got multiple values for argument 'event'`.
Use `payload=event` instead. Fixed in 5 handlers in commit `af51665`.

### 2. Docker bundling on Windows WSL2: permission denied
CDK's default `-u 1000:1000` Docker bundling doesn't match WSL2 bind mount
ownership. Add `user="root"` to `BundlingOptions`. Currently moot because all
data Lambdas are pure-Python + boto3, but still applies to the dbt
`DockerImageAsset` and any future Lambda that needs C extensions.
See commit `2efbfe7`.

### 3. `ec2.Vpc(availability_zones=[...])` is silently ignored
That's not a real parameter on the Vpc construct. To provide static AZs
(and avoid an AWS API call during synth), override `Stack.availability_zones`
as a property. See `NetworkStack`, `RedshiftStack`, any other stack that
creates or references a VPC.

### 4. Circular CDK dependency: Compute vs Redshift
Compute needs Redshift admin secret + SSM params; Redshift needs a VPC + SG
that was originally owned by Compute. Fix: split out `NetworkStack` that owns
the VPC, and have `RedshiftStack` own the `dbt_security_group` that Compute
imports. Compute imports Redshift creds explicitly as constructor params
(cross-stack ref) which creates a real CloudFormation dependency edge.
See commit `749c4b9`.

### 5. DataSync location creation races the IAM policy attachment
`grant_read_write` on a bucket creates an auto-generated
`DataSyncRole/DefaultPolicy` iam.Policy with no ordering dependency on
`CfnLocationS3`. CloudFormation creates them in parallel and DataSync's
create-time `s3:ListBucket` probe fails. Fix: create the policy as an
explicit `iam.Policy` construct, then add
`location.node.add_dependency(datasync_policy)` on both CfnLocationS3
resources. See commit `212a46e`.

### 6. EC2 Security Group descriptions reject non-ASCII
Em-dashes (`—`) fail with "Character sets beyond ASCII are not supported".
Use plain `-`. Fixed in commit `d50e5e8`. Be mindful of this for any new
SG construct.

### 7. New AWS accounts need explicit Redshift Serverless enrollment
`SubscriptionRequiredException` until the user clicks through the
"Create workgroup" wizard in the Redshift console. Just visiting the
dashboard is NOT enough. **Active blocker as of this writing** — see issue #2.

### 8. Redshift SQL dialect differences from Snowflake
| Snowflake | Redshift |
|---|---|
| `COUNT_IF(col IS NULL)` | `SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END)` |
| `TIMESTAMPDIFF('hour', a, b)` | `DATEDIFF('hour', a, b)` |
| `data:field::TYPE` | `data.field::type` (SUPER dot notation) |
| `VARIANT` | `SUPER` |
| Subquery without alias OK | Subquery REQUIRES explicit alias |
| `CURRENT_TIMESTAMP()` | `CURRENT_TIMESTAMP` (no parens) |

All refactored in commit `6429f3b`.

### 9. Test module cache collision
Multiple test files add different Lambda dirs to `sys.path`, then
`import handler` picks up whichever one is first on the path.
`tests/test_load_handler.py::_fresh_handler_module` force-prepends its
Lambda dir and clears `sys.modules` to isolate each test's imports.
Pattern to copy if you add more per-Lambda tests.

### 10. `log_event(..., event=event)` redux
Same bug pattern, different entry point. `tests/conftest.py` sets fake AWS
env vars (`AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, etc.) so that Lambda
modules which do module-level `boto3.client('ssm')` can be imported during
test collection without a real AWS credential chain.

---

## Communication and workflow conventions

**The user prefers:**
- **Concise responses.** Lead with the answer or action. Skip preamble.
- **Production-quality code, not quick hacks.** Each step gets real logic,
  error handling, and tests.
- **One step at a time.** Build a working, solid thing, then move to the
  next. Don't try to do 3 things in parallel that should be 3 separate
  focused efforts.
- **TodoWrite for multi-step work.** Update status as you go.
- **Explicit branching per `CONTRIBUTING.md`.** No direct-to-main commits
  (except for the pre-strategy commits at the bottom of the history).
- **Squash merges** so `main` stays one-commit-per-PR.
- **Honest trade-off discussions** when there's a real architectural
  decision to make. Present options with pros/cons, give a recommendation,
  let the user decide.

**The user does NOT want:**
- Excessive explanation of obvious things
- "Let me think step by step" filler
- Time estimates unless specifically asked
- Pushing back on their architectural decisions (the decision happens at
  discussion time; once they've chosen, execute)
- Wrap-up paragraphs restating what you just did

**Session-end ritual** — when the user says "pause", "end session", or
similar:
1. Verify the working tree is clean (all commits pushed)
2. Give a structured progress report (deployed vs pending, blockers, next steps)
3. Update this CLAUDE.md file if architecture changed
4. Close any completed todos, clearly mark blocked ones

---

## How to resume work

### Quickstart sanity check

```bash
cd C:/Users/Entity/Projects/data-pipeline
source .venv/Scripts/activate
pytest tests/                        # should show 58 passed
gh issue list --state open           # current backlog
git log --oneline -10                # recent history
aws sts get-caller-identity          # confirms AWS creds still work
```

### To continue Phase C (current blocker)

Issue #2 is the active blocker. Once the user has enrolled in Redshift
Serverless:

```bash
# Verify enrollment
aws redshift-serverless list-workgroups
# Should return an empty list, NOT SubscriptionRequiredException

# Clean up the failed Redshift stack if it's still in ROLLBACK_COMPLETE
aws cloudformation describe-stacks --stack-name DataPipeline-Redshift 2>&1 | head -5
aws cloudformation delete-stack --stack-name DataPipeline-Redshift
# Wait ~30s for deletion, confirm gone

# Deploy
cdk deploy --all --require-approval never --context alert_email=simulacrum@simu-dev.com
```

After all 8 stacks are up:
1. Close issue #5, move to issue #3 (SQL setup via Query Editor v2 or Data API)
2. Close #3, move to #4 (E2E test with `examples/event_jsonplaceholder.json`)
3. Close #4 → Phase C complete

### To start a new feature branch

Per `CONTRIBUTING.md`:

```bash
git checkout main
git pull
git checkout -b feature/descriptive-name    # or fix/, refactor/, infra/, etc.

# ... make changes, commit incrementally ...

git push -u origin feature/descriptive-name
gh pr create --title "..." --body "Closes #N"

# Wait for CI (test + synth)
gh pr checks <pr-number>

# Merge and clean up
gh pr merge <pr-number> --squash --delete-branch
git checkout main
git pull
git branch -D feature/descriptive-name
```

### To diagnose a failed pipeline run

After Phase C is complete:

```bash
# Find the state machine execution
aws stepfunctions list-executions \
    --state-machine-arn arn:aws:states:us-east-1:004947744611:stateMachine:data-pipeline-orchestrator \
    --max-items 5

# Get details on a specific execution
aws stepfunctions describe-execution --execution-arn <arn>

# Check the input / output / error details
aws stepfunctions get-execution-history --execution-arn <arn> --max-items 20

# Lambda logs
aws logs tail /aws/lambda/DataPipeline-Ingestion-LoadFunction --follow

# Fargate dbt logs
aws logs tail /data-pipeline/dbt --follow
```

---

## Update log

- **2026-04-08** — Initial creation. Phase C in progress, Redshift enrollment
  blocked on issue #2. Six GitHub issues tracking remaining work. Branching
  strategy formalized in `CONTRIBUTING.md` (PR #1). 5/8 stacks deployed.
