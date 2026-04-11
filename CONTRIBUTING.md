# Contributing

This repo follows GitHub flow with category-prefixed branches. It's a solo project, so the process is lightweight - short-lived branches, squash merges, clean history that reads well as a portfolio piece.

## Branching

`main` is always deployable. CI runs tests and `cdk synth` on every PR, and nothing gets merged until both pass.

Every change goes through a branch and a PR, even docs and typo fixes. Branches should be short-lived - aim to merge within a day or two. Delete them after merge, both locally and on the remote.

Squash-merge is the default. That keeps `main` history as one clean commit per PR, regardless of how many messy WIP commits happened along the way.

## Branch naming

Format: `<category>/<kebab-case-description>`

| Prefix | When to use | Example |
|---|---|---|
| `feature/` | New capability or user-visible functionality | `feature/add-kaggle-ingest` |
| `fix/` | Bug fix | `fix/datasync-iam-race` |
| `refactor/` | Internal restructuring, no behavior change | `refactor/split-network-stack` |
| `infra/` | CDK stacks / AWS resource changes | `infra/add-rds-alarm` |
| `ci/` | GitHub Actions, workflow files | `ci/cache-pip-downloads` |
| `docs/` | Documentation only | `docs/branching-strategy` |
| `chore/` | Dependency bumps, housekeeping | `chore/bump-dbt-1.9` |

Pick the most specific category. `infra` beats `feature` when the change is purely a CDK stack edit. `fix` beats `refactor` when the change corrects a real bug vs. just cleaning things up.

## Commit messages

Lowercase, present tense. `add kaggle ingest handler` not `Added kaggle handler`.

Keep the subject line under 72 characters. The body should explain *why*, not *what* - the diff already shows what changed. If you're writing "and" in the subject, it's probably two commits.

Good example:

```
fix: datasync location creation races the iam policy attachment

Root cause: CfnLocationS3 runs a create-time access probe against the
bucket. CDK's grant_read_write adds policies via an auto-generated
DataSyncRole/DefaultPolicy, but there's no ordering dependency between
that policy and the CfnLocationS3 resource. CloudFormation creates them
in parallel and the probe fires before the policy attaches.

Fix: replace grant_* with an explicit iam.Policy, then add
.node.add_dependency(datasync_policy) on both locations.
```

## Pull requests

Title follows the same style as a commit message. If the PR is a single squash-merged commit, the PR title becomes the final commit subject.

For the description, use the template at `.github/pull_request_template.md`. Cover what changed, why, how you tested it, and any related issues (use `Closes #N` to auto-close them).

Every PR triggers two CI checks:
- `test` - the pytest suite
- `synth` - `cdk synth --all` with a dummy account ID

The deploy job only runs on pushes to `main` (gated behind `vars.DEPLOY_ENABLED`), so PRs won't accidentally trigger deploys.

Since this is a solo project, the review step is a self-review on the PR page before hitting merge. That extra pass catches more issues than you'd think.

## Issue tracking

Open GitHub issues for bugs, features, and blockers - even ones you plan to fix immediately. It creates an audit trail and lets you cross-reference from PRs with `Closes #N`.

## Running tests locally

```bash
python -m venv .venv
.venv/Scripts/activate    # Windows
# source .venv/bin/activate  # Mac/Linux
pip install -r requirements-dev.txt

pytest tests/ -v
```

## Validating CDK locally

Requires Docker Desktop running (for the dbt `DockerImageAsset`):

```bash
cdk synth --all
```

No AWS credentials needed for synth. The Network and RDS stacks have AZ overrides that skip `DescribeAvailabilityZones` calls, so it works fully offline.

## Deploying

```bash
cdk deploy --all --context alert_email=you@example.com
```

See [SETUP.md](SETUP.md) for first-time account prep (CDK bootstrap, IAM, etc.).
