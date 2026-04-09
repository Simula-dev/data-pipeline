# Contributing

This repo uses **GitHub flow** with category-prefixed branches. The design
favors simplicity (solo developer, short-lived branches) while keeping the
commit history clean enough to read as a portfolio artifact.

## Branching rules

1. **`main` is always deployable.** CI (`test` + `synth`) must pass before
   any merge.
2. **Never commit directly to `main`.** Every change \u2014 including docs,
   typo fixes, and dependency bumps \u2014 goes through a branch and a PR.
3. **Branches are short-lived.** Aim to merge within 1-2 days. Long-running
   branches accumulate drift and merge conflicts.
4. **Delete branches after merge.** Both locally and on the remote.
5. **Squash-merge by default.** Keeps `main` history as one commit per PR,
   regardless of how many WIP commits happened on the branch.

## Branch naming

Format: `<category>/<kebab-case-description>`

| Prefix | When to use | Example |
|---|---|---|
| `feature/` | New capability or user-visible functionality | `feature/add-kaggle-ingest` |
| `fix/` | Bug fix | `fix/datasync-iam-race` |
| `refactor/` | Internal restructuring, no behavior change | `refactor/split-network-stack` |
| `infra/` | CDK stacks / AWS resource changes | `infra/add-redshift-alarm` |
| `ci/` | GitHub Actions, workflow files | `ci/cache-pip-downloads` |
| `docs/` | Documentation only | `docs/branching-strategy` |
| `chore/` | Dependency bumps, housekeeping | `chore/bump-dbt-1.9` |

Pick the most specific category. `infra` beats `feature` when the change
is purely a CDK stack edit. `fix` beats `refactor` when the change
corrects a real bug vs. just cleaning up.

## Commit messages

- **Lowercase, present tense.** `add kaggle ingest handler` not
  `Added kaggle handler` or `Adding kaggle handler`.
- **Subject line \u2264 72 chars.**
- **Body explains WHY, not WHAT.** The diff shows what changed; the
  commit body explains the context, the alternative you rejected, or
  the subtle interaction with another system.
- **One logical change per commit.** If you find yourself writing "and"
  in the subject, split into two commits.

Example:

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

### Title

Same style as a commit message. If the PR has one commit and will be
squash-merged, the PR title becomes the final commit subject.

### Description

Use the template at `.github/pull_request_template.md`. Answer:
- **What** changed
- **Why** it changed
- **How** it was tested
- **Related** issues (use `Closes #N` to auto-close)

### CI

Every PR triggers:
- `test` \u2014 pytest suite (must pass)
- `synth` \u2014 `cdk synth --all` with a dummy account ID (must pass)

The `deploy` job is gated behind `vars.DEPLOY_ENABLED == 'true'` and
only runs on `main` branch pushes, so PRs don't trigger deploys.

### Review policy

This is a solo project; self-review the diff on the PR page before
merging. That one extra pass catches more issues than you'd expect.

## Issue tracking

Open GitHub issues for:
- **Bugs** \u2014 even ones you plan to fix yourself. Creates an audit trail
  and lets you reference from the fix PR.
- **Features** \u2014 describe the what and the why before starting the branch.
- **Blockers** \u2014 external dependencies (e.g., waiting on account
  enrollment, service availability, design decisions).

Reference issues from PRs with `Closes #N` to auto-close on merge.

## Running tests locally

```bash
# One-time setup
python -m venv .venv
.venv/Scripts/activate    # Windows
# source .venv/bin/activate  # Mac/Linux
pip install -r requirements-dev.txt

# Run the test suite
pytest tests/ -v
```

## Validating CDK locally

Requires Docker Desktop running (for the dbt `DockerImageAsset`).

```bash
cdk synth --all
```

No AWS credentials are required for `cdk synth` \u2014 the Network, Compute,
and Redshift stacks have AZ overrides that avoid `DescribeAvailabilityZones`
calls, so synth works fully offline.

## Deploying

```bash
cdk deploy --all --context alert_email=you@example.com
```

See `SETUP.md` for first-time account prep (CDK bootstrap, IAM user, etc.).
