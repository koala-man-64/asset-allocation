# Project & Workflow Audit Report

### 1. Executive Summary
- Phase 3 adds a new Postgres migration (`0004_backtest_runs.sql`), introduces a Postgres-backed run store for the backtest API, and switches the Container Apps manifest to `BACKTEST_RUN_STORE_MODE=postgres`. CI workflows remain structurally unchanged.
- Workflow security posture remains similar to earlier phases: actions are pinned and `permissions:` blocks exist; deploy secrets are injected only in deploy steps.
- The primary governance risk remains **secrets exposure via CI workflows and deploy-time templating** (existing posture): `envsubst` writes secrets into temporary YAML files, and the PR test workflow uses cloud-related secrets.
- **Risk rating: Medium** (acceptable for trusted contributors with guardrails; revisit if accepting untrusted fork PRs).

### 2. Scope & Assumptions
- **In scope:** Phase 3 change set (migration `0004`, backtest API config/manifest changes, docs), and GitHub Actions workflows relevant to deploy/tests.
- **Excluded:** Azure subscription/resource posture and runtime network policy validation (must be validated operationally).
- **Assumptions:** GitHub secrets are configured only in repo settings; deploy logs do not echo rendered YAML contents; PR contributors are trusted (or workflows are restricted).

### 3. Inventory Snapshot
- **Languages/runtimes:** Python 3.10 (`pyproject.toml:1`), GitHub Actions.
- **Workflows present:** `.github/workflows/deploy.yml`, `.github/workflows/run_tests.yml`, `.github/workflows/trigger_all_jobs.yml`, `.github/workflows/lint_workflows.yml` (`ls -la .github/workflows`).
- **Postgres migrations:** `deploy/sql/postgres/migrations/0001_*` through `0004_*`.
- **Governance docs:** `CONTRIBUTING.md`, `SECURITY.md`, `.github/CODEOWNERS`.
- **AGENTS.md:** none discovered via filename scan in repo root (repo uses `.codex/skills/*` per session setup).

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None introduced by Phase 3 change set.

#### 4.2 Major
- **[Deploy-time templating writes DSN secrets into temporary YAML]**
  - **Evidence:** `.github/workflows/deploy.yml:301` renders `deploy/app_backtest_api.yaml` via `envsubst` into `deploy/app_backtest_api.tmp.yaml`, and the deploy step receives `BACKTEST_POSTGRES_DSN` as an environment secret (`.github/workflows/deploy.yml:267`).
  - **Why it matters:** any accidental printing or retention of rendered YAML could leak DSNs.
  - **Recommendation:** keep the current pattern of not printing the rendered YAML and deleting the temp file; avoid `set -x`; keep DSNs confined to deploy workflow only.
  - **Acceptance Criteria:** deploy logs do not print DSN values; temp YAML files are removed; DSNs are referenced only in deploy workflow contexts.
  - **Owner Suggestion:** DevOps Agent / Project Workflow Auditor Agent

- **[PR test workflow uses secrets (existing posture)]**
  - **Evidence:** `.github/workflows/run_tests.yml:3` triggers on `pull_request` and injects cloud secrets for test execution (`.github/workflows/run_tests.yml:49`).
  - **Why it matters:** if untrusted fork PRs are allowed, attacker-controlled code could exfiltrate secrets during CI.
  - **Recommendation:** if PRs can be untrusted, split workflows so PR jobs run without secrets (use mocks), or restrict secrets to trusted actors/branches.
  - **Acceptance Criteria:** fork/untrusted PRs do not receive high-value secrets; trusted PRs remain covered by the required test gates.
  - **Owner Suggestion:** DevOps Agent + QA Release Gate Agent

#### 4.3 Minor
- **[Phase 3 enables Postgres mode in the backtest app manifest]**
  - **Evidence:** `deploy/app_backtest_api.yaml:73` sets `BACKTEST_RUN_STORE_MODE=postgres`.
  - **Why it matters:** deployments now depend on Postgres migration state and DSN secrets being present.
  - **Recommendation:** ensure migrations are applied in the target DB before deploy; maintain a short runbook (`docs/postgres_phase3.md:31`).
  - **Acceptance Criteria:** migration apply step documented and executed; `/readyz` is green post-deploy.
  - **Owner Suggestion:** Delivery Engineer Agent / QA Release Gate Agent

### 5. Roadmap (Phased)
- **Quick wins (0-2 days)**
  - Add a deploy preflight checklist item to ensure Postgres migrations are applied before flipping modes.
- **Near-term (1-2 weeks)**
  - Rework PR CI so forks/untrusted contributors do not receive secrets (if applicable).
  - Add a dev-only Postgres smoke test job (ephemeral Postgres container) for the run-store SQL layer.
- **Later (backlog)**
  - Private networking for Postgres (private endpoint + VNet integration) once baseline stability is confirmed.

### 6. Release/Delivery Gates
- Least privilege workflow permissions: **Pass** (`rg -n "^\\s*permissions\\s*:" .github/workflows`).
- Trigger safety (`pull_request_target`): **Pass** (not used; `rg -n "pull_request_target" .github/workflows` returned none).
- Action pinning: **Pass** (pinned actions observed in workflows).
- Secrets handling: **Pass w/ conditions** (deploy-only DSNs; address PR secret posture if accepting untrusted PRs).
- Migration discipline: **Pass w/ conditions** (migrations must be applied before enabling Postgres mode).

### 7. Evidence Log
- Files reviewed:
  - `.github/workflows/deploy.yml:267`
  - `.github/workflows/run_tests.yml:49`
  - `deploy/app_backtest_api.yaml:73`
  - `deploy/sql/postgres/migrations/0004_backtest_runs.sql:3`
  - `docs/postgres_phase3.md:31`
- Commands run (safe):
  - `ls -la .github/workflows`
  - `rg -n "^\\s*permissions\\s*:" .github/workflows`
  - `rg -n "pull_request_target" .github/workflows`
  - `rg -n "envsubst < deploy/app_backtest_api\\.yaml" .github/workflows/deploy.yml`
  - `rg -n "BACKTEST_POSTGRES_DSN" .github/workflows/deploy.yml`

