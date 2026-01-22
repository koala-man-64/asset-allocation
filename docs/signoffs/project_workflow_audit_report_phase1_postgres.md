# Project & Workflow Audit Report

### 1. Executive Summary
- Phase 1 adds Postgres provisioning/migration scripts and extends the deploy workflow to inject Postgres DSN secrets into Container Apps YAML via `envsubst`.
- Action pinning and explicit workflow permissions remain intact.
- Main workflow risk remains secrets exposure: deploy-time templating writes secrets to temporary YAML, and the repo currently runs tests on `pull_request` using cloud secrets (existing posture).
- **Risk rating: Medium** (acceptable with guardrails and trusted PR contributors).

### 2. Scope & Assumptions
- **In scope:** `.github/workflows/deploy.yml`, `deploy/*.yaml`, new `deploy/*.ps1` scripts, and Phase 1 docs.
- **Assumptions:** Postgres DSNs are stored only in GitHub Secrets and not echoed in CI logs; PR contributors are trusted or secrets are not exposed to forks.

### 3. Inventory Snapshot
- **Workflows present:**
  - `.github/workflows/deploy.yml` (Azure deploy; push `main` + manual)
  - `.github/workflows/run_tests.yml` (push + PR; uses cloud secrets)
  - `.github/workflows/trigger_all_jobs.yml` (manual)
  - `.github/workflows/lint_workflows.yml` (actionlint)
- **Instruction/policy files:**
  - `CONTRIBUTING.md`, `SECURITY.md`, `.github/CODEOWNERS`

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None introduced by Phase 1 change set.

#### 4.2 Major
- **[Deploy workflow injects new DSN secrets; ensure deploy-only usage]**
  - **Evidence:** `.github/workflows/deploy.yml` references:
    - `secrets.BACKTEST_POSTGRES_DSN`
    - `secrets.RANKING_POSTGRES_DSN`
  - **Why it matters:** DSNs are high-value secrets; they must not be used in PR workflows or printed.
  - **Recommendation:** Keep DSNs confined to deploy workflow only (current state), avoid printing rendered YAML, and delete temp YAML files (current pattern).
  - **Acceptance Criteria:** No DSN secret references appear in `.github/workflows/run_tests.yml` or other PR workflows; deploy logs never echo DSNs; temp YAML removed.
  - **Owner Suggestion:** DevOps Agent / Project Workflow Auditor Agent

- **[PR CI uses cloud secrets (existing posture)]**
  - **Evidence:** `.github/workflows/run_tests.yml` triggers on `pull_request` and uses `${{ secrets.AZURE_STORAGE_CONNECTION_STRING }}`.
  - **Why it matters:** Untrusted PRs + secrets can enable exfiltration.
  - **Recommendation:** If PRs can be untrusted (forks/external contributors), split workflows so PR jobs run without secrets or restrict PR triggers.
  - **Acceptance Criteria:** Either PR contributors are limited to trusted users or PR workflows do not receive high-value secrets.
  - **Owner Suggestion:** DevOps Agent + QA Release Gate Agent

#### 4.3 Minor
- **[No Dependabot config]**
  - **Evidence:** `.github/dependabot.yml` absent.
  - **Recommendation:** Add Dependabot for Python requirements + UI lockfile.
  - **Acceptance Criteria:** Scheduled dependency update PRs.
  - **Owner Suggestion:** Project Workflow Auditor Agent

### 5. Roadmap (Phased)
- **Quick wins (0-2 days)**
  - Add a note in `docs/postgres_phase1.md` (already present) to keep DSNs deploy-only.
  - Add a CI guard to ensure rendered `deploy/*.tmp.yaml` files are not committed.
- **Near-term (1-2 weeks)**
  - Split PR CI to run without secrets or restrict PR secret access.
  - Add Dependabot.
- **Later (backlog)**
  - Private networking for Postgres (private endpoint + VNet integration).

### 6. Release/Delivery Gates
- Least privilege workflow permissions: **Pass**
- Trigger safety (`pull_request_target`): **Pass** (not used)
- Action pinning: **Pass** (commit SHA pinning observed)
- Secrets handling: **Pass w/ conditions** (deploy-only DSNs; avoid printing rendered YAML; address PR secret posture if untrusted)

### 7. Evidence Log
- Files reviewed:
  - `.github/workflows/deploy.yml`
  - `.github/workflows/run_tests.yml`
  - `deploy/app_backtest_api.yaml`
  - `deploy/job_platinum_ranking.yaml`
  - `deploy/provision_azure_postgres.ps1`
  - `deploy/apply_postgres_migrations.ps1`
  - `docs/postgres_phase1.md`
- Commands run:
  - Safe repo inventory snapshot and workflow grep (no secrets printed).

