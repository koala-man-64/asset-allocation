# Project & Workflow Audit Report

### 1. Executive Summary
- Phase 2 introduces Postgres dual-write for ranking signals, adds a new Postgres driver dependency, and adds a new Postgres migration. CI/CD workflows are unchanged, but deploy-time secrets (DSNs) now become operationally required for the ranking job when enabled.
- Workflow security posture remains largely the same as Phase 1: actions are pinned, `permissions:` blocks exist, and deploy secrets are confined to `.github/workflows/deploy.yml`.
- The main governance risk remains **secrets exposure in PR workflows** (existing posture) and **deploy-time templating** writing secrets into temporary YAML via `envsubst`.
- **Risk rating: Medium** (acceptable with guardrails and trusted PR contributors).

### 2. Scope & Assumptions
- **In scope:** Phase 2 change set (Postgres deps, migrations, ranking job dual-write code) and GitHub workflows relevant to deploy/tests.
- **Excluded:** Azure resource configuration validation (performed operationally), Postgres private networking, and Phase 3/4 features.
- **Assumptions:** DSNs are stored only in GitHub Secrets and not echoed in logs; PR contributors are trusted unless workflows are restricted.

### 3. Inventory Snapshot
- **Languages/runtimes:** Python 3.10+ (`pyproject.toml:1`), GitHub Actions.
- **Workflows present:** `.github/workflows/deploy.yml`, `.github/workflows/run_tests.yml`, `.github/workflows/trigger_all_jobs.yml`, `.github/workflows/lint_workflows.yml`.
- **Dependency pinning:** `requirements.txt`, `requirements.lock.txt`, `requirements-dev.lock.txt` (Phase 2 adds psycopg pins).
- **Governance docs:** `CONTRIBUTING.md`, `SECURITY.md`, `.github/CODEOWNERS`.
- **AGENTS.md:** none discovered via filename scan (repo uses `.codex/skills/*`).

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None introduced by Phase 2 change set.

#### 4.2 Major
- **[Deploy-time templating writes DSN secrets into temporary YAML]**
  - **Evidence:** `.github/workflows/deploy.yml:1` uses `envsubst` to render `deploy/job_platinum_ranking.yaml:1` containing DSN secret values.
  - **Why it matters:** Any accidental printing or retention of rendered YAML can leak DSNs.
  - **Recommendation:** Continue deleting temp YAML (current pattern), avoid printing rendered YAML, and keep DSNs confined to deploy workflow only.
  - **Acceptance Criteria:** No workflow logs print DSNs; temp YAML removed; DSNs referenced only in deploy workflow.
  - **Owner Suggestion:** DevOps Agent / Project Workflow Auditor Agent

- **[PR workflow uses cloud secrets (existing posture)]**
  - **Evidence:** `.github/workflows/run_tests.yml:1` triggers on `pull_request` and uses secrets.
  - **Why it matters:** Untrusted PR code execution can exfiltrate secrets.
  - **Recommendation:** If PRs can be untrusted (forks/external), split workflows so PR jobs run without secrets or restrict PR triggers to trusted users.
  - **Acceptance Criteria:** PR workflows do not receive high-value secrets from forks/untrusted contributors.
  - **Owner Suggestion:** DevOps Agent + QA Release Gate Agent

#### 4.3 Minor
- **[Supply chain: new dependency introduced]**
  - **Evidence:** `requirements.txt:1`, `requirements.lock.txt:1`, `requirements-dev.lock.txt:1` add `psycopg==3.2.3` and `psycopg-binary==3.2.3`.
  - **Why it matters:** Adds a new external dependency surface.
  - **Recommendation:** Keep versions pinned (done); consider adding Dependabot to keep pins current.
  - **Acceptance Criteria:** Dependabot config added (optional); periodic dependency updates.
  - **Owner Suggestion:** Project Workflow Auditor Agent

- **[“client_secret” string appears in code/docs]**
  - **Evidence:** `scripts/common/delta_core.py:1` references `AZURE_CLIENT_SECRET` and a workflow report doc references the term.
  - **Why it matters:** Not a secret by itself, but can trigger naive secret scanners; ensure no actual secret values are committed.
  - **Recommendation:** Keep value sources env-only; continue filename-only secret scans in CI if desired.
  - **Acceptance Criteria:** No committed secrets detected by safe scans.
  - **Owner Suggestion:** Project Workflow Auditor Agent

### 5. Roadmap (Phased)
- **Quick wins (0-2 days)**
  - Add `.github/dependabot.yml` for Python deps.
  - Add a CI check that fails if rendered `deploy/*.tmp.yaml` files are committed.
- **Near-term (1-2 weeks)**
  - Split PR CI workflows to run without secrets or restrict PR triggers.
  - Add a Postgres smoke test job (dev-only) with an ephemeral Postgres container (optional).
- **Later (backlog)**
  - Private networking for Postgres (private endpoint + VNet integration).

### 6. Release/Delivery Gates
- Least privilege workflow permissions: **Pass** (permissions blocks present).
- Trigger safety (`pull_request_target`): **Pass** (not used).
- Action pinning: **Pass** (pinned actions observed).
- Secrets handling: **Pass w/ conditions** (deploy-only DSNs; avoid printing rendered YAML; address PR secret posture if untrusted).
- Dependency pinning: **Pass** (lockfiles updated).

### 7. Evidence Log
- Files reviewed:
  - `.github/workflows/deploy.yml:1`
  - `.github/workflows/run_tests.yml:1`
  - `deploy/job_platinum_ranking.yaml:1`
  - `requirements.txt:1`
  - `requirements.lock.txt:1`
  - `requirements-dev.lock.txt:1`
  - `scripts/ranking/signals.py:219`
  - `scripts/ranking/postgres_signals.py:1`
- Commands run (safe):
  - `rg -n "^\\s*permissions\\s*:" .github/workflows`
  - `rg -n "pull_request_target" .github/workflows`
  - `ls -la .github/workflows`
  - `rg -l "AKIA[0-9A-Z]{16}" .`
  - `rg -l "ghp_[A-Za-z0-9]{36}" .`
  - `rg -l -- "-----BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY-----" .`
  - `rg -l "client_secret" .`

