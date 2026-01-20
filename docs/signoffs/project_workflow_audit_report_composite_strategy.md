# Project & Workflow Audit Report

### 1. Executive Summary
- Overall posture is **Medium risk**: CI is pinned and reproducible, tests are comprehensive, and the composite strategy changes are covered by automated tests.
- Deployment artifacts and workflows are functional but have governance gaps: deployment manifests embed subscription/resource IDs, and deploy paths rely on ACR admin credentials and storage connection strings (secrets management and portability concerns).
- Near-term priorities: (1) parameterize deployment manifest IDs, (2) prefer managed identity for ACR pulls and storage access, (3) keep CI invocation consistent (`python -m pytest`) to avoid environment-dependent import behavior.

### 2. Scope & Assumptions
- In-scope:
  - Composite strategy change set and new provisioning script
  - CI workflows: `.github/workflows/run_tests.yml`, `.github/workflows/deploy.yml`
  - Deployment manifests under `deploy/`
- Assumptions:
  - Azure identity provisioning for GitHub OIDC (service principal + federated credentials) is handled outside this repo.

### 3. Inventory Snapshot
- Key languages/runtimes: Python 3.10, FastAPI service, Node (UI build in CI).
- CI/CD workflows:
  - `Run Tests` builds UI and runs Python tests.
  - `Build and Deploy` builds/pushes images and deploys Container Apps + Jobs via Azure CLI.
- Instruction/policy files:
  - `CONTRIBUTING.md`, `SECURITY.md`, `README.md`
  - Signoff artifacts under `docs/signoffs/`

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None identified in the composite feature change set.

#### 4.2 Major
- **[Provisioning script secret handling needs safe defaults]**
  - **Evidence:** `deploy/provision_azure.ps1` retrieves storage connection strings and (optionally) ACR passwords.
  - **Why it matters:** Printing secrets to stdout is an easy accidental leak vector (shell history, CI logs, screen recording).
  - **Recommendation:** Default to redacting secrets and require an explicit flag to emit them.
  - **Acceptance Criteria:** Script only prints secrets when a user passes an explicit opt-in switch; otherwise outputs `<redacted>`.
  - **Owner Suggestion:** Delivery Engineer Agent

- **[Deployment manifests contain hard-coded subscription/resource IDs]**
  - **Evidence:** `deploy/app_backtest_api.yaml`, `deploy/job_*.yaml` contain full `managedEnvironmentId`/`environmentId` resource IDs with a specific subscription GUID.
  - **Why it matters:** Reduces portability; requires manual edits for new subscriptions/resource groups; increases drift risk.
  - **Recommendation:** Parameterize subscription/resource group/environment name via envsubst variables (already used in deploy workflow).
  - **Acceptance Criteria:** No literal subscription GUIDs in `deploy/*.yaml`; deploy workflow exports the required env vars for `envsubst`.
  - **Owner Suggestion:** DevOps Agent / Delivery Engineer Agent

- **[CI test invocation should use interpreter module form]**
  - **Evidence:** Updated `.github/workflows/run_tests.yml` to use `python -m pytest`.
  - **Why it matters:** Avoids environment-dependent import path differences between `pytest` entrypoints and module invocation.
  - **Recommendation:** Keep this invocation consistent across docs and workflows.
  - **Acceptance Criteria:** CI uses `python -m pytest`; README matches.
  - **Owner Suggestion:** QA Release Gate Agent / Delivery Engineer Agent

#### 4.3 Minor
- **[ACR admin credential usage in deployment]**
  - **Evidence:** `deploy.yml` reads ACR password via `az acr credential show` and injects it as a Container Apps secret.
  - **Why it matters:** Admin credentials are higher blast radius than identity-based `AcrPull`.
  - **Recommendation:** Prefer managed identity with `AcrPull` role and disable ACR admin.
  - **Acceptance Criteria:** Container Apps pull images via identity; ACR admin disabled.
  - **Owner Suggestion:** DevOps Agent / Security Agent

### 5. Roadmap (Phased)
- **Quick wins (0-2 days)**
  - Keep `python -m pytest` in CI and docs (done in this change set).
  - Keep provisioning output secrets redacted unless explicitly requested (done in this change set).
- **Near-term (1-2 weeks)**
  - Parameterize `deploy/*.yaml` environment IDs and storage account names.
  - Add RBAC role assignment automation for the Backtest API managed identity (storage data-plane).
- **Later (backlog)**
  - Replace ACR admin secrets with managed identity pulls.
  - Add key vault integration for storage connection string usage by jobs.

### 6. Release/Delivery Gates
- Tests in CI: **Pass** (local evidence: `python3 -m pytest -q`).
- Workflow pinning: **Pass** (pinned action digests present in workflows).
- Secrets handling: **Pass/Unknown** (repo contains `.env`; secrets not audited here; provisioning script now redacts by default).
- Deployment portability: **Fail** (hard-coded resource IDs remain in `deploy/*.yaml`).

### 7. Evidence Log
- Commands run:
  - `python3 -m pytest -q`
  - `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out audit_snapshot.json`
- Generated artifacts:
  - `audit_snapshot.json`

