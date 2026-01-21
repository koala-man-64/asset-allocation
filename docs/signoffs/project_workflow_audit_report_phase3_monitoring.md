### 1. Executive Summary
Phase 3 introduces new external integrations (Azure Monitor + Log Analytics) and likely new environment configuration keys. Repo/CI posture is generally strong (explicit workflow permissions, pinned actions, UI build/test in CI). Primary workflow/governance risks for Phase 3 are: (1) adding dependencies without lockfile alignment, and (2) introducing environment variables/secrets that could leak in logs or be misconfigured across environments.

**Risk rating:** Medium (due to planned external integrations; not due to current repo posture).

### 2. Scope & Assumptions
- **In scope:** CI workflows, dependency pinning posture, env-var/secret handling patterns, and Phase 3 planning impacts.
- **Out of scope:** Azure subscription security, RBAC correctness, and org-level branch protections.
- **Assumptions:** Phase 3 will continue to prefer Managed Identity and avoid embedding secrets.

### 3. Inventory Snapshot
- **CI workflows present:**
  - `.github/workflows/run_tests.yml` (UI build/test + pytest)
  - `.github/workflows/lint_workflows.yml` (workflow lint)
  - `.github/workflows/deploy.yml` (deploy pipeline)
- **Dependency model:** pinned `requirements*.lock.txt` and `pnpm` lockfile for ui2.0.

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None identified for the plan itself.

#### 4.2 Major
- **[Planned Azure Monitor/Logs integration must remain hermetic in CI]**
  - **Evidence:** CI runs pytest without Azure runtime identities; network access is not guaranteed.
  - **Why it matters:** tests must not depend on live Azure, or CI will be flaky/blocked.
  - **Recommendation:** require fakes/mocks for Monitor/Log Analytics clients; add guardrails to prevent accidental network calls in tests.
  - **Acceptance Criteria:** Phase 3 test suite runs without Azure credentials and without outbound dependencies.
  - **Owner Suggestion:** QA Release Gate Agent + Delivery Engineer Agent

#### 4.3 Minor
- **[Config governance for many env vars]**
  - **Evidence:** Phase 2 already relies on multiple `SYSTEM_HEALTH_*` vars; Phase 3 will add more.
  - **Recommendation:** document per-env required keys; validate combos at runtime; add a “monitoring enabled checklist” in deployment docs.
  - **Acceptance Criteria:** misconfig produces clear warnings; `.env.template` updated with Phase 3 keys.
  - **Owner Suggestion:** Delivery Engineer Agent

### 5. Roadmap (Phased)
- **Quick wins (0-2 days):** update `.env.template` + deployment docs for Phase 3 keys; ensure tests are hermetic.
- **Near-term (1-2 weeks):** add minimal metrics and probe failure observability; consider `.gitattributes`/`.editorconfig` if line-ending drift becomes noisy.
- **Later (backlog):** add dependency automation (Dependabot) if not already adopted.

### 6. Release/Delivery Gates
- **Tests in CI:** Pass (existing)
- **UI build/test in CI:** Pass (existing)
- **Workflow least privilege:** Pass (explicit `permissions:` observed)
- **Supply-chain pinning:** Pass (pinned action SHAs and container digest observed)

### 7. Evidence Log
- Workflows referenced: `.github/workflows/run_tests.yml`, `.github/workflows/lint_workflows.yml`.

