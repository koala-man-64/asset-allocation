### 1. Executive Summary
Phase 3A adds a new monitoring module and env toggles but does not introduce new CI workflows or secret-handling patterns. CI already runs `pytest` and builds/tests ui2.0, so the Phase 3A change remains well-covered. Primary workflow risk remains consistent dependency governance as new Azure Monitor/logging deps are introduced in later Phase 3 work.

**Risk rating:** Low (for Phase 3A)

### 2. Scope & Assumptions
- **In scope:** repo workflows, dependency pinning posture, and Phase 3A monitoring changes.
- **Out of scope:** Azure RBAC and environment-level configuration correctness.

### 3. Inventory Snapshot
- Audit snapshot: `docs/signoffs/audit_snapshot_phase3a_resource_health.json`
- Workflows: `.github/workflows/run_tests.yml`, `.github/workflows/lint_workflows.yml`, `.github/workflows/deploy.yml`, `.github/workflows/trigger_all_jobs.yml`

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None identified.

#### 4.2 Major
- None specific to Phase 3A (no new workflows/deps requiring special handling).

#### 4.3 Minor
- **[Config surface area continues to grow]**
  - **Evidence:** new `SYSTEM_HEALTH_RESOURCE_HEALTH_*` env vars (documented in `.env.template`).
  - **Recommendation:** keep env var grouping consistent and validate required combinations at runtime.
  - **Acceptance Criteria:** misconfig produces a clear warning alert; docs list required keys.
  - **Owner Suggestion:** Delivery Engineer Agent

### 5. Roadmap (Phased)
- **Quick wins (0-2 days):** ensure Phase 3A env vars are set consistently per environment; add a short ops runbook snippet.
- **Near-term (1-2 weeks):** before Phase 3B (Monitor/Logs), confirm dependency lockfiles are updated and CI remains hermetic.
- **Later (backlog):** consider `.gitattributes`/`.editorconfig` to reduce cross-platform diff noise.

### 6. Release/Delivery Gates
- **Tests in CI:** Pass (workflow exists, Phase 3A tests are hermetic)
- **UI build/test in CI:** Pass (existing)
- **Workflow lint:** Pass (existing)
- **Least-privilege permissions:** Pass (explicit permissions observed)

### 7. Evidence Log
- Generated: `docs/signoffs/audit_snapshot_phase3a_resource_health.json`
- Local tests: `python3 -m pytest -q` → **125 passed, 3 warnings**

