### 1. Executive Summary
Phase 3B adds new monitoring capabilities without introducing new CI workflows or new Python dependencies. Existing CI already runs `pytest` and builds/tests ui2.0. The main workflow risks for Phase 3B are operational/configuration governance: ensuring metric names, thresholds, and KQL query templates are versioned/documented and that sensitive data is not introduced into configs or logs.

**Risk rating:** Low-to-Medium (primarily operational/config-driven)

### 2. Scope & Assumptions
- **In scope:** workflows, dependency pinning posture, and Phase 3B monitoring additions.
- **Out of scope:** Azure RBAC correctness, workspace-level permissions, and org branch protections.

### 3. Inventory Snapshot
- Audit snapshot: `docs/signoffs/audit_snapshot_phase3b_monitor_signals.json`
- Workflows: `.github/workflows/run_tests.yml` (pytest + ui2.0 build/test), `.github/workflows/lint_workflows.yml`

### 4. Findings (Triaged)
#### 4.1 Critical (Must Fix)
- None identified.

#### 4.2 Major
- **[KQL query governance]**
  - **Evidence:** queries are configured via `SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON`.
  - **Why it matters:** unsafe or unscoped queries can be expensive; configs may drift between environments.
  - **Recommendation:** keep query templates allowlisted and reviewed; store canonical examples in repo docs; avoid embedding secrets in queries.
  - **Acceptance Criteria:** env template includes the keys and an example shape; deploy config references reviewed templates.
  - **Owner Suggestion:** Project Workflow Auditor Agent / Delivery Engineer Agent

#### 4.3 Minor
- **[Config sprawl]**
  - **Evidence:** new `SYSTEM_HEALTH_MONITOR_METRICS_*` and `SYSTEM_HEALTH_LOG_ANALYTICS_*` knobs.
  - **Recommendation:** keep naming consistent and validate required combinations at runtime (misconfig ⇒ warning alert).
  - **Acceptance Criteria:** enabling probes without required keys produces a clear warning alert and does not break `/system/health`.

### 5. Roadmap (Phased)
- **Quick wins (0-2 days):** add a short ops/runbook note listing required RBAC roles for Metrics and Log Analytics.
- **Near-term (1-2 weeks):** add minimal probe telemetry metrics (latency/errors) once a metrics backend exists.
- **Later (backlog):** consider `.gitattributes`/`.editorconfig` if cross-platform formatting drift becomes noisy.

### 6. Release/Delivery Gates
- **Tests in CI:** Pass (existing workflow; Phase 3B tests are hermetic)
- **UI build/test in CI:** Pass (existing)
- **Workflow least privilege:** Pass (explicit permissions observed)

### 7. Evidence Log
- Generated: `docs/signoffs/audit_snapshot_phase3b_monitor_signals.json`
- Local tests: `python3 -m pytest -q` → **132 passed, 3 warnings**

