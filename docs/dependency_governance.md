# Dependency Governance Runbook

## Source Of Truth

Runtime dependencies are defined in `pyproject.toml` under `[project].dependencies`.

`requirements.txt` and `requirements.lock.txt` are generated/synchronized runtime manifests derived from that source.

## Required Checks

- CI workflow: `.github/workflows/dependency_governance.yml`
- Script: `scripts/dependency_governance.py check`

The check enforces:
- `pyproject.toml` runtime dependencies are pinned with `==`
- `requirements.txt` matches runtime dependencies from `pyproject.toml`
- `requirements.lock.txt` matches `requirements.txt`
- `requirements-dev.lock.txt` has pinned dependencies

## Update Workflow

1. Edit runtime dependencies in `pyproject.toml`.
2. Synchronize runtime manifests:

```bash
python3 scripts/dependency_governance.py sync
```

3. Validate before committing:

```bash
python3 scripts/dependency_governance.py check --report artifacts/dependency_governance_report.json
```

4. Run supply-chain checks locally where available:

```bash
python3 -m pip_audit --strict -r requirements.lock.txt
python3 -m pip_audit --strict -r requirements-dev.lock.txt
```

## Scheduled Security Scans

`.github/workflows/supply_chain_security.yml` runs on:
- pull requests
- pushes to `main`
- weekdays (scheduled)
- manual dispatch

It uploads JSON `pip-audit` artifacts (`pip-audit-runtime.json`, `pip-audit-dev.json`) for traceability.

## Branch Protection (Repository Settings)

Configure branch protection for `main` to require these checks:
- `Dependency Governance / dependency-drift-check`
- `Supply Chain Security / python-dependency-audit`
- `Supply Chain Security / ui-dependency-audit`

This setting is outside repository code and must be managed in GitHub repository settings.
