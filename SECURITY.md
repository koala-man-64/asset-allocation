# Security Policy

## Reporting a Vulnerability

If this repository is hosted on GitHub, use the repository Security tab to report a vulnerability privately.

If GitHub Security Advisories are not available for this repo, report the issue to the repository owner or maintainer through your internal security process. Do not open a public issue with exploit details.

## Authentication and Authorization

- Deployed runtimes must configure `API_KEY`, `API_OIDC_ISSUER` plus `API_OIDC_AUDIENCE`, or both.
- API key auth uses `API_KEY` and the fixed `X-API-Key` header.
- OIDC auth validates issuer and audience and can require scopes and roles. The service discovers JWKS from the issuer unless `API_OIDC_JWKS_URL` is set explicitly.
- The UI receives its runtime auth and API base URL settings from `/config.js`.
- Local development can fall back to anonymous access only when no auth providers are configured and the runtime is local. Deployed environments do not allow anonymous auth.

## Secrets and Identities

- Do not commit secrets. `.gitignore` excludes `.env` and `.env.*`, while `.env.template` is the checked-in contract.
- `deploy/app_api.yaml` uses Container App secret references for the API key, Azure Storage connection string, provider API keys, and `POSTGRES_DSN`.
- Azure deployment uses a user-assigned managed identity for registry pulls and platform access.

## Response Hardening and Input Validation

- API middleware sets `X-Content-Type-Options: nosniff` and `X-Frame-Options: DENY`.
- `API_CSP` controls the Content Security Policy header when set.
- CORS origins are parsed from `API_CORS_ALLOW_ORIGINS`; wildcard `*` is removed when credentials are enabled.
- `api/service/security.py` validates run IDs, artifact names, local paths, and ADLS container/path inputs for filesystem- and artifact-related operations.

## Dependency Hygiene

- Runtime dependencies are pinned in `pyproject.toml`, `requirements.txt`, and `requirements.lock.txt`.
- CI and supply-chain workflows consume `requirements.lock.txt` and `requirements-dev.lock.txt`.
- Run `python3 scripts/dependency_governance.py check --report artifacts/dependency_governance_report.json` before merging dependency changes.

## Evidence

- `.gitignore`
- `.env.template`
- `api/service/app.py`
- `api/service/settings.py`
- `api/service/auth.py`
- `api/service/security.py`
- `deploy/app_api.yaml`
- `.github/workflows/run_tests.yml`
- `.github/workflows/supply_chain_security.yml`
- `.github/workflows/dependency_governance.yml`
- `scripts/dependency_governance.py`
