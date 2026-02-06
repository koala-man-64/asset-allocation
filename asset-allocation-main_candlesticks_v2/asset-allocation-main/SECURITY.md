# Security Policy

## Reporting a vulnerability
If this repository is hosted on GitHub, use the repository **Security** tab to report a vulnerability privately.

If you do not have access to GitHub Security Advisories for this repo, report the issue to the repository owner/maintainer via your internal security process (do not file a public issue with exploit details).

## Secrets handling
- Do not commit secrets. This repo ignores `.env` via `.gitignore`.
- Use environment variables and/or your secret manager (e.g., GitHub Actions secrets, Azure Key Vault/managed identity).
- If a secret is accidentally committed, rotate it immediately and remove it from history as required by your org’s policy.

## Dependency hygiene
- Prefer pinned dependencies for reproducible builds.
- Review dependency updates for supply-chain risk.

