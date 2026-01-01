---
name: super-user-operator
description: "Guide interactions with the human Super User/Operator who performs external actions, approvals, and evidence collection. Use when Codex needs privileged access actions, environment confirmations, command output, screenshots, or approval for risky operations."
---

# Super User Operator

## Overview
Use this skill to request real-world actions and confirmations from the human operator who sits outside the agentic team. Treat the operator as the source of truth for access, environment state, and go/no-go decisions.

## When to Invoke
- Need privileged access (cloud console, CI/CD, private repos, local machine).
- Need authoritative environment truth (deployed versions, config values, endpoints, runtime status).
- Need command output, logs, or screenshots that the agent cannot access.
- Need explicit approval for risky operations (prod changes, migrations, destructive actions).
- Need business rules or acceptance criteria that are not documented.

## Request Protocol
- Provide a clear, bounded request with a work item ID and expected evidence.
- Prefer read-only checks first.
- Include exact commands to run and where to run them.
- Specify any required redactions and the acceptable level of detail.
- Ask for a single, structured response in the Operator Response format below.

## Operator Response (Required)
Ask the operator to respond using this exact structure:

### Operator Response
- **Work Item ID:**
- **Action Taken:**
- **Environment:** local / dev / prod (include region or cluster name if relevant)
- **Commands Run:**
- **Result:** success / failure
- **Raw Output:** (paste logs or terminal output, redact secrets)
- **Artifacts:** (screenshots, links, files)
- **Redactions Performed:** yes/no (what was redacted)
- **Notes / Observations:**

## Redaction Rules
- Never request or include secrets (tokens, private keys, passwords, connection strings).
- If output contains secrets, require replacing them with `<REDACTED>` and note where they appeared.
- Allow non-sensitive identifiers unless the operator reports they are restricted.

## Authority and Decision Rights
- Treat operator confirmations as ground truth for environment state and approvals.
- Do not override operator decisions on allowed environments or change windows.

## If the Operator Cannot Perform an Action
- Ask for the reason (permissions, access, policy, time).
- Request the closest safe alternative (read-only command, screenshot, or a contact who can run it).
- Update the work item as Blocked if no alternative is possible.

## Stop Conditions
- Do not repeat the same request without new information.
- If the operator reports completion and a loop persists, ask for the current blocker and the next required evidence.

## Example Operator Request
Use a concise request like:

"Work Item ID: WI-102. Please run `az functionapp config appsettings list -g <RG> -n <APP>` in the dev subscription and return the raw output. Redact any secrets."
