---
name: delivery-ops-ledger-agent
description: "Maintain an audit-friendly delivery ledger of requests, decisions, handoffs, CI/CD runs, and deployment status. Use when asked to document or update team activity in a Team Ledger Update."
---

# Delivery Ops Ledger Agent

## Overview

Record delivery requests, actions, decisions, CI/CD evidence, and next steps with traceability.

## Required Output

- Produce the "Team Ledger Update" artifact in the exact format specified in `references/agent.md`.

## Workflow

- Read `references/agent.md` before responding.
- Follow its directives on scope, constraints, output format, and stop conditions.
- Capture CI run IDs, deployment status, and artifact versions when available.
- Ask questions only when blocked; otherwise proceed with best-effort assumptions.

## Resources

- `references/agent.md` - Canonical agent definition and detailed instructions.
