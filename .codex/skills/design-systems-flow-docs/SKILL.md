---
name: design-systems-flow-docs
description: UI/UX design and product documentation focused on design systems, user flows, wireframes, and component specs. Use when Codex must produce an implementable UI Design & Documentation Pack for web apps, dashboards, or reports; define information architecture and navigation; document flows and states; or specify reusable components and interaction rules.
---

# Design Systems & Flow Docs

## Overview

Use this skill to design clear, implementable UI experiences and deliver a UI Design & Documentation Pack that engineers can build from with minimal ambiguity.

## Inputs to Gather

- Collect product requirements, acceptance criteria, user stories, and target platform (web, dashboard, report).
- Request existing screenshots, brand guidelines, or design tokens when available.
- Capture constraints for accessibility, performance, compliance, and browser support.
- Identify stakeholder preferences (e.g., simple, dense, executive-friendly).
- If inputs are missing, propose sensible defaults and label assumptions.

## Workflow

1. Scope and audience
   - Identify user roles, primary tasks, and success criteria.
   - Separate executive overview needs from operator workflows.
2. Information architecture
   - Define page list, navigation model, and grouping.
   - Note role-based visibility or permissions.
3. Flows and states
   - Document happy path plus alternate, error, loading, empty, and permission-restricted states.
   - Provide diagram-ready steps and optional Mermaid flowchart text.
4. Wireframes
   - Provide text wireframes per page with layout sections and component placements.
   - Note responsive behavior and data requirements.
5. Component system
   - Inventory reusable components with props, states, validation timing, and interaction rules.
   - Include accessibility notes and performance considerations.
6. Reporting and dashboards (if applicable)
   - Specify KPI display rules, filter semantics, chart/table interactions, and annotations.
7. Copy and messaging
   - Provide empty-state, error, confirmation, and destructive-action copy guidance.
8. Accessibility and QA checklist
   - Ensure keyboard navigation, focus order, contrast, and screen reader labeling.
9. Handoffs
   - Summarize build priorities for implementation, key flows for QA, and decisions for bookkeeping.

## Output: UI Design & Documentation Pack

### 1. Executive Overview
- State what is being designed.
- List primary users and primary tasks.
- Note key design principles applied.

### 2. Information Architecture
- Provide page list with purpose.
- Define navigation model (tabs, sidebar, breadcrumbs).
- Note permissions or role-based visibility.

### 3. Key User Flows (Diagram-Ready)
- Provide bullet steps for each key task.
- Include happy path plus error/empty/loading states.
- Add Mermaid flowchart text if helpful.

### 4. Page-Level Wireframes (Text)
- Describe layout sections (header/filters/content/footer).
- Place components and controls per section.
- Note responsiveness and data requirements (fields needed).

### 5. Component Inventory & Interaction Specs
- List component purpose and states (default/loading/empty/error/disabled).
- Define validations and interaction rules (debounce, pagination, sorting).
- Include accessibility and performance notes.

### 6. Reporting/Dashboard Spec (If Applicable)
- Define KPI display rules and units.
- Specify filter defaults, reset behavior, and time range semantics.
- Describe chart/table interactions (drill-down, tooltips, exports).
- Define annotation/alert patterns and thresholds.

### 7. Copy & Messaging
- Provide empty state copy and guidance.
- Provide error message templates.
- Provide confirmations and destructive action copy.

### 8. Accessibility & QA Checklist
- Keyboard navigation and focus order.
- Contrast and text sizing.
- Screen reader labels and form error patterns.

### 9. Handoffs
- `Handoff: Implementation Agent` - component list + priorities.
- `Handoff: QA Agent` - key flows to validate.
- `Handoff: Bookkeeper Agent` - decisions, scope, and artifacts to record.

## Constraints and Quality Bar

- Do not invent business rules or data fields; mark unknowns as **Unknown** and list what is needed.
- Document conflicting requirements and propose a default resolution.
- Prefer minimal, consistent UI patterns; avoid novelty unless it solves a clear problem.
- Design for accessibility, error prevention, and recovery by default.
- Respect data nuance (definitions, filters, time windows, units, quality flags).
