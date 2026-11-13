# Architecture - {{PROJECT_ID}}

**Authored by**: data-engineer
**Date**: {{DATE_UTC}}
**Based on**: requirements.md (APPROVED by product-owner at {{APPROVAL_TS}})

## Decision summary
<one paragraph: engines chosen, major design choices, scale posture>

## Component diagram
<ASCII or Mermaid; name every component>

## Data architecture
### Sources
### Bronze layer
### Silver / vault layer
### Gold / mart layer
### Serving tier

## Pipeline specification
| Stage | Input | Output | Engine | Key transformation | Scale posture |
|---|---|---|---|---|---|

## Data contracts
<schema per table: column, type, nullability, source, invariants>

## APIs and contracts
<if applicable>

## Dependencies on existing code
<files to extend; files not to touch; deps to avoid>

## Development task breakdown
| Task ID | Assigned agent | Description | Prerequisites | Acceptance |
|---|---|---|---|---|
| T-001 | developer-backend | ... | - | ... |

## Parallelizable workstreams
Workstream A: {{task list}}
Workstream B: {{task list}}
Workstream C: {{task list}}

## Dashboard (preliminary read)
<needed? which KPIs would be served if yes? spec comes later in dashboard_spec.md>

## Risks
- ...

## Non-negotiables inherited from CLAUDE.md / plan.md
- ...

## Status
`DRAFT | APPROVED | NEEDS_CHANGES`
