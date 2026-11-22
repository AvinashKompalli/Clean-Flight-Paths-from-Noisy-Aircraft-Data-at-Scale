# ETL4 Multi-Agent Engineering Organization

Seven roles, one orchestrator + six specialists, implemented as Claude Code agent definitions in `.claude/agents/` and invocable via `/workflow`.

## Roster

| Agent | File | Role | Authority |
|---|---|---|---|
| `orchestrator` | `.claude/agents/orchestrator.md` | Chief Orchestrator | Task dispatch, state, convention-enforced gates |
| `product-owner` | `.claude/agents/product-owner.md` | Business + Final Acceptance | Requirements, BRD, acceptance criteria, final ACCEPT/REJECT |
| `data-engineer` | `.claude/agents/data-engineer.md` | Chief Technical Architect | Architecture, data contracts, technical review, dashboard spec |
| `developer-backend` | `.claude/agents/developer-backend.md` | Senior Backend / Core Dev | Python backend, data processing, services |
| `developer-frontend` | `.claude/agents/developer-frontend.md` | Frontend / Dashboard Dev | Grafana + UI implementation |
| `unit-tester` | `.claude/agents/unit-tester.md` | Independent Unit Tester | pytest coverage |
| `functional-tester` | `.claude/agents/functional-tester.md` | Functional / UI/UX QA | e2e, integration, dashboard UX |

## Invocation

- `/workflow <description>` fires the Chief Orchestrator
- Direct specialist calls via the Agent tool with `subagent_type: <agent-name>` (orchestrator-only)

## Project state

All project state lives at `multi-agent-engineering/projects/<project_id>/`:

```
projects/
  PROJ-YYYYMMDD-<slug>/
    state.json                # current phase, task statuses, approval history
    requirements.md           # Product Owner
    architecture.md           # Data Engineer
    data_contracts.md
    technical_review.md       # Data Engineer core-solution analysis
    dashboard_spec.md         # if applicable
    dashboard_analysis.md     # if applicable
    acceptance_report.md      # Product Owner final
    tasks/
      T-001.json              # one file per task (status machine)
      ...
    handoffs/
      YYYYMMDDTHHMMSS-from-to.json
      ...
    artifacts/                # produced code snapshots / symlinks
    test_report.md            # Functional QA output
```

## Approval gates (convention-enforced)

The orchestrator does not advance `state.json.phase` past a gate until the required specialist returns a handoff with `STATUS: APPROVED`:

| Gate | Required approver |
|---|---|
| `REQUIREMENTS -> ARCHITECTURE` | `product-owner` |
| `ARCHITECTURE -> IMPLEMENTATION` | `data-engineer` |
| `IMPLEMENTATION -> UNIT_TEST` | at least one developer's handoff APPROVED (orchestrator may dispatch unit tests in parallel after partial APPROVED) |
| `UNIT_TEST -> FUNCTIONAL_TEST` | `unit-tester` (pass rate 100%) |
| `FUNCTIONAL_TEST -> CORE_SOLUTION_REVIEW` | `functional-tester` |
| `CORE_SOLUTION_REVIEW -> DASHBOARD (if any)` | `data-engineer` |
| `DASHBOARD -> FINAL_ACCEPTANCE` | `data-engineer` (dashboard analysis APPROVED) |
| `FINAL_ACCEPTANCE -> DONE` | `product-owner` (ACCEPTED) |

Rejected gates recycle only affected workstreams. The orchestrator never fabricates approvals.

## Runtime capabilities and limitations (honest)

**Supported by Claude Code runtime:**
- Persistent agent definitions discoverable across sessions
- Slash command registration
- Parallel subagent dispatch via multiple Agent tool calls in one message
- Fork subagent for context-sharing dispatches
- SendMessage / ListAgents for async live specialist coordination
- File-based project state

**Not supported (convention-enforced by the orchestrator instead):**
- Runtime-blocking approval gates (orchestrator must refuse to advance)
- DAG executor that survives restarts (orchestrator rebuilds the DAG on each invocation from `state.json`)
- Automatic retry with backoff (orchestrator decides on NEEDS_CHANGES returns)
- System-level task state machine (lives in `tasks/*.json`)
- Inter-agent message bus for completed subagents (completed agents are gone; new ones read state files)

## Schemas

See `multi-agent-engineering/schemas/` for the JSON contracts specialists emit back to the orchestrator.

## Templates

See `multi-agent-engineering/templates/` for the shape the orchestrator expects for `requirements.md`, `architecture.md`, `test_report.md`, `acceptance_report.md`.

## Standing rules every specialist must honor

- `CLAUDE.md` at repo root is non-negotiable
- `plan.md` and `ETL4 (3) (1).pdf` are the authoritative project spec
- `etl4/docs/decisions.md` gets an ADR for every schema or algorithm-parameter change
- No Claude attribution trailer on commits or PRs (CLAUDE.md §2.1)
- No secrets or large data committed (CLAUDE.md §2.2, §2.3)
- `icao24` is pandas string dtype (CLAUDE.md §2.6)
- Windows cp1252-safe output (CLAUDE.md §2.7)
- Use tokens minimally (user standing rule)
