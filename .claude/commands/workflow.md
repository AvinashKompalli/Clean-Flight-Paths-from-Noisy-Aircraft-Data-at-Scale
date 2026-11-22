---
description: Activate the ETL4 multi-agent engineering organization to execute or continue a project. The Chief Orchestrator parses the request, loads any existing project state, dispatches specialists in dependency order, and enforces convention-based approval gates. Use for any end-to-end delivery that needs product + architecture + implementation + test + acceptance work coordinated.
argument-hint: <project description | PROJECT-ID | 'continue PROJ-xxx' | 'fix defects from PROJ-xxx' | 'accept PROJ-xxx'>
---

# /workflow $ARGUMENTS

Invoke the Chief Orchestrator to coordinate the ETL4 multi-agent engineering organization.

## What this command does

1. Routes the request to the `orchestrator` agent via the Agent tool.
2. The orchestrator:
   - Inspects `multi-agent-engineering/projects/` for prior state matching the request
   - Decides new-project vs continuation vs change-request vs defect-fix vs revalidation vs final-acceptance
   - Dispatches the right specialist agents in dependency order, in parallel where safe
   - Persists state to `multi-agent-engineering/projects/<project_id>/state.json` after every meaningful step
   - Enforces convention-based approval gates (runtime does not block; the orchestrator does)
   - Reports a consolidated status back to this conversation

## Invocation

Launch the orchestrator with the user's exact request:

Use the Agent tool with:
- `subagent_type`: `orchestrator`
- `description`: `Orchestrate: $ARGUMENTS`
- `prompt`: an initialization message that includes:
  - the user's literal request: `$ARGUMENTS`
  - a reminder that the project workspace lives under `multi-agent-engineering/projects/`
  - a directive to check `multi-agent-engineering/projects/` for any existing project that matches, and to resume rather than restart
  - the current working directory: `B:\IISC\DE\Proj`
  - a reminder that ETL4 standing rules in `CLAUDE.md` apply to every specialist

## Examples

- `/workflow Build a customer analytics application`
- `/workflow Add deviation detection metrics to the matrix per PDF spec`
- `/workflow Continue PROJ-20261114-wi1-metrics`
- `/workflow Fix the defects in PROJ-20261114-wi1-metrics/test_report.md`
- `/workflow Accept PROJ-20261114-wi1-metrics`

## Limitations (honest statement)

Claude Code's agent runtime supports agent definitions and parallel dispatch, but:
- Approval gates are convention-enforced by the orchestrator, not blocked by the runtime
- Project state persists in disk files; the system does not remember across invocations without those files
- The orchestrator cannot preempt a specialist mid-run; failures surface at the next return
- Inter-agent messaging between async specialists uses SendMessage; batch dispatch via Agent tool is one-shot per specialist
