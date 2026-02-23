# WORKFLOW

Purpose:
- This file explains the *order of work* and how the pieces connect.
- It should not repeat the full spec. The spec is in design/SPEC.md.

## Folder roles (to prevent “all over the place”)
- design/ = what we are building + rules + decisions + progress
- instructions/ = only “how to run commands”
- tools/ = runnable scripts (orchestrators)
- src/ = reusable code modules (business logic)
- notebooks/ = thin visualizations only (no core logic)

## Required build order (must follow)
1) Ingest + storage foundation (raw/canonical/registry)
2) Derived tables + flexible charts (validate data quality)
3) PnL/execution engine (daily + intraday)
4) Feature system + caching
5) Optimization + constraints + robustness

## Config rule (always)
- Excel is edited by the user.
- Python scripts read config/exports/config_snapshot_latest.json.

## End-of-thread rule (always)
When the user says “start a new thread”:
1) ChatGPT writes updated design docs (SPEC/WORKFLOW/ROADMAP/DECISIONS/PROGRESS) in chat.
2) User pastes ONE Codex prompt that archives + overwrites docs exactly.
3) User runs tools/end_thread_handover.ps1 once.
4) User pastes the generated context_pack.md into the new chat thread.
<CONTENT_END>
