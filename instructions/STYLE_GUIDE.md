# STYLE GUIDE (Project Rules)

This file is a short set of rules to prevent the project from becoming messy.

## Rule 1 — Design docs are written by ChatGPT
- SPEC/WORKFLOW/ROADMAP/DECISIONS/PROGRESS are authored in chat by ChatGPT.
- Codex only copies them into files and writes/edits code.

## Rule 2 — Excel is the control plane
- All config lives in config/run_config.xlsx
- Scripts read config/exports/config_snapshot_latest.json (never the open workbook)

## Rule 3 — No core logic in notebooks
- Notebooks are thin visualization/runners only.
- All real logic lives in src/ or tools/ orchestrators.

## Rule 4 — Modularity
- Prefer small modules with stable contracts.
- Avoid deep import chains.
- Prefer pure functions.

## Rule 5 — Instruction headers required
Every .py file must begin with an instruction header explaining:
- what it does
- where to run it
- inputs/outputs
- how to tell it worked

Enforced by tools/check_instruction_headers.py and a git pre-commit hook.
<CONTENT_END>
