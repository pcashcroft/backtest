# Project Style Guide: Clear Instructions Everywhere

This guide is the single source of truth for instruction headers.

## Non-Negotiable Rule
Every code file in `src/`, `tools/`, `tests/`, and `notebooks/` MUST include a top “Instruction Header”.

## Instruction Header Template
Each file must include a short header that answers:
- What this file does (plain English)
- Where it runs (terminal [command line], Jupyter [notebook], etc.)
- Inputs (files, folders, config snapshot)
- Outputs (files, folders, tables)
- How to run (exact commands; prefer `pybt`)
- What success looks like
- Common failures + fixes

## Notebooks
For notebooks (`.ipynb`), the first cell must be a markdown cell that contains the same Instruction Header.

## Checklist (Before Committing)
- Instruction Header present? (yes/no)

## Examples

### Example: Tool Script
```text
INSTRUCTION HEADER
What this file does: Creates the Excel control-plane workbook.
Where it runs: Terminal [command line].
Inputs: `src/backtest/config/schema.py`.
Outputs: `config/run_config.xlsx`.
How to run: `pybt tools/make_run_config_xlsx.py`
What success looks like: Prints “Created workbook...” and the file exists.
Common failures + fixes: openpyxl missing -> `pybt -m pip install openpyxl`.
```

### Example: Src Module
```text
INSTRUCTION HEADER
What this file does: Loads the config snapshot JSON into memory.
Where it runs: Called by other Python code (not directly in terminal).
Inputs: `config/exports/config_snapshot_latest.json`.
Outputs: In-memory data structure (Python dict).
How to run: Import and call the function from another script.
What success looks like: No errors and data returned.
Common failures + fixes: File missing -> run the snapshot export tool.
```

### Example: Notebook
```text
INSTRUCTION HEADER
What this file does: Visualizes a backtest result.
Where it runs: Jupyter [notebook].
Inputs: `E:\BacktestData\runs\...`.
Outputs: Charts and tables in the notebook.
How to run: Open the notebook and run all cells.
What success looks like: Charts render without errors.
Common failures + fixes: File missing -> run the backtest first.
```
