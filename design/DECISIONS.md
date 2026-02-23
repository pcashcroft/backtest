# DECISIONS

Purpose:
- This file is the “memory” of what we agreed.
- If we agree something in chat and it matters later, write it here.

Rules:
- Add new items at the bottom with a date.
- Keep it plain English.

---

## Decisions log

### 2026-02-23 — Design ownership + handover process
- ChatGPT writes design docs (SPEC/WORKFLOW/ROADMAP/DECISIONS/PROGRESS).
- Codex only copies design text and writes/edits code.
- End of every thread: update design docs → commit/push → regenerate context pack.

### 2026-02-23 — What not to commit
- Do not commit:
  - archive/
  - context_pack.md
  - notebook checkpoints (.ipynb_checkpoints/)
These are local/generated only.

### 2026-02-23 — Instruction headers are mandatory
- Every Python file must contain an INSTRUCTION HEADER near the top.
- Enforced by tools/check_instruction_headers.py and a git pre-commit hook.

### 2026-02-23 — Daily consolidated dataset ingestion
- Source file: E:\BacktestData\raw\consolidated.xlsm
- Date column name: Date
- Columns ingested:
  SpxCombined_pos
  SpxSystematic_pos
  SpxLS_pos
  SpxMF_pos
  SpxRetail_pos
  Spx_NetOptionsPositioning
  Spx_DlrGamma
  EUshorts_pos
  EUetf_pos
  EUrp_pos
  EUcta_pos
  EULS_pos
  EUMF_pos
  EUComb_pos
- Treated as z-scores (units=zscore).
- Timing assumption: populated 20:00 New York time (configurable).

### 2026-02-23 — Daily canonical storage format
- Canonical daily output is long format:
  date, series_id, value
- Saved under: E:\BacktestData\canonical\daily_series (partitioned by year)

### 2026-02-23 — ES trades canonical storage format
- Raw DBN files: E:\BacktestData\raw\Emini_trade_data\glbx-mdp3-YYYYMMDD.trades.dbn
- Canonical Parquet root: E:\BacktestData\canonical\es_trades
- Partitioning:
  session=FULL/date=YYYY-MM-DD/...
  session=RTH/date=YYYY-MM-DD/...
- Columns kept:
  ts_event, ts_recv, symbol, price, size, side, sequence, flags
- RTH definition:
  09:30–16:00 America/New_York, weekdays only.
  US holidays may have no RTH trades (expected).
<CONTENT_END>
