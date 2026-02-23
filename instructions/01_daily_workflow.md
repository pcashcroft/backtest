# Daily Workflow — Simple

This file is only “what to run”.
Project rules and requirements are in design/SPEC.md.

## Every day (or whenever you update Excel or data)
1) Update your source files (append-only).
2) Update config/run_config.xlsx in Excel if needed.
3) Close Excel.
4) Export the config snapshot:
   - pybt tools/export_config_snapshot.py

## Ingest daily consolidated dataset
- pybt tools/ingest_daily_consolidated.py

## Ingest ES trades (Databento DBN files)
- C:\Users\pcash\anaconda3\envs\backtest\python.exe tools\ingest_es_trades_databento.py --incremental --only-session BOTH

## Starting a new chat thread (REQUIRED process)
When you want to start a new chat thread:

1) Tell ChatGPT: “start a new thread”.
2) ChatGPT will produce updated design docs in chat.
3) You paste one Codex prompt that applies those docs.
4) You run the handover script once:
   - powershell -ExecutionPolicy Bypass -File tools\end_thread_handover.ps1 -Message "Thread handover: <short message>"
5) Paste the newly generated context_pack.md into the new chat thread.
<CONTENT_END>
