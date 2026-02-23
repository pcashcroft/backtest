# DOC UPDATE PROTOCOL

Goal:
- Keep the repo as the “memory” of the project.
- Make starting a new chat thread painless and consistent.

## Non-negotiable rule
- ChatGPT writes all design text (SPEC/WORKFLOW/ROADMAP/DECISIONS/PROGRESS).
- Codex only copies that text into files and writes/edits code.

## When to run this protocol
Run this protocol whenever:
- a thread is ending, OR
- we agreed a change that matters later.

## The process (repeat every time)
1) ChatGPT produces updated full file contents in chat.
2) User pastes one Codex prompt that:
   - archives existing docs into archive/<timestamp>/...
   - overwrites docs exactly with ChatGPT’s text
3) User runs tools/end_thread_handover.ps1 once.
4) The script:
   - checks instruction headers
   - commits and pushes
   - regenerates context_pack.md and copies it to clipboard
5) User pastes context_pack.md into the next chat thread.

## What counts as “must be written down”
Anything that is agreed and might be needed later, for example:
- changes to chart requirements
- changes to execution assumptions
- new datasets/instruments
- changes to workflow order
This goes into design/DECISIONS.md and design/PROGRESS.md (and SPEC if it is a real requirement change).
<CONTENT_END>
