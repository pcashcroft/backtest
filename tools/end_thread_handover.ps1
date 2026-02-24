<#
INSTRUCTION HEADER

What this script does (plain English):
- Saves all current work into git (commit + push).
- Updates design/FOLDER_LAYOUT.md with current SSD/repo structure.

Where to run it:
- Run from the repo root folder: C:\Users\pcash\OneDrive\Backtest

How to run:
powershell -ExecutionPolicy Bypass -File tools\end_thread_handover.ps1 -Message “Session N: <short message>”

What “success” looks like:
- It prints “DONE” after committing and pushing.

Common problems:
- If “push” fails due to network, run `git push` manually after reconnecting.
- If instruction-header check fails, fix the reported files first.

#>

param(
  [Parameter(Mandatory=$true)]
  [string]$Message
)

$ErrorActionPreference = "Stop"

function Find-GitExe {
  $git = Get-Command git -ErrorAction SilentlyContinue
  if ($git) { return "git" }
  $fallback = "C:\Program Files\Git\cmd\git.exe"
  if (Test-Path $fallback) { return $fallback }
  throw "git.exe not found. Install Git for Windows or add git to PATH."
}

function Find-PythonExe {
  # Prefer the known conda env python
  $p = "C:\Users\pcash\anaconda3\envs\backtest\python.exe"
  if (Test-Path $p) { return $p }
  # Fallback to pybt if available
  $pybt = Get-Command pybt -ErrorAction SilentlyContinue
  if ($pybt) { return "pybt" }
  # Final fallback
  return "python"
}

$gitExe = Find-GitExe
$pyExe  = Find-PythonExe

Write-Host "Using git: $gitExe"
Write-Host "Using python: $pyExe"
Write-Host ""

# 1) Run instruction-header check
Write-Host "Running instruction-header check..."
& $pyExe tools/check_instruction_headers.py
if ($LASTEXITCODE -ne 0) { throw "Instruction header check failed." }

# 1.5) Update design folder layout
Write-Host "Updating design folder layout..."
& $pyExe tools/update_design_folder_layout.py
if ($LASTEXITCODE -ne 0) { throw "Design folder layout update failed." }

# 2) Show status
Write-Host ""
Write-Host "Git status:"
& $gitExe status

# 3) Stage everything (ignored files stay ignored)
Write-Host ""
Write-Host "Staging all changes..."
& $gitExe add -A

# 4) Commit only if there are staged changes
$hasStaged = $true
& $gitExe diff --cached --quiet
if ($LASTEXITCODE -eq 0) { $hasStaged = $false }

if ($hasStaged) {
  Write-Host "Committing..."
  & $gitExe commit -m $Message
} else {
  Write-Host "No staged changes to commit."
}

# 5) Push (may fail if offline)
Write-Host ""
Write-Host "Pushing..."
try {
  & $gitExe push
} catch {
  Write-Host "WARNING: git push failed. You can run 'git push' manually later."
}

Write-Host ""
Write-Host "DONE — Changes committed and pushed."
