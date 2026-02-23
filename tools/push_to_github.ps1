# INSTRUCTION HEADER
# What this does: Pushes your current local commits to GitHub (origin).
# Why: Codex may be unable to reach GitHub, but your workstation can.
# How to run:
#   powershell -ExecutionPolicy Bypass -File tools\push_to_github.ps1
# Success looks like: "Everything up-to-date" or a successful push summary.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

git status
git push