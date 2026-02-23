# Doc Update Protocol (Plain English)

Use these steps every time you change docs.

## 1) Archive the current docs
Command:
```powershell
$ts = Get-Date -Format yyyyMMdd_HHmmss
New-Item -ItemType Directory -Force -Path "archive/$ts" | Out-Null
```
Copy any doc you are about to change:
```powershell
Copy-Item -Force instructions/00_setup.md "archive/$ts/instructions/00_setup.md"
Copy-Item -Force instructions/01_daily_workflow.md "archive/$ts/instructions/01_daily_workflow.md"
Copy-Item -Force design/SPEC.md "archive/$ts/design/SPEC.md"
Copy-Item -Force design/WORKFLOW.md "archive/$ts/design/WORKFLOW.md"
Copy-Item -Force design/ROADMAP.md "archive/$ts/design/ROADMAP.md"
Copy-Item -Force design/DOC_UPDATE_PROTOCOL.md "archive/$ts/design/DOC_UPDATE_PROTOCOL.md"
```

## 2) Overwrite the docs
Edit the original files in place. Do not change the archive copies.

## 3) Optional commit and push
Commands:
```powershell
git add instructions/00_setup.md instructions/01_daily_workflow.md design/SPEC.md design/WORKFLOW.md design/ROADMAP.md design/DOC_UPDATE_PROTOCOL.md archive/$ts
git commit -m "Update docs"
git push
```
