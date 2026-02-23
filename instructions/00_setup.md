# Setup (Windows)

## 1) Create venv (optional)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

## 2) Install requirements
```powershell
python -m pip install -r requirements.txt
```

## 3) Confirm SSD path exists
Ensure `E:\BacktestData` is available (SSD).

## 4) Bootstrap foundation
```powershell
python tools/bootstrap_foundation.py
```

## 5) Open/edit Excel and export JSON
- Edit `config/run_config.xlsx` in Excel.
- Export a JSON snapshot using:
```powershell
python tools/export_config_snapshot.py
```
