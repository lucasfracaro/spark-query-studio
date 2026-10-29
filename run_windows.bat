@echo off
cd /d "%~dp0"
if not exist .venv (python -m venv .venv && .venv\Scripts\pip install -r requirements.txt)
.venv\Scripts\streamlit run app.py --server.address localhost --browser.gatherUsageStats false
