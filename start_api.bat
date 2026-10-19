@echo off
title Quotex Live Data API
if not exist .env (
  copy .env.example .env >nul
  echo [!] .env banano hoyeche. email/password dite .env edit koro, then abar run koro.
  notepad .env
  pause
  exit /b
)
pip install -r requirements.txt
python -m uvicorn app:app --host 127.0.0.1 --port 8000
pause
