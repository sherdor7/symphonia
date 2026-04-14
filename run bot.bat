@echo off
echo Starting Backend...
start cmd /k python -m uvicorn backend:app --reload

timeout /t 3

echo Starting Ngrok...
start cmd /k ngrok http 8000

timeout /t 5

echo Starting Telegram Bot...
start cmd /k python bot.py

echo All services started.
pause