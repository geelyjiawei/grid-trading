@echo off
chcp 65001 >nul
title Grid Trading

echo ============================================
echo   Grid Trading - Starting...
echo ============================================
echo.

py --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python was not found. Please install Python 3.11 or newer.
    echo Download: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/2] Installing dependencies...
py -m pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)

echo.
echo [2/2] Starting server...
echo Open http://localhost:8000
echo Press Ctrl+C to stop.
echo.

cd backend
py -m uvicorn main:app --host 0.0.0.0 --port 8000

pause
