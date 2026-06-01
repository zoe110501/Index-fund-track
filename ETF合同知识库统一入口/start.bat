@echo off
chcp 65001 >nul
title ETF 合同知识库统一入口

echo ============================================================
echo  ETF 合同知识库统一入口
echo  请选择 ETF 或 ETF联接 进入对应系统
echo ============================================================
echo.

cd /d "%~dp0"

echo [1/3] Checking Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.8 or newer.
    pause
    exit /b 1
)

echo [2/3] Checking dependencies...
python -c "import flask" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing Flask...
    python -m pip install flask --quiet --disable-pip-version-check
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed.
        echo Please run: python -m pip install flask
        pause
        exit /b 1
    )
)

echo [3/3] Starting launcher with automatic port selection...

echo.
echo Preferred launcher URL: http://127.0.0.1:5000
echo If port 5000 is busy, the launcher will choose another free port.
echo ETF system preferred port: 5001
echo ETF linked system preferred port: 5002
echo Child system ports are also selected automatically when needed.
echo Press Ctrl+C to stop all systems started by this launcher.
echo.

python app.py

echo.
echo Launcher stopped.
pause
