@echo off
chcp 65001 >nul
title ETF联接基金合同知识库 Web应用

echo ============================================================
echo  ETF联接基金合同知识库 Web应用
echo  南方基金 - 合规工具
echo ============================================================
echo.

cd /d "%~dp0"

echo [1/4] Checking Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.8 or newer.
    pause
    exit /b 1
)

echo [2/4] Checking dependencies...
python -c "import flask, docx, jinja2" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing missing dependencies...
    python -m pip install flask python-docx jinja2 --quiet --disable-pip-version-check
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed.
        echo Please run: python -m pip install flask python-docx jinja2
        pause
        exit /b 1
    )
)

echo [3/4] Checking port 5000...
for /f %%P in ('powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique"') do (
    if not "%%P"=="0" (
        echo [ERROR] Port 5000 is already in use by process %%P.
        echo Please stop that process and run start.bat again.
        pause
        exit /b 1
    )
)

echo [4/4] Starting server from:
echo %CD%
echo.
echo URL: http://127.0.0.1:5000
echo Press Ctrl+C to stop the server.
echo.

python app.py

echo.
echo Server stopped.
pause
