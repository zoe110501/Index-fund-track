@echo off
chcp 65001 >nul
title ETF基金合同知识库 Web应用

echo ============================================================
echo  ETF 基金合同知识库 Web 应用
echo  南方基金 · 合规工具
echo ============================================================
echo.

:: 检查 Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未找到 Python，请先安装 Python 3.8 或更高版本。
    echo       下载地址：https://www.python.org/downloads/
    pause
    exit /b 1
)

:: 安装依赖（如已安装则跳过）
echo [1/2] 检查并安装依赖（flask、python-docx）...
python -m pip install flask python-docx --quiet --disable-pip-version-check
if %errorlevel% neq 0 (
    echo [警告] pip 安装可能失败，尝试继续启动...
)

echo [2/2] 启动服务器，浏览器将自动打开...
echo.
echo  访问地址：http://127.0.0.1:5000
echo  按 Ctrl+C 停止服务器
echo.

:: 切换到脚本所在目录
cd /d "%~dp0"
python app.py

echo.
echo 服务器已停止。
pause
