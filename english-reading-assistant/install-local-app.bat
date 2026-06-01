@echo off
setlocal EnableExtensions

chcp 65001 >nul 2>nul

set "APP_NAME=英读助手"
set "APP_DIR=%~dp0"
set "LAUNCHER=%APP_DIR%auto-start.bat"
set "DESKTOP_LINK=%USERPROFILE%\Desktop\英读助手.lnk"
set "START_MENU_DIR=%APPDATA%\Microsoft\Windows\Start Menu\Programs\英读助手"
set "START_MENU_LINK=%START_MENU_DIR%\英读助手.lnk"

if not exist "%LAUNCHER%" (
  echo Cannot find auto-start.bat in %APP_DIR%
  exit /b 1
)

if not exist "%START_MENU_DIR%" mkdir "%START_MENU_DIR%" >nul 2>nul

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$shell = New-Object -ComObject WScript.Shell;" ^
  "$desktop = $shell.CreateShortcut('%DESKTOP_LINK%');" ^
  "$desktop.TargetPath = '%LAUNCHER%';" ^
  "$desktop.Arguments = 'app';" ^
  "$desktop.WorkingDirectory = '%APP_DIR%';" ^
  "$desktop.WindowStyle = 7;" ^
  "$desktop.Description = 'Open English Reading Assistant as a local desktop app';" ^
  "$desktop.Save();" ^
  "$start = $shell.CreateShortcut('%START_MENU_LINK%');" ^
  "$start.TargetPath = '%LAUNCHER%';" ^
  "$start.Arguments = 'app';" ^
  "$start.WorkingDirectory = '%APP_DIR%';" ^
  "$start.WindowStyle = 7;" ^
  "$start.Description = 'Open English Reading Assistant as a local desktop app';" ^
  "$start.Save();"

if errorlevel 1 (
  echo Failed to create local app shortcuts.
  exit /b 1
)

echo Installed local app shortcuts:
echo   Desktop: %DESKTOP_LINK%
echo   Start Menu: %START_MENU_LINK%
echo.
echo You can now open %APP_NAME% like a local app.
exit /b 0
