@echo off
setlocal EnableExtensions

chcp 65001 >nul 2>nul

set "APP_NAME=English Reading Assistant"
set "APP_DIR=%~dp0"
set "PORT=3000"
set "MODE=%~1"
set "STARTUP_LINK=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\English Reading Assistant.lnk"
set "LOG_DIR=%APP_DIR%logs"
set "LOG_FILE=%LOG_DIR%\auto-start.log"
set "SERVER_LOG_FILE=%LOG_DIR%\server.log"

if not "%ERA_PORT%"=="" set "PORT=%ERA_PORT%"
if "%MODE%"=="" set "MODE=launch"
set "APP_URL=http://localhost:%PORT%"

if /I "%MODE%"=="install" goto install
if /I "%MODE%"=="uninstall" goto uninstall
if /I "%MODE%"=="launch" goto launch_prod
if /I "%MODE%"=="app" goto launch_app
if /I "%MODE%"=="dev" goto launch_dev
if /I "%MODE%"=="serve-prod" goto serve_prod
if /I "%MODE%"=="serve-dev" goto serve_dev
if /I "%MODE%"=="status" goto status

echo Usage:
echo   auto-start.bat             Start in background and open http://localhost:%PORT%/login
echo   auto-start.bat app         Start in background and open a desktop app window
echo   auto-start.bat dev         Start dev server in background and open browser
echo   auto-start.bat install     Register current user startup shortcut
echo   auto-start.bat uninstall   Remove current user startup shortcut
echo   auto-start.bat status      Check port %PORT%
exit /b 1

:install
call :ensure_log_dir
call :log Installing startup shortcut for %APP_NAME%.
powershell -NoProfile -ExecutionPolicy Bypass -Command "$shell = New-Object -ComObject WScript.Shell; $shortcut = $shell.CreateShortcut('%STARTUP_LINK%'); $shortcut.TargetPath = '%~f0'; $shortcut.Arguments = 'launch'; $shortcut.WorkingDirectory = '%APP_DIR%'; $shortcut.WindowStyle = 7; $shortcut.Description = 'Start English Reading Assistant on login'; $shortcut.Save()"
if errorlevel 1 (
  echo Failed to install startup shortcut.
  exit /b 1
)
echo Installed: %STARTUP_LINK%
echo The app will start automatically next time this Windows user signs in.
exit /b 0

:uninstall
if exist "%STARTUP_LINK%" (
  del "%STARTUP_LINK%"
  echo Removed: %STARTUP_LINK%
) else (
  echo Startup shortcut was not found.
)
exit /b 0

:launch_prod
call :ensure_log_dir
call :port_check
if errorlevel 2 goto open_browser
call :log Launching production server in background on port %PORT%.
start "%APP_NAME%" /min cmd /c ""%~f0" serve-prod >> "%SERVER_LOG_FILE%" 2>>&1"
call :wait_for_port
goto open_browser

:launch_app
call :ensure_log_dir
call :port_check
if errorlevel 2 goto open_app_window
call :log Launching local app server in background on port %PORT%.
start "%APP_NAME%" /min cmd /c ""%~f0" serve-prod >> "%SERVER_LOG_FILE%" 2>>&1"
call :wait_for_port
goto open_app_window

:launch_dev
call :ensure_log_dir
call :port_check
if errorlevel 2 goto open_browser
call :log Launching development server in background on port %PORT%.
start "%APP_NAME% Dev" /min cmd /c ""%~f0" serve-dev >> "%SERVER_LOG_FILE%" 2>>&1"
call :wait_for_port
goto open_browser

:serve_prod
call :prepare || exit /b 1
if not exist ".next" (
  call :log Production build was not found. Building first.
  call npm run build
  if errorlevel 1 exit /b 1
)
call :log Starting production server at http://localhost:%PORT%
call npm run start -- --port %PORT%
exit /b %errorlevel%

:serve_dev
call :prepare || exit /b 1
call :log Starting development server at http://localhost:%PORT%
call npm run dev -- --port %PORT%
exit /b %errorlevel%

:status
call :port_check
if errorlevel 2 (
  echo Running: http://localhost:%PORT%/login
) else (
  echo Not running on port %PORT%.
)
exit /b 0

:prepare
cd /d "%APP_DIR%"
if not exist "package.json" (
  call :log Cannot find package.json in %APP_DIR%
  exit /b 1
)
if not exist "node_modules\.bin\next.cmd" (
  call :log Dependencies are missing. Installing packages.
  call npm install --no-audit --no-fund
  if errorlevel 1 exit /b 1
)
exit /b 0

:port_check
powershell -NoProfile -ExecutionPolicy Bypass -Command "if (Get-NetTCPConnection -LocalPort %PORT% -State Listen -ErrorAction SilentlyContinue) { exit 2 }"
if errorlevel 2 exit /b 2
exit /b 0

:wait_for_port
for /L %%i in (1,1,30) do (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "if (Get-NetTCPConnection -LocalPort %PORT% -State Listen -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }"
  if not errorlevel 1 exit /b 0
  timeout /t 1 /nobreak >nul
)
call :log Server did not become ready within 30 seconds. Check %LOG_FILE%
exit /b 1

:open_browser
call :log Opening browser at http://localhost:%PORT%/login
start "" "http://localhost:%PORT%/login"
exit /b 0

:open_app_window
call :log Opening desktop app window at http://localhost:%PORT%
call :find_browser
if "%APP_BROWSER%"=="" goto open_browser
start "%APP_NAME%" "%APP_BROWSER%" --app=http://localhost:%PORT%
exit /b 0

:find_browser
set "APP_BROWSER="
if not "%ERA_BROWSER%"=="" if exist "%ERA_BROWSER%" set "APP_BROWSER=%ERA_BROWSER%"
if not "%APP_BROWSER%"=="" exit /b 0
if exist "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" set "APP_BROWSER=%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"
if not "%APP_BROWSER%"=="" exit /b 0
if exist "%ProgramFiles%\Microsoft\Edge\Application\msedge.exe" set "APP_BROWSER=%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"
if not "%APP_BROWSER%"=="" exit /b 0
if exist "%LocalAppData%\Microsoft\Edge\Application\msedge.exe" set "APP_BROWSER=%LocalAppData%\Microsoft\Edge\Application\msedge.exe"
if not "%APP_BROWSER%"=="" exit /b 0
where msedge.exe >nul 2>nul
if not errorlevel 1 set "APP_BROWSER=msedge.exe"
if not "%APP_BROWSER%"=="" exit /b 0
where chrome.exe >nul 2>nul
if not errorlevel 1 set "APP_BROWSER=chrome.exe"
exit /b 0

:ensure_log_dir
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>nul
exit /b 0

:log
echo [%date% %time%] %*>> "%LOG_FILE%"
exit /b 0
