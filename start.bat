@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if %errorlevel%==0 (
  python wb_usage_server.py %*
  goto :end
)

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 wb_usage_server.py %*
  goto :end
)

for /d %%D in ("%USERPROFILE%\.workbuddy\binaries\python\versions\*") do (
  if exist "%%~fD\python.exe" (
    "%%~fD\python.exe" wb_usage_server.py %*
    goto :end
  )
)

echo.
echo Python 3.8+ not found. Install it from https://www.python.org/downloads/
echo.
pause

:end
