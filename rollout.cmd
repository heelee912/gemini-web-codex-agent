@echo off
setlocal EnableDelayedExpansion

where py >nul 2>&1
if %errorlevel%==0 (
  py -3 "%~dp0tools\rollout_cli.py" %*
  set "exit_code=!errorlevel!"
  exit /b !exit_code!
)

where python >nul 2>&1
if %errorlevel%==0 (
  python "%~dp0tools\rollout_cli.py" %*
  set "exit_code=!errorlevel!"
  exit /b !exit_code!
)

echo Python 3.11 or later was not found on PATH.
exit /b 1
