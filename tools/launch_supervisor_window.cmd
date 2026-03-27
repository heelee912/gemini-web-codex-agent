@echo off
setlocal

if "%~1"=="" (
  echo Usage: launch_supervisor_window.cmd CODEX_THREAD_ID
  exit /b 1
)

set "CODEX_THREAD_ID=%~1"
set "GEMINI_BATCH_TIMEOUT_BASE_SECONDS=120"
set "GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS=180"
set "PROJECT_ROOT_WIN=%~dp0.."
for %%I in ("%PROJECT_ROOT_WIN%") do set "PROJECT_ROOT_WIN=%%~fI"

if not defined PROJECT_ROOT_WSL (
  for /f "usebackq delims=" %%I in (`wsl.exe wslpath -a "%PROJECT_ROOT_WIN%"`) do set "PROJECT_ROOT_WSL=%%I"
)

if not defined PROJECT_ROOT_WSL (
  echo Failed to resolve the project root inside WSL.
  exit /b 1
)

set "SUPERVISOR_SCRIPT_WSL=%PROJECT_ROOT_WSL%/tools/gemini_ui_supervisor.py"

title Subtitle Rollout Supervisor

wsl.exe env ^
  "CODEX_THREAD_ID=%CODEX_THREAD_ID%" ^
  "GEMINI_BATCH_TIMEOUT_BASE_SECONDS=%GEMINI_BATCH_TIMEOUT_BASE_SECONDS%" ^
  "GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS=%GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS%" ^
  python3 "%SUPERVISOR_SCRIPT_WSL%"
