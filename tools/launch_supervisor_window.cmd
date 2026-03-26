@echo off
setlocal

if "%~1"=="" (
  echo Usage: launch_supervisor_window.cmd CODEX_THREAD_ID
  exit /b 1
)

set "CODEX_THREAD_ID=%~1"
set "GEMINI_BATCH_TIMEOUT_BASE_SECONDS=120"
set "GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS=180"
title Teogonia Supervisor

wsl.exe bash -lc "cd /mnt/e/Media/신통기 && export CODEX_THREAD_ID='%CODEX_THREAD_ID%' GEMINI_BATCH_TIMEOUT_BASE_SECONDS='%GEMINI_BATCH_TIMEOUT_BASE_SECONDS%' GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS='%GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS%'; python3 tools/gemini_ui_supervisor.py"
