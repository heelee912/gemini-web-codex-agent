@echo off
setlocal
call "%~dp0rollout.cmd" setup --install-deps %*
exit /b %errorlevel%
