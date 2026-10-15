@echo off
cd /d "%~dp0"
title EPOS WhatsApp gateway
echo Starting the EPOS WhatsApp gateway. Keep this window open.
:loop
node index.js
echo Gateway stopped, restarting in 10 seconds...
timeout /t 10 >nul
goto loop
