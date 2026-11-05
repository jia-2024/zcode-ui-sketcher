@echo off
chcp 65001 >nul
title 草图转UI - ZCode版
cd /d "%~dp0"
echo 正在启动「草图转UI · ZCode 版」...
node server.js
pause
