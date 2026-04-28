@echo off
chcp 65001 >nul
title Grid Trading Tests

echo ============================================
echo   Grid Trading - Running Tests...
echo ============================================
echo.

py -m unittest discover -s tests -v

pause
