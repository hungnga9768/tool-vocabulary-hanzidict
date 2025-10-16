@echo off
echo Starting memory-safe translation process...
echo This will run with garbage collection enabled to prevent memory issues.
echo.

REM Run with garbage collection enabled and increased memory limit
node --expose-gc --max-old-space-size=4096 translator-full-data.js

echo.
echo Process completed. Check the console output above for results.
pause
