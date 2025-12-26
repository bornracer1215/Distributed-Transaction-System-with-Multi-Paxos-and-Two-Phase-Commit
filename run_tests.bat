@echo off
echo ============================================================
echo Running CSV Test Suite
echo ============================================================
echo.

if not exist "CSE535-F25-Project-3-Demo-Tests.csv" (
    echo ERROR: test_set.csv not found!
    echo Please create test_set.csv in the current directory.
    pause
    exit /b 1
)

echo Starting tests in 3 seconds...
timeout /t 3 /nobreak

echo.
python client.py CSE535-F25-Project-3-Demo-Tests.csv

echo.
echo ============================================================
echo Tests complete!
echo ============================================================
pause