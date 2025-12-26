@echo off
echo ============================================================
echo Starting Distributed Transaction System
echo ============================================================
echo.

echo Creating directories...
if not exist "data" mkdir data
if not exist "logs" mkdir logs

echo.
echo Starting all 9 nodes in separate windows...
echo.

start "Node 1" cmd /k python node.py 1
timeout /t 1 /nobreak >nul

start "Node 2" cmd /k python node.py 2
timeout /t 1 /nobreak >nul

start "Node 3" cmd /k python node.py 3
timeout /t 1 /nobreak >nul

start "Node 4" cmd /k python node.py 4
timeout /t 1 /nobreak >nul

start "Node 5" cmd /k python node.py 5
timeout /t 1 /nobreak >nul

start "Node 6" cmd /k python node.py 6
timeout /t 1 /nobreak >nul

start "Node 7" cmd /k python node.py 7
timeout /t 1 /nobreak >nul

start "Node 8" cmd /k python node.py 8
timeout /t 1 /nobreak >nul

start "Node 9" cmd /k python node.py 9

echo.
echo ============================================================
echo All nodes started!
echo ============================================================
echo.
echo 9 node windows are now open
echo Waiting 5 seconds for leader election...
timeout /t 5 /nobreak

echo.
echo ============================================================
echo System is running!
echo ============================================================
echo.
echo To run CSV tests, open a NEW terminal and type:
echo   python client.py test_sets.csv
echo.
echo OR use: run_tests.bat
echo.
echo To stop: Close all node windows
echo.
pause