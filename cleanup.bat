@echo off
echo Cleaning up databases and logs...

if exist data rmdir /s /q data
if exist logs rmdir /s /q logs

mkdir data
mkdir logs

echo Cleanup complete!
echo.
echo You can now start the clusters fresh.
pause