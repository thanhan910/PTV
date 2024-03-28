@echo off
if "%~1"=="" (
    echo Usage: %0 [path_to_file]
    exit /b 1
)

echo Searching for "INSERT 0 1"...
:loop
findstr /C:"INSERT 0 1" "%~1" | find /C /V ""
timeout /t 1 >nul
goto :loop
