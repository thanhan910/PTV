@echo off
setlocal

@REM Get current timestamp in the format YYYYMMDD_HHMMSS
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set "timestamp=%datetime:~0,4%%datetime:~4,2%%datetime:~6,2%_%datetime:~8,2%%datetime:~10,2%%datetime:~12,2%"

@REM Get current timestamp to measure the time it takes to execute the script
set "script_start_time=%time%"
echo Script start time: %script_start_time%

@REM Folder where the script resides
set "script_folder=%~dp0"

@REM Folder where the GTFS ZIP file will be saved
set "download_folder=%script_folder%downloads\%timestamp%"

@REM Ensure the folder exists, create it if necessary
if not exist "%download_folder%" mkdir "%download_folder%"

@REM URL of the GTFS ZIP file
set "url=http://data.ptv.vic.gov.au/downloads/gtfs.zip"

@REM Name of the downloaded file
set "filename=gtfs.zip"

@REM Combine folder path and filename to get the full path
set "filepath=%download_folder%\%filename%"

@REM Download the file using PowerShell
powershell -Command "(New-Object Net.WebClient).DownloadFile('%url%', '%filepath%')"

echo GTFS file downloaded successfully to %filepath%

@REM Capture script end time
set "script_end_time=%time%"
echo Script end time: %script_end_time%

@REM Calculate the time taken by the script
@REM Convert start time to centiseconds for easy calculation
for /F "tokens=1-4 delims=:.," %%a in ("%script_start_time%") do set /a "start=((%%a*3600)+%%b*60+1%%c*100+1%%d-10000)*100"
@REM Convert end time to centiseconds for easy calculation
for /F "tokens=1-4 delims=:.," %%a in ("%script_end_time%") do set /a "end=((%%a*3600)+%%b*60+1%%c*100+1%%d-10000)*100"
@REM Calculate the difference
set /a "diff=end-start"
@REM Convert the difference back to hours, minutes, seconds, and centiseconds
set /a "hours=diff/(100*3600), remainder=diff%%(100*3600), minutes=remainder/(100*60), remainder=remainder%%(100*60), seconds=remainder/100, centiseconds=remainder%%100"
@REM Format and echo the time taken by the script
echo Time taken by the script: %hours%:%minutes%:%seconds%.%centiseconds%

endlocal
