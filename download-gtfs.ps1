param(
    [switch]$u
)

# Get current timestamp in the format YYYYMMDD_HHMMSS
$datetime = Get-Date -Format "yyyyMMdd_HHmmss"
$timestamp = $datetime -replace "[-.: ]", ""

# Get current timestamp to measure the time it takes to execute the script
$script_start_time = Get-Date -Format "HH:mm:ss"
Write-Host "Script start time: $script_start_time"

# Folder where the script resides
$script_folder = Split-Path -Parent $MyInvocation.MyCommand.Definition

# Folder where the GTFS ZIP file will be saved
$download_folder = Join-Path -Path $script_folder -ChildPath "downloads\$timestamp"

# Ensure the folder exists, create it if necessary
if (-not (Test-Path -Path $download_folder)) {
    New-Item -Path $download_folder -ItemType Directory | Out-Null
}

# URL of the GTFS ZIP file
$url = "http://data.ptv.vic.gov.au/downloads/gtfs.zip"

# Name of the downloaded file
$filename = "gtfs.zip"

# Combine folder path and filename to get the full path
$filepath = Join-Path -Path $download_folder -ChildPath $filename

# Download the file using PowerShell
(New-Object System.Net.WebClient).DownloadFile($url, $filepath)

Write-Host "GTFS file downloaded successfully to $filepath"

# Unzip the downloaded GTFS file if the switch is provided
if ($u) {

    # Unzip the downloaded GTFS file
    Write-Host "Unzipping GTFS file..."
    Expand-Archive -Path $filepath -DestinationPath "$download_folder\gtfs" -Force

    # Unzip google_transit.zip files inside all folders
    Write-Host "Unzipping google_transit.zip files inside all folders..."
    $gtfs_folders = Get-ChildItem -Path "$download_folder\gtfs" -Directory
    foreach ($folder in $gtfs_folders) {
        Write-Host "Processing folder $($folder.FullName)..."
        $google_transit_zip = Join-Path -Path $folder.FullName -ChildPath "google_transit.zip"
        if (Test-Path -Path $google_transit_zip) {
            Write-Host "Unzipping $google_transit_zip..."
            Expand-Archive -Path $google_transit_zip -DestinationPath $folder.FullName -Force
        } else {
            Write-Host "$google_transit_zip not found inside folder $($folder.FullName)"
        }
    }
}

# Capture script end time
$script_end_time = Get-Date -Format "HH:mm:ss"
Write-Host "Script end time: $script_end_time"

# Calculate the time taken by the script
$start = [DateTime]::ParseExact($script_start_time, "HH:mm:ss", $null).Ticks
$end = [DateTime]::ParseExact($script_end_time, "HH:mm:ss", $null).Ticks
$diff = ($end - $start) / 10000

# Format and echo the time taken by the script
$time_taken = [TimeSpan]::FromMilliseconds($diff).ToString("hh\:mm\:ss\.ff")
Write-Host "Time taken by the script: $time_taken"
