#!/bin/bash

# Get current timestamp in the format YYYYMMDD_HHMMSS
timestamp=$(date +"%Y%m%d_%H%M%S")

# Get current timestamp to measure the time it takes to execute the script
script_start_time=$(date +"%T")
echo "Script start time: $script_start_time"

# Folder where the script resides
script_folder="$(dirname "$0")"

# Folder where the GTFS ZIP file will be saved
download_folder="$script_folder/downloads/$timestamp"

# Ensure the folder exists, create it if necessary
mkdir -p "$download_folder"

# URL of the GTFS ZIP file
url="http://data.ptv.vic.gov.au/downloads/gtfs.zip"

# Name of the downloaded file
filename="gtfs.zip"

# Combine folder path and filename to get the full path
filepath="$download_folder/$filename"

# Download the file using curl
curl -o "$filepath" "$url"

echo "GTFS file downloaded successfully to $filepath"

# Capture script end time
script_end_time=$(date +"%T")
echo "Script end time: $script_end_time"

# Calculate the time taken by the script
# Convert start time to seconds since epoch
start=$(date -d "$script_start_time" +"%s")
# Convert end time to seconds since epoch
end=$(date -d "$script_end_time" +"%s")
# Calculate the difference in seconds
diff=$((end - start))
# Calculate hours, minutes, and seconds from the difference
hours=$((diff / 3600))
minutes=$(( (diff % 3600) / 60 ))
seconds=$((diff % 60))

echo "Time taken by the script: $hours:$minutes:$seconds"

exit 0
