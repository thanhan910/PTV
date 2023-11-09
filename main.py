import requests
import zipfile
import io
import pandas as pd

# # URL of the main GTFS ZIP file
# url = "http://data.ptv.vic.gov.au/downloads/gtfs.zip"

def process_gtfs_from_zipfile_object(main_zip_ref):
    # Create a dictionary to store all data
    all_data = {}

    # Iterate through the file list in the main GTFS ZIP
    for file_name in main_zip_ref.namelist():
        # Check if the item is a directory
        if file_name.endswith('/'):
            subdir_name = file_name.strip('/')
            
            # Convert the folder name to an integer
            folder_number = int(subdir_name)
            
            # Look for the nested ZIP file inside the subdirectory
            nested_zip_path = f"{subdir_name}/google_transit.zip"
            
            # Check if the nested ZIP file exists in the subdirectory
            if nested_zip_path in main_zip_ref.namelist():
                # Create a dictionary to store DataFrames for each folder
                folder_data = {}
                
                # Extract the nested ZIP contents directly from memory
                with main_zip_ref.open(nested_zip_path) as nested_zip_file:
                    with zipfile.ZipFile(io.BytesIO(nested_zip_file.read())) as nested_zip_ref:
                        nested_file_list = nested_zip_ref.namelist()
                        for nested_file_name in nested_file_list:
                            if nested_file_name.endswith('.txt'):
                                with nested_zip_ref.open(nested_file_name) as nested_file:
                                    # Read the CSV content as a Pandas DataFrame
                                    folder_data[nested_file_name.removesuffix('.txt')] = pd.read_csv(nested_file)
                
                # Create a Pandas Series for the folder data
                all_data[int(folder_number)] = pd.Series(folder_data)

            else:
                print("Nested ZIP file not found in", subdir_name)

    # Convert the dictionary to a Pandas Series
    all_data = pd.Series(all_data) 

    # Sort the series by the folder number
    all_data.sort_index(inplace=True)

    return all_data

def process_gtfs_from_url(url):
    # Send an HTTP GET request to get the main GTFS ZIP file content
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        # Create a ZipFile object from the response content
        with zipfile.ZipFile(io.BytesIO(response.content)) as main_zip_ref:
            return process_gtfs_from_zipfile_object(main_zip_ref)

    else:
        print(response, "Failed to fetch the main GTFS ZIP file.")

def process_gtfs_from_local_zip(zip_path):
    # Create a ZipFile object from the local ZIP file
    with zipfile.ZipFile(zip_path) as main_zip_ref:
        return process_gtfs_from_zipfile_object(main_zip_ref)