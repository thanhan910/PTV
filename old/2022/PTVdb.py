import os
import zipfile
import urllib.request 
import shutil
import pandas as pd
from datetime import date, timedelta, datetime

# https://developers.google.com/transit/gtfs/reference

def download_file(url, to_filepath):
    urllib.request.urlretrieve(url, to_filepath)

def extract_zip(zipfile_path, to_folder_path):
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(to_folder_path)

def extract_all(directory):
    '''
    Recursive function to extract all zip files in targetdir, all zip files inside the zip files, and so on.

    Every zip file will be extracted to a folder with the same name
    '''

    for _rootdir, _dirs, _files in os.walk(directory):
        
        for f in _files:
            
            fpath = os.path.join(_rootdir, f)
            
            if(fpath.endswith(".zip")):

                directory_to_extract_to = fpath.rstrip(".zip")
                extract_zip(fpath, directory_to_extract_to)
                
                extract_all(directory_to_extract_to)

def extract_zip_all(zipfile_path : str):
    '''
    Recursive function to extract a zip file and all zip files inside the zip files.

    Every zip file will be extracted to a folder with the same name
    '''
    directory_to_extract_to = zipfile_path.rstrip(".zip")

    extract_zip(zipfile_path = zipfile_path, to_folder_path = directory_to_extract_to)

    extract_all(directory = directory_to_extract_to)

class PTVdb:
    def __init__(self, database_directory, gtfs_zip_name : str = "gtfs.zip", data_directory_name = "data") -> None:

        self.database_directory_path = database_directory

        if(not os.path.exists(self.database_directory_path)):
            os.makedirs(self.database_directory_path)

        self.gtfs_zip_name = gtfs_zip_name

        self.gtfs_directory_name = self.gtfs_zip_name.rstrip(".zip")

        self.data_directory_name = data_directory_name

        self.data_directory_path = os.path.join(self.database_directory_path, self.data_directory_name)

    URL = "http://data.ptv.vic.gov.au/downloads/gtfs.zip"

    CALENDAR_SERVICES = 'calendar_services'

    PRIMARYKEYS = {
        'agency' : ['agency_id'], 
        'calendar': ['service_id'], 
        'calendar_dates': ['service_id', 'date'], 
        'routes': ['route_id'], 
        'shapes': ['shape_id', 'shape_pt_sequence'], 
        'stops': ['stop_id'], 
        'stop_times': ['trip_id' , 'stop_sequence'], 
        'trips' : ['trip_id'],
        CALENDAR_SERVICES : ['date', 'service_id']
    }

    SERVICES = {
        '1' : "Regional Train",
        '2' : "Train",
        '3' : "Tram",
        '4' : "Bus",
        '5' : "Regional Coach",
        '6' : "Regional Bus",
        '7' : "undefined",
        '8' : "undefined",
        '10' : "Airport SkyTrain",
        '11' : "Airport SkyBus",
    }

    def transform_database(self, gtfs_directory, data_directory):
        for _rootdir, _dirs, _files in os.walk(gtfs_directory):

            for f in _files:

                fpath = os.path.join(_rootdir, f)

                if(fpath.endswith(".txt")):

                    ftxt = fpath.replace(gtfs_directory, data_directory).replace("\\google_transit", "")

                    fcsv = ftxt.replace(".txt", ".csv")

                    fdir = os.path.dirname(ftxt)

                    if(not os.path.exists(fdir)):
                        os.makedirs(fdir)

                    shutil.copy(fpath, fcsv)

                    shutil.copy(fpath, ftxt)

    def build_database(self, database_directory_path = None, remove_gtfs_directory = False, create_calendar_services = True):
        '''Import and build PTV GTFS database'''

        # https://developers.google.com/transit/gtfs/reference
        
        # [Finished in 74.4s]

        if(database_directory_path != None):
            self.database_directory_path = database_directory_path

        download_file(self.URL, os.path.join(self.database_directory_path, self.gtfs_zip_name))
        extract_zip_all(zipfile_path = os.path.join(self.database_directory_path, self.gtfs_zip_name))
        self.transform_database(
            gtfs_directory = os.path.join(self.database_directory_path, self.gtfs_directory_name), 
            data_directory = os.path.join(self.database_directory_path, self.data_directory_name)
        )

        self.data_directory_path = os.path.join(self.database_directory_path, self.data_directory_name)

        if(remove_gtfs_directory):
            os.remove(os.path.join(self.database_directory_path, self.data_directory_name))
            os.remove(os.path.join(self.database_directory_path, self.gtfs_zip_name))

        if(create_calendar_services):
            for service in self.SERVICES.keys():
                df = self.calendar_services(service=service, create_record=True)
                fcsv = os.path.join(self.data_directory_path, str(service), self.CALENDAR_SERVICES + ".csv")
                ftxt = os.path.join(self.data_directory_path, str(service), self.CALENDAR_SERVICES + ".txt")
                df.to_csv(fcsv)
                df.to_csv(ftxt)

    def select(self, service, record, columns=None):
        service = str(service)
        if(service not in self.SERVICES.keys()):
            raise ValueError("service has not existed")
        if(record not in self.PRIMARYKEYS.keys()):
            raise ValueError("record has not existed")

        csvpath = os.path.join(self.data_directory_path, service, record + ".csv")
        
        df = pd.read_csv(csvpath)

        if(columns != None): return df[columns]
        else: return df

    def calendar_services(self, service, create_record = True):
        '''
        Shows what service occur in each date
        '''
        calendars = {}

        df_calendars = self.select(service=service, record="calendar")
        df_calendar_dates = self.select(service=service, record="calendar_dates")

        for index, row in df_calendars.iterrows():

            start_date = datetime.strptime(str(row["start_date"]), '%Y%m%d')
            end_date = datetime.strptime(str(row["end_date"]), '%Y%m%d')

            delta = end_date - start_date   # returns timedelta

            for i in range(delta.days + 1):

                day = start_date + timedelta(days=i)

                daystring = day.strftime('%Y%m%d')

                daystring = int(daystring)

                try:
                    calendars[daystring]
                except:
                    calendars[daystring] = []

                weekday = day.strftime('%A').lower()

                if(row[weekday] == 1):
                    calendars[daystring].append(row["service_id"])

        for index, row in df_calendar_dates.iterrows():

            edate = int(row["date"])
            
            if(row["exception_type"] == 2 and row["service_id"] in calendars[edate]):
                calendars[edate].remove(row["service_id"])
            
            elif(row["exception_type"] == 1 and row["service_id"] not in calendars[edate]):
                calendars[edate].append(row["service_id"])
        
        if(not create_record): return calendars

        calendar_services = { "date" : [], "service_id" : [] }

        for key, value in calendars.items():
            for v in value:
                calendar_services["date"].append(key)
                calendar_services["service_id"].append(v)

        df = pd.DataFrame(data=calendar_services)

        df = df.sort_values(by="date")

        return df
                



