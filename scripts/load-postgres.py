import time
import subprocess
import os
import psycopg2
import threading
import logging
import sys


# Create a logger

def create_logger(log_filepath, level=logging.INFO, name = "root"):

    os.makedirs(os.path.dirname(log_filepath), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a handler for logging to file
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create a handler for logging to console
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger



def track_row_count(log_file):
    if os.path.exists(log_file):
        lines_count = len(open(log_file).readlines())
        logger.info(f"{lines_count} rows written to {log_file} in the last 1 second.")
    # Sleep for 1 second
    threading.Timer(1.0, track_row_count, args=(log_file,)).start()


if __name__ == "__main__":

    logger = create_logger("../local/logs/load-postgres.log")

    DATASHARE_DIR = "../local/datashare"
    SHP_DIRNAMES = ["PTV", "TRANSPORT", "VMADD", "VMPROP", "VMTRANS"]
    SHP_DIRS = {dirname: os.path.join(DATASHARE_DIR, dirname) for dirname in SHP_DIRNAMES}
    SHP_FILEDIRS = {
        dirname: {
            filename.split(".")[0]: dirpath
            for filename in os.listdir(dirpath)
            if filename.endswith(".shp")
        }
        for dirname, dirpath in SHP_DIRS.items()
    }
    user = "postgres"
    password = "postgres"
    with open("local-create-database.sql", "w") as f:

        for dirname, filedirs in SHP_FILEDIRS.items():
            database_name = dirname.lower()

            sql = f"DROP DATABASE IF EXISTS {database_name};"
            f.write(sql + "\n")
            sql = f"CREATE DATABASE {database_name} WITH OWNER = {user} ENCODING = 'UTF8' CONNECTION LIMIT = -1;"
            f.write(sql + "\n")
            # Use the database
            sql = f"\\c {database_name};"
            f.write(sql + "\n")
            sql = f"CREATE EXTENSION IF NOT EXISTS postgis;"
            f.write(sql + "\n")
        


    # conn = psycopg2.connect(
    #     dbname="postgres",
    #     user="postgres",
    #     password="postgres",
    #     host="localhost",
    #     port="5432",
    # )

    # conn.autocommit = True  # Set autocommit mode to True

    # cursor = conn.cursor()
    
    # for dirname, filedirs in SHP_FILEDIRS.items():
    #     database_name = dirname.lower()
    #     # Drop the database if it exists
    #     cursor.execute(f"DROP DATABASE IF EXISTS {database_name};")
    #     logger.info(f"[DROPPED] Dropped database {database_name}.")

    # for dirname, filedirs in SHP_FILEDIRS.items():
    #     database_name = dirname.lower()
    #     # Create a new database using psycopg2
    #     cursor.execute(
    #         f"CREATE DATABASE {database_name} WITH OWNER = {user} ENCODING = 'UTF8' CONNECTION LIMIT = -1;"
    #     )
    #     logger.info(f"[CREATED] Created database {database_name}.")
    #     # Create extensions
    #     cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    #     logger.info(f"[CREATED] Created postgis extension in database {database_name}.")

    # conn.commit()

    # cursor.close()
    # conn.close()

    if not os.path.exists("../local/logs/db"):
        os.makedirs(f"../local/logs/db", exist_ok=True)
    else:
        for file in os.listdir("../local/logs/db"):
            os.remove(f"../local/logs/db/{file}")

    # Create a batch file to load the shapefiles into the database
    with open("local-load-postgres.bat", "w") as f:
        f.write("@echo on\n")
        # Set password
        f.write(f'set PGPASSWORD={password}\n')
        
        f.write('psql -U postgres -a -f "local-create-database.sql"\n')
        '''
        psql "postgresql://postgres:postgres@localhost:5432/postgres" -a -f "local-create-database.sql"
        '''
        for dirname, filedirs in SHP_FILEDIRS.items():
            database_name = dirname.lower()
            for shp_name, shp_filedir in filedirs.items():
                shp_filepath = os.path.join(shp_filedir, f"{shp_name}.shp")
                shp_filepath = os.path.abspath(shp_filepath)
                table_name = shp_name.lower()
                log_filepath = f"../local/logs/db/{database_name}_{table_name}.log"
                log_filepath = os.path.abspath(log_filepath)
                command = f'shp2pgsql -s 7844 -I "{shp_filepath}" public.{table_name} | psql "postgresql://{user}:{password}@localhost:5432/{database_name}" >> "{log_filepath}"'
                f.write(command + "\n")
                command = f'echo "Loaded {shp_name}.shp into database {database_name} table {table_name}."'
                f.write(command + "\n")
                command = f'echo "Loaded {shp_name}.shp into database {database_name} table {table_name}." >> "{log_filepath}"'
                f.write(command + "\n")

        f.write("pause")

    # Execute the batch file
    # result = subprocess.call("local-load-postgres.bat", shell=True)

    # for dirname, filedirs in SHP_FILEDIRS.items():
    #     database_name = dirname
    #     for shp_name, shp_filedir in filedirs.items():
    #         # for shp_name, shp_dirpath in shp_path_info.items():
    #         shp_filepath = os.path.join(shp_filedir, f"{shp_name}.shp")
    #         shp_filepath = os.path.abspath(shp_filepath)
    #         table_name = shp_name
    #         log_filepath = f"../local/logs/db/{database_name}_{table_name}.log"
    #         log_filepath = os.path.abspath(log_filepath)
    #         command = f'shp2pgsql -s 7844 -I {shp_filepath} {table_name} | psql "postgresql://{user}:{password}@localhost:5432/{database_name}" >> "{log_filepath}"'


            # try:
            #     # Start the row count tracking thread in a background process
            #     # thread = threading.Thread(target=track_row_count, args=(f"{database_name}_{table_name}.log",))
            #     # thread.start()
            #     # Execute the commands and also capture the output in the console
            #     result = subprocess.call(command, shell=True)
                
            #     # result = subprocess.run(
            #     #     command,
            #     #     shell=True,
            #     #     stdout=subprocess.PIPE,
            #     #     stderr=subprocess.STDOUT,
            #     #     text=True,
            #     # )
                
            #     # with open(f"../local/logs/db/{database_name}_{table_name}.log", "a") as f:
            #     #     f.write(result.stdout.decode("utf-8"))
            #     logger.info(f"[FINISHED] Loaded {shp_name}.shp into database {database_name} table {table_name}.")

            #     # # Troubleshooting
            #     # # Count the number of rows in the table
            #     # for _ in range(5):
            #     #     try:
            #     #         conn = psycopg2.connect(
            #     #             dbname=database_name,
            #     #             user=user,
            #     #             password=password,
            #     #             host="localhost",
            #     #             port="5432",
            #     #         )
            #     #         cursor = conn.cursor()
            #     #         cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            #     #         count = cursor.fetchone()[0]
            #     #         logger.info(f"Number of rows in {table_name}: {count}")
            #     #         cursor.close()
            #     #         conn.close()
            #     #     except Exception as e:
            #     #         logger.error(f"Error counting rows in {table_name}.")
            #     #         logger.error(e)
            #     #         continue
                    
            #     #     time.sleep(5)

                
            # except subprocess.CalledProcessError as e:
            #     logger.error(f"Error loading {shp_name}.shp into database {database_name} table {table_name}.")
            #     logger.error(e.output)
            #     continue
