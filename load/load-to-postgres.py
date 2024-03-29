import psycopg2
import subprocess
import os
import psycopg2
import json
import logging
import sys


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



if __name__ == '__main__':

    LOG_DIR = '../local/logs/vic_db' 
    LOG_DIR = os.path.join(os.path.dirname(__file__), LOG_DIR)
    os.makedirs(LOG_DIR, exist_ok=True)
    DATA_DIR = '../local/datashare/data'
    DATA_DIR = os.path.join(os.path.dirname(__file__), DATA_DIR)


    logger = create_logger('../local/logs/load-to-postgres.log')


    # Prepare and build load commands
    TABLES = []
    SCHEMAS = [dirname.lower() for dirname in os.listdir(DATA_DIR)]
    for dirname in os.listdir(DATA_DIR):
        files = os.listdir(os.path.join(DATA_DIR, dirname))
        shp_files = [file.removesuffix('.shp') for file in files if file.endswith('.shp')]
        dbf_files = [file.removesuffix('.dbf') for file in files if file.endswith('.dbf')]
        assert all(filename in dbf_files for filename in shp_files)
        dbf_no_shp_files = [filename for filename in dbf_files if filename not in shp_files]
        filenames = [(filename, 'shp') for filename in shp_files] + [(filename, 'dbf') for filename in dbf_no_shp_files]

        schema_name = dirname.lower()
        for filename, file_extension in filenames:
            table_name = filename.lower()
            filepath = os.path.join(DATA_DIR, dirname, f'{filename}.{file_extension}')
            filepath = os.path.abspath(filepath)
            
            column_names_txt = f'{filename.lower()}_column_names.txt'
            if filename == 'ADDRESS_1':
                column_names_txt = f'address_column_names.txt'

            column_names_txt_path = os.path.join(DATA_DIR, dirname, column_names_txt)
            with open(column_names_txt_path) as f:
                shp_column_names = [line.strip() for line in f.readlines()][4:]
                shp_column_names = { line.split(' = ')[0]: line.split(' = ')[1] for line in shp_column_names }

            TABLES.append((schema_name, table_name, filepath, shp_column_names))


    
    BAT_COMMANDS = []
    BAT_COMMANDS.append('set PGPASSWORD=postgres')
    BAT_COMMANDS.append('psql -U postgres -h localhost -p 5432 -c "CREATE DATABASE vic_db OWNER postgres ENCODING \'UTF8\' CONNECTION LIMIT -1"')
    BAT_COMMANDS.append('psql -U postgres -h localhost -p 5432 -d vic_db -c "CREATE EXTENSION IF NOT EXISTS postgis"')
    for schema_name in SCHEMAS:
        BAT_COMMANDS.append(f'psql -U postgres -h localhost -p 5432 -d vic_db -c "CREATE SCHEMA IF NOT EXISTS {schema_name}"')

    for (schema_name, table_name, filepath, shp_column_names) in TABLES:
        log_path = os.path.abspath(os.path.join(LOG_DIR, f'{schema_name}.{table_name}.log'))
        BAT_COMMANDS.append(f'echo Loading {schema_name}.{table_name} >> "{log_path}"')
        BAT_COMMANDS.append(f'shp2pgsql -s 7844 "{filepath}" "{schema_name}.{table_name}" | psql -d vic_db -U postgres -h localhost -p 5432 >> "{log_path}"')
        BAT_COMMANDS.append(f'echo Finished loading {schema_name}.{table_name}')
        BAT_COMMANDS.append(f'echo Finished loading {schema_name}.{table_name} >> "{log_path}"')

    RENAME_COLUMN_SQL_COMMANDS = []

    for (schema_name, table_name, filepath, shp_column_names) in TABLES:
        sql = ' '.join(f"ALTER TABLE {schema_name}.{table_name} RENAME COLUMN {old_column_name.lower()} TO {new_column_name.lower()};" for old_column_name, new_column_name in shp_column_names.items() if old_column_name != new_column_name)
        RENAME_COLUMN_SQL_COMMANDS.append(sql)

    for sql in RENAME_COLUMN_SQL_COMMANDS:
        BAT_COMMANDS.append(f'psql -U postgres -h localhost -p 5432 -d vic_db -c "{sql}"')

    with open(os.path.join(os.path.dirname(__file__), 'load-to-postgres.bat'), 'w') as f:
        for bat_command in BAT_COMMANDS:
            f.write(bat_command)
            f.write('\n')

    # # Execute the bat commands
    # logger.info('Executing load-to-postgres.bat')
    # subprocess.run('load-to-postgres.bat', shell=True)
    # logger.info('Finished executing load-to-postgres.bat')



    # Create database and enable postgis extension if not exists and needed
    
    # # TODO: If needed, uncomment this block of code to create the database and enable postgis extension
    # # Create connection to postgres database
    # with psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port='5432') as conn1:
    #     conn1.autocommit = True
    #     with conn1.cursor() as cursor1:
    #         conn1.commit() # commit any open transactions
    #         cursor1.execute('DROP DATABASE IF EXISTS vic_db')
    #         logger.info(cursor1.statusmessage)
    #         cursor1.execute('CREATE DATABASE vic_db OWNER postgres ENCODING UTF8 CONNECTION LIMIT -1')
    #         logger.info(cursor1.statusmessage)
    

    # Create new connection to vic_db database
    # with psycopg2.connect(dbname='vic_db', user='postgres', password='postgres', host='localhost', port='5432') as conn:
    #     conn.autocommit = True
    #     with conn.cursor() as cursor:
    #         cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis')
    #         logger.info(cursor.statusmessage)
    #         logger.info('Created postgis extension')

    #     for schema_name in SCHEMAS:
    #         logger.info(f'Creating schema {schema_name}')
    #         cursor.execute(f'CREATE SCHEMA {schema_name};')
    #         logger.info(cursor.statusmessage)
    #         # cursor.fetchone()
    #         logger.info(f'Created schema {schema_name}')
        
    #     with open('load-to-postgres.bat', 'w') as f:
    #         for bat_command in BAT_COMMANDS:
    #             f.write(bat_command)
    #             f.write('\n')
        
    #     # Execute the bat commands
    #     logger.info('Executing load-to-postgres.bat')
    #     subprocess.run('load-to-postgres.bat', shell=True)
    #     logger.info('Finished executing load-to-postgres.bat')

    #     # for bat_command in BAT_COMMANDS:
    #     #     logger.info(bat_command)
    #     #     process = subprocess.Popen(bat_command, shell=True, stdout=subprocess.PIPE)
    #     #     process.wait()
    #     #     logger.info(process.returncode)  # should print 0 if the command was successful

    #     SQL_COMMANDS = []

    #     for (schema_name, table_name, filepath, shp_column_names) in TABLES:
    #         sql = f'ALTER TABLE {schema_name}.{table_name} '
    #         sql += ', '.join(f'RENAME COLUMN {old_column_name.lower()} TO {new_column_name.lower()}' for old_column_name, new_column_name in 
    #         shp_column_names.items())
    #         sql += ';'
    #         SQL_COMMANDS.append(sql)

    #     for sql in SQL_COMMANDS:
    #         logger.info(sql)
    #         cursor.execute(sql)
    #         logger.info(cursor.statusmessage)

    #     # cursor.close()
    #     # conn.close()
