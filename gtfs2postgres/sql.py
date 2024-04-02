import os

create_sql_file_path = os.path.join(os.path.dirname(__file__), "../sql/create.sql")
with open(create_sql_file_path, "r") as f:
    create_gtfs_sql = f.read()
    

full_sql_script = ""
for mode_id in [1, 2, 3, 4, 5, 6, 7, 8, 10, 11]:
    schema_name = f"gtfs_{mode_id}"
    table_create_sql = f"""
DROP SCHEMA IF EXISTS {schema_name} CASCADE;
CREATE SCHEMA IF NOT EXISTS {schema_name};
SET search_path TO {schema_name};
{create_gtfs_sql}
"""
    full_sql_script += table_create_sql

sql_file_path = "../sql/gtfs.sql"

sql_file_path = os.path.join(os.path.dirname(__file__), sql_file_path)

with open(sql_file_path, "w") as f:
    f.write(full_sql_script)

