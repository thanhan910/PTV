# Manual

## How to import an SHP file to a PostgreSQL database

Make sure that psql is available in the system.

```bash
psql -U postgres -h localhost -p 5432
```

You can also automatically authenticate with password by setting the `PGPASSWORD` environment variable.


On Linux:
```bash
export PGPASSWORD=postgres
```

On Windows:
```bat
set PGPASSWORD=postgres
```

1. Create a new database in PostgreSQL with PostGIS extension enabled.

Connect to the PostgreSQL server.

```bash
psql -U postgres -h localhost -p 5432
```

Create a new database with the following command:

```sql
CREATE DATABASE my_db OWNER postgres ENCODING UTF8 CONNECTION LIMIT -1;
```

Use the database name `my_db` for the rest of the steps.
```sql
\c vic_db
```

Create the PostGIS extension in the database.

```sql
CREATE EXTENSION postgis;
```

2. Import the SHP file to the database.

Prior to importing the SHP file, make sure that the `shp2pgsql` command is available in the system.

```bash
shp2pgsql -s <EPSG code> -I path/to/shapefile.shp schema.table_name | psql -d my_db -U postgres -h localhost -p 5432
```

Replace `<EPSG code>` with the EPSG code of the shapefile, for example, `4326`, or `3857`, or `28356`.

Check the EPSG code of the shapefile by running the following command:

```bash
ogrinfo path/to/shapefile.shp -al -so
```

You can also import a dbf file only, without having to specify the EPSG code.

```bash
shp2pgsql -I path/to/shapefile.dbf schema.table_name | psql -d my_db -U postgres -h localhost -p 5432
```


If you want to use a column name other than `geom` for the geometry column, you can specify it in the `shp2pgsql` command.

```bash
shp2pgsql -s <EPSG code> -g column_name -I path/to/shapefile.shp schema.table_name | psql -d my_db -U postgres -h localhost -p 5432
```

If you want to import the shapefile to an existing table, you can use the `-a` flag.

```bash
shp2pgsql -s <EPSG code> -I -a path/to/shapefile.shp schema.table_name | psql -d my_db -U postgres -h localhost -p 5432
```

If you want to use a column as the primary key, you can specify it in the `shp2pgsql` command.

```bash
shp2pgsql -s <EPSG code> -I -k column_name path/to/shapefile.shp schema.table_name | psql -d my_db -U postgres -h localhost -p 5432
```