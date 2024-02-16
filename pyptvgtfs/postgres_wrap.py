from sqlalchemy import create_engine
import psycopg2


class PostGreSQLWrapper:
    def __init__(self, db_name, db_user="postgres", db_password="postgres", db_host="localhost", db_port=5432) -> None:
        
        # Create the database URL
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        # Create an SQLAlchemy engine to connect to the database
        engine = create_engine(db_url)

        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        cursor = connection.cursor()

        self.engine = engine
        self.connection = connection
        self.cursor = cursor

    def __del__(self):
        self.cursor.close()
        self.connection.close()

        

