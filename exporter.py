import pandas as pd
from sqlalchemy import create_engine
from config import URL_DB
import psycopg2


class DataExporter:
    def __init__(self):
        self.db_url = URL_DB

    def export_to_database(self, dataframes, table_names):
        engine = create_engine(self.db_url)

        if len(dataframes) != len(table_names):
            raise ValueError("Number of dataframes and table names should be the same.")

        with engine.connect() as connection:
            for df, table_name in zip(dataframes, table_names):
                df.to_sql(table_name, connection, schema='staging', if_exists='append', index=False)

        print("Data exported to database successfully.")

    def export_to_excel(self, dataframes, file_names):
        if len(dataframes) != len(file_names):
            raise ValueError("Number of dataframes and file names should be the same.")

        for df, file_name in zip(dataframes, file_names):
            df.to_excel(file_name, index=False)

        print("Data exported to Excel files successfully.")

