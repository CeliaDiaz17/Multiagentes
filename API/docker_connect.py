import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os


def pandas_to_sql_type(pd_type):
    if pd_type == "object":
        return "TEXT"
    elif pd_type == "int64":
        return "BIGINT"
    elif pd_type == "float64":
        return "DOUBLE PRECISION"
    elif pd_type == "datetime64[ns]":
        return "TIMESTAMP"
    else:
        return "TEXT"


class docker_connect:
    def __init__(self):
        config = leer_configuracion('config.txt')
        self.host = config.get('host')
        self.port = config.get('port')
        self.dbname = config.get('dbname')
        self.user = config.get('user')
        self.password = config.get('password')

        self.connection = None
        self.cursor = None

    def connect(self):
        conn_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        engine = create_engine(conn_string)
        return engine

    def dump_data(self, directory):
        engine = self.connect()
        csv_directory = directory
        csv_files = [f for f in os.listdir(
            csv_directory) if f.endswith('.csv')]
        for csv_file in csv_files:
            table_name = csv_file.replace(".csv", "")

            chunksize = 10000  # puedes ajustar este valor según tus necesidades
            for chunk in pd.read_csv(os.path.join(csv_directory, csv_file), chunksize=chunksize):
                df = chunk
                column_types = ", ".join(
                    [f"{col} {pandas_to_sql_type(str(df[col].dtype))}" for col in df.columns])
                create_table_sql = f"CREATE TABLE if not exists {table_name} ({column_types});"

                # Crear la tabla
                with engine.connect() as conn:
                    conn.execute(text(create_table_sql))

                # Insertar los datos en la tabla
                # cambiamos 'replace' por 'append'
                df.to_sql(table_name, engine, if_exists='append', index=False)

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed.")


def leer_configuracion(archivo_config):
    config = {}
    with open(archivo_config, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            config[key] = value
    return config


if __name__ == '__main__':
    """Basic connector for testing, """
    directory = "/gold"
    connector = docker_connect()
    connector.dump_data(directory)
    connector.close_connection()
