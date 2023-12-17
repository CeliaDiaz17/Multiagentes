import psycopg2
from psycopg2 import OperationalError


def ddbb_connection_decorator(func):
    def wrapper(*args, **kwargs):
        conexion_db = Connect()

        if not conexion_db.open_connection():
            print("No se pudo establecer la conexión a la base de datos.")
            return None

        try:
            resultado = func(*args, **kwargs)
            return resultado

        finally:
            conexion_db.close_connection()

    return wrapper


def leer_configuracion(archivo_config):
    config = {}
    with open(archivo_config, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            config[key] = value
    return config


class Connect:
    def __init__(self):
        # Establecer tus propias credenciales de conexión aquí
        self.user = None
        self.password = None
        self.host = None
        self.port = None
        self.database = None
        self.connection = None
        self.cursor = None

    def open_connection(self):
        config = leer_configuracion('config.txt')
        try:
            self.connection = psycopg2.connect(
                user=config.get('user'),
                password=config.get('password'),
                host=config.get('host'),
                port=config.get('port'),
                database=config.get('dbname')
            )
            self.cursor = self.connection.cursor()
            print("Conexión a PostgreSQL establecida.")
        except psycopg2.OperationalError as e:
            print(f"Error durante la conexión a PostgreSQL: {e}")
            self.connection = None
            self.cursor = None
            return False

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Conexión a PostgreSQL cerrada")

    # @ddbb_connection_decorator
    def execute_query(self, consulta):
        self.open_connection()
        if self.connection is None:
            print("La conexión a la base de datos no está establecida.")
            return None
        cursor = self.connection.cursor()
        cursor.execute(consulta)
        results = cursor.fetchall()
        self.close_connection()
        return results

# Función decoradora para manejar la conexión
