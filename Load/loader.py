
from Config.config import Config
import sqlite3

class Loader:
    """
    Clase para cargar los datos limpios a un destino.
    """
    def __init__(self, df):
        self.df = df

    def to_csv(self, output_path):
        """
        Guarda el DataFrame limpio en un archivo CSV.
        """
        try:
            self.df.to_csv(output_path, index=False)
            print(f"Datos guardados en {output_path}")
        except Exception as e:
            print(f"Error al guardar datos: {e}")

    def to_sqlite(self, db_path=None, table_name=None):
        """
        Guarda el DataFrame limpio en una base de datos SQLite.
        """
        db_path = db_path or Config.SQLITE_DB_PATH
        table_name = table_name or Config.SQLITE_TABLE
        try:
            conn = sqlite3.connect(db_path)
            self.df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            print(f"Datos guardados en la base de datos SQLite: {db_path}, tabla: {table_name}")
        except Exception as e:
            print(f"Error al guardar en SQLite: {e}")
