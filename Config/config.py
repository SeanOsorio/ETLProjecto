class Config:
    """
    Clase de configuración para rutas y parámetros del ETL.
    """
    INPUT_PATH = '/workspaces/ETLProject/Extract/Files/ncr_ride_bookings.csv'
    SQLITE_DB_PATH = '/workspaces/ETLProject/Extract/Files/etl_data.db'
    SQLITE_TABLE = 'ride_bookings_clean'
