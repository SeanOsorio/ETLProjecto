import os

class Config:
    """
    Clase de configuración para rutas y parámetros del ETL.
    """
    # Base paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Input data paths
    INPUT_PATH = os.path.join(PROJECT_ROOT, 'Extract', 'Files', 'steam_games.csv')
    OUTPUT_CLEAN_PATH = os.path.join(PROJECT_ROOT, 'Extract', 'Files', 'steam_games_clean.csv')
    
    # Database configuration
    DATABASE_DIR = os.path.join(PROJECT_ROOT, 'Database')
    SQLITE_DB_PATH = os.path.join(DATABASE_DIR, 'steam_games.db')
    
    # Table names
    GAMES_TABLE = 'games'
    DEVELOPERS_TABLE = 'developers'
    PUBLISHERS_TABLE = 'publishers'
    GENRES_TABLE = 'genres'
    ETL_LOGS_TABLE = 'etl_logs'
    
    # Legacy table names (for backward compatibility)
    RIDES_TABLE = 'games'  # Map to games table
    LOCATIONS_TABLE = 'genres'
    PAYMENT_TYPES_TABLE = 'developers'
    SQLITE_TABLE = 'games'
    
    # ETL Configuration
    BATCH_SIZE = 1000
    MAX_RETRIES = 3
    
    # Logging configuration
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
