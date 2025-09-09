from Extract.extractor import Extractor
from Transform.transformer import Transformer
from Load.loader import Loader
from Database.steam_db_creator import create_steam_games_database
from Config.config import Config
import logging
import sys

def setup_logging():
    """Configure logging for the main ETL process."""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format=Config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('etl_process.log')
        ]
    )
    return logging.getLogger(__name__)

def main():
    """Main ETL process orchestrator."""
    logger = setup_logging()
    logger.info("Starting ETL process...")
    
    try:
        # Paso 0: Inicializar la base de datos
        logger.info("Initializing database...")
        if not create_steam_games_database(Config.SQLITE_DB_PATH, reset=False):
            logger.error("Failed to initialize database. Aborting ETL process.")
            return False
        logger.info("Database initialized successfully")
        
        # Paso 1: Extraer los datos
        logger.info("Starting data extraction...")
        extractor = Extractor(Config.INPUT_PATH)
        df = extractor.extract()
        
        if df is None:
            logger.error("Failed to extract data. Aborting ETL process.")
            return False
        logger.info(f"Extracted {len(df)} records successfully")

        # Paso 2: Transformar los datos
        logger.info("Starting data transformation...")
        transformer = Transformer(df)
        cleaned_df = transformer.clean()
        
        if cleaned_df is None:
            logger.error("Failed to transform data. Aborting ETL process.")
            return False
        logger.info(f"Transformed {len(cleaned_df)} records successfully")

        # Paso 3: Cargar los datos
        logger.info("Starting data loading...")
        loader = Loader(cleaned_df)
        
        # Load to CSV (existing functionality)
        if hasattr(Config, 'OUTPUT_CLEAN_PATH'):
            loader.to_csv(Config.OUTPUT_CLEAN_PATH)
            logger.info(f"Data saved to CSV: {Config.OUTPUT_CLEAN_PATH}")
        
        # Load to database (new functionality)
        if loader.to_database(Config.SQLITE_DB_PATH, Config.GAMES_TABLE):
            logger.info(f"Data loaded to database table: {Config.GAMES_TABLE}")
        else:
            logger.error("Failed to load data to database")
            return False
        
        logger.info("ETL process completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in ETL process: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

