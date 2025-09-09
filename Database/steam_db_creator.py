"""
Steam Games Database Creator Module
Handles database initialization and table creation for Steam games data.
"""

import sqlite3
import os
import logging
from typing import Optional


class SteamGamesDBCreator:
    """
    Handles the creation and initialization of the SQLite database for Steam games ETL project.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize SteamGamesDBCreator with database path.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration for the database operations."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """
        Create connection to the SQLite database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            self.connection = sqlite3.connect(self.db_path)
            self.logger.info(f"Successfully connected to database: {self.db_path}")
            return True
        
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
    
    def create_tables(self) -> bool:
        """
        Create all necessary tables for the Steam games ETL project.
        
        Returns:
            bool: True if tables created successfully, False otherwise
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Create games table (main table)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS games (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    game_type TEXT,
                    name TEXT NOT NULL,
                    desc_snippet TEXT,
                    recent_reviews TEXT,
                    all_reviews TEXT,
                    release_date TEXT,
                    developer TEXT,
                    publisher TEXT,
                    popular_tags TEXT,
                    game_details TEXT,
                    languages TEXT,
                    achievements INTEGER,
                    genre TEXT,
                    game_description TEXT,
                    mature_content TEXT,
                    minimum_requirements TEXT,
                    recommended_requirements TEXT,
                    original_price REAL,
                    discount_price REAL,
                    final_price REAL,
                    discount_percentage REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create developers table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS developers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    games_count INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create publishers table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS publishers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    games_count INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create genres table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS genres (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    games_count INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create ETL logs table for tracking processing
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS etl_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    process_name TEXT NOT NULL,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME,
                    status TEXT CHECK(status IN ('STARTED', 'COMPLETED', 'FAILED')) NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_name ON games(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_developer ON games(developer)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_publisher ON games(publisher)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_genre ON games(genre)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_release_date ON games(release_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_price ON games(original_price)')
            
            self.connection.commit()
            self.logger.info("All tables created successfully")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"Error creating tables: {e}")
            return False
    
    def drop_all_tables(self) -> bool:
        """
        Drop all tables (useful for resetting the database).
        
        Returns:
            bool: True if tables dropped successfully, False otherwise
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            # Drop each table
            for table in tables:
                table_name = table[0]
                if table_name != 'sqlite_sequence':  # Skip SQLite system table
                    cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            
            self.connection.commit()
            self.logger.info("All tables dropped successfully")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"Error dropping tables: {e}")
            return False
    
    def initialize_database(self) -> bool:
        """
        Complete database initialization process.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        self.logger.info("Starting Steam Games database initialization...")
        
        if not self.connect():
            return False
        
        if not self.create_tables():
            self.disconnect()
            return False
        
        self.disconnect()
        self.logger.info("Steam Games database initialization completed successfully")
        return True


def create_steam_games_database(db_path: str, reset: bool = False) -> bool:
    """
    Convenience function to create and initialize the Steam games database.
    
    Args:
        db_path (str): Path to the database file
        reset (bool): Whether to drop existing tables first
        
    Returns:
        bool: True if successful, False otherwise
    """
    db_creator = SteamGamesDBCreator(db_path)
    
    if reset:
        if db_creator.connect():
            db_creator.drop_all_tables()
            db_creator.disconnect()
    
    return db_creator.initialize_database()


if __name__ == "__main__":
    # Example usage
    db_path = os.path.join(os.path.dirname(__file__), "steam_games.db")
    
    if create_steam_games_database(db_path, reset=False):
        print("Steam Games database created successfully!")
    else:
        print("Failed to create Steam Games database")
