"""
Database Creator Module for ETL Project
Handles database initialization and table creation for ride booking data.
"""

import sqlite3
import os
import logging
from typing import Optional


class DatabaseCreator:
    """
    Handles the creation and initialization of the SQLite database for the ETL project.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize DatabaseCreator with database path.
        
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
        Create all necessary tables for the ride booking ETL project.
        
        Returns:
            bool: True if tables created successfully, False otherwise
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Create rides table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rides (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ride_id TEXT UNIQUE NOT NULL,
                    pickup_datetime TEXT NOT NULL,
                    pickup_locationid INTEGER,
                    dropoff_locationid INTEGER,
                    passenger_count INTEGER,
                    trip_distance REAL,
                    fare_amount REAL,
                    extra REAL,
                    mta_tax REAL,
                    tip_amount REAL,
                    tolls_amount REAL,
                    total_amount REAL,
                    payment_type INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create locations table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS locations (
                    location_id INTEGER PRIMARY KEY,
                    borough TEXT,
                    zone TEXT,
                    service_zone TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create payment_types table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS payment_types (
                    payment_type_id INTEGER PRIMARY KEY,
                    payment_method TEXT NOT NULL,
                    description TEXT,
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rides_pickup_datetime ON rides(pickup_datetime)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rides_pickup_location ON rides(pickup_locationid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rides_dropoff_location ON rides(dropoff_locationid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rides_payment_type ON rides(payment_type)')
            
            self.connection.commit()
            self.logger.info("All tables created successfully")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"Error creating tables: {e}")
            return False
    
    def insert_default_data(self) -> bool:
        """
        Insert default/reference data into lookup tables.
        
        Returns:
            bool: True if data inserted successfully, False otherwise
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Insert default payment types
            payment_types = [
                (1, 'Credit Card', 'Standard credit card payment'),
                (2, 'Cash', 'Cash payment'),
                (3, 'No Charge', 'No charge trip'),
                (4, 'Dispute', 'Disputed payment'),
                (5, 'Unknown', 'Unknown payment method'),
                (6, 'Voided Trip', 'Voided trip')
            ]
            
            cursor.executemany('''
                INSERT OR IGNORE INTO payment_types (payment_type_id, payment_method, description)
                VALUES (?, ?, ?)
            ''', payment_types)
            
            self.connection.commit()
            self.logger.info("Default data inserted successfully")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting default data: {e}")
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
    
    def get_table_info(self, table_name: str) -> list:
        """
        Get information about a specific table.
        
        Args:
            table_name (str): Name of the table to inspect
            
        Returns:
            list: Table schema information
        """
        if not self.connection:
            self.logger.error("No database connection available")
            return []
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return cursor.fetchall()
        
        except sqlite3.Error as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            return []
    
    def initialize_database(self) -> bool:
        """
        Complete database initialization process.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        self.logger.info("Starting database initialization...")
        
        if not self.connect():
            return False
        
        if not self.create_tables():
            self.disconnect()
            return False
        
        if not self.insert_default_data():
            self.disconnect()
            return False
        
        self.disconnect()
        self.logger.info("Database initialization completed successfully")
        return True


def create_database(db_path: str, reset: bool = False) -> bool:
    """
    Convenience function to create and initialize the database.
    
    Args:
        db_path (str): Path to the database file
        reset (bool): Whether to drop existing tables first
        
    Returns:
        bool: True if successful, False otherwise
    """
    db_creator = DatabaseCreator(db_path)
    
    if reset:
        if db_creator.connect():
            db_creator.drop_all_tables()
            db_creator.disconnect()
    
    return db_creator.initialize_database()


if __name__ == "__main__":
    # Example usage
    db_path = os.path.join(os.path.dirname(__file__), "ride_bookings.db")
    
    if create_database(db_path, reset=False):
        print("Database created successfully!")
    else:
        print("Failed to create database")
