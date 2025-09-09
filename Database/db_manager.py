"""
Database Management Utilities for ETL Project
Provides utility functions for database inspection and management.
"""

import sqlite3
import logging
from typing import List, Dict, Any, Optional
from Database.db_creator import DatabaseCreator
from Config.config import Config


class DatabaseManager:
    """
    Utility class for managing and inspecting the ETL database.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize DatabaseManager with database path.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path or Config.SQLITE_DB_PATH
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """
        Get a connection to the database.
        
        Returns:
            Optional[sqlite3.Connection]: Database connection or None if failed
        """
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None
    
    def get_table_list(self) -> List[str]:
        """
        Get a list of all tables in the database.
        
        Returns:
            List[str]: List of table names
        """
        connection = self.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            connection.close()
            return tables
        except sqlite3.Error as e:
            self.logger.error(f"Error getting table list: {e}")
            connection.close()
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the schema information for a specific table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of column information dictionaries
        """
        connection = self.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            connection.close()
            
            # Convert to more readable format
            schema = []
            for col in columns:
                schema.append({
                    'column_id': col[0],
                    'name': col[1],
                    'type': col[2],
                    'not_null': bool(col[3]),
                    'default_value': col[4],
                    'primary_key': bool(col[5])
                })
            
            return schema
        except sqlite3.Error as e:
            self.logger.error(f"Error getting table schema for {table_name}: {e}")
            connection.close()
            return []
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get the number of records in a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            int: Number of records, -1 if error
        """
        connection = self.get_connection()
        if not connection:
            return -1
        
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            connection.close()
            return count
        except sqlite3.Error as e:
            self.logger.error(f"Error getting count for table {table_name}: {e}")
            connection.close()
            return -1
    
    def get_etl_logs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent ETL process logs.
        
        Args:
            limit (int): Maximum number of logs to retrieve
            
        Returns:
            List[Dict[str, Any]]: List of ETL log dictionaries
        """
        connection = self.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor()
            cursor.execute('''
                SELECT log_id, process_name, start_time, end_time, status, 
                       records_processed, error_message, created_at
                FROM etl_logs 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (limit,))
            
            logs = []
            for row in cursor.fetchall():
                logs.append({
                    'log_id': row[0],
                    'process_name': row[1],
                    'start_time': row[2],
                    'end_time': row[3],
                    'status': row[4],
                    'records_processed': row[5],
                    'error_message': row[6],
                    'created_at': row[7]
                })
            
            connection.close()
            return logs
        except sqlite3.Error as e:
            self.logger.error(f"Error getting ETL logs: {e}")
            connection.close()
            return []
    
    def get_database_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of the database.
        
        Returns:
            Dict[str, Any]: Database summary information
        """
        summary = {
            'database_path': self.db_path,
            'tables': {},
            'total_records': 0,
            'recent_etl_logs': []
        }
        
        # Get all tables and their information
        tables = self.get_table_list()
        for table in tables:
            count = self.get_table_count(table)
            schema = self.get_table_schema(table)
            
            summary['tables'][table] = {
                'record_count': count,
                'column_count': len(schema),
                'columns': [col['name'] for col in schema]
            }
            
            if count > 0:
                summary['total_records'] += count
        
        # Get recent ETL logs
        summary['recent_etl_logs'] = self.get_etl_logs(5)
        
        return summary
    
    def export_table_to_csv(self, table_name: str, output_path: str) -> bool:
        """
        Export a table to CSV format.
        
        Args:
            table_name (str): Name of the table to export
            output_path (str): Path where to save the CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        connection = self.get_connection()
        if not connection:
            return False
        
        try:
            # Use pandas if available, otherwise manual export
            try:
                import pandas as pd
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", connection)
                df.to_csv(output_path, index=False)
            except ImportError:
                # Manual CSV export if pandas not available
                import csv
                cursor = connection.cursor()
                cursor.execute(f"SELECT * FROM {table_name}")
                
                with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                    # Get column names
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    writer = csv.writer(csvfile)
                    writer.writerow(columns)
                    
                    # Write data
                    cursor.execute(f"SELECT * FROM {table_name}")
                    for row in cursor.fetchall():
                        writer.writerow(row)
            
            connection.close()
            self.logger.info(f"Table {table_name} exported to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting table {table_name}: {e}")
            connection.close()
            return False
    
    def reset_database(self) -> bool:
        """
        Reset the database by dropping and recreating all tables.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            db_creator = DatabaseCreator(self.db_path)
            
            # Connect and drop tables
            if db_creator.connect():
                if db_creator.drop_all_tables():
                    db_creator.disconnect()
                    # Reinitialize the database
                    return db_creator.initialize_database()
                else:
                    db_creator.disconnect()
                    return False
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error resetting database: {e}")
            return False


def print_database_summary(db_path: str = None):
    """
    Print a formatted summary of the database to console.
    
    Args:
        db_path (str): Path to the database file
    """
    db_manager = DatabaseManager(db_path)
    summary = db_manager.get_database_summary()
    
    print("=" * 50)
    print("DATABASE SUMMARY")
    print("=" * 50)
    print(f"Database Path: {summary['database_path']}")
    print(f"Total Records: {summary['total_records']}")
    print(f"Total Tables: {len(summary['tables'])}")
    print()
    
    print("TABLES:")
    print("-" * 30)
    for table_name, table_info in summary['tables'].items():
        print(f"  {table_name}:")
        print(f"    Records: {table_info['record_count']}")
        print(f"    Columns: {table_info['column_count']}")
        print(f"    Column Names: {', '.join(table_info['columns'])}")
        print()
    
    if summary['recent_etl_logs']:
        print("RECENT ETL LOGS:")
        print("-" * 30)
        for log in summary['recent_etl_logs']:
            print(f"  Process: {log['process_name']} | Status: {log['status']} | Records: {log['records_processed']}")
            print(f"    Started: {log['start_time']}")
            if log['end_time']:
                print(f"    Ended: {log['end_time']}")
            if log['error_message']:
                print(f"    Error: {log['error_message']}")
            print()


if __name__ == "__main__":
    # Example usage
    print_database_summary()
