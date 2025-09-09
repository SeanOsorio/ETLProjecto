
import pandas as pd
import sqlite3
import logging
from typing import Optional, Union
from datetime import datetime
from Config.config import Config


class Loader:
    """
    Clase para cargar los datos limpios a diferentes destinos (CSV, SQLite).
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize the Loader with a DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame to be loaded
        """
        self.df = df
        self.logger = logging.getLogger(__name__)
    
    def to_csv(self, output_path: str) -> bool:
        """
        Guarda el DataFrame limpio en un archivo CSV.
        
        Args:
            output_path (str): Path where to save the CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.df.to_csv(output_path, index=False)
            self.logger.info(f"Data saved to CSV: {output_path}")
            print(f"Datos guardados en {output_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            print(f"Error al guardar datos: {e}")
            return False
    
    def to_sqlite(self, db_path: Optional[str] = None, table_name: Optional[str] = None) -> bool:
        """
        Guarda el DataFrame limpio en una base de datos SQLite (método legacy).
        
        Args:
            db_path (str, optional): Database file path
            table_name (str, optional): Table name
            
        Returns:
            bool: True if successful, False otherwise
        """
        db_path = db_path or Config.SQLITE_DB_PATH
        table_name = table_name or Config.SQLITE_TABLE
        
        try:
            conn = sqlite3.connect(db_path)
            self.df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            self.logger.info(f"Data saved to SQLite: {db_path}, table: {table_name}")
            print(f"Datos guardados en la base de datos SQLite: {db_path}, tabla: {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving to SQLite: {e}")
            print(f"Error al guardar en SQLite: {e}")
            return False
    
    def to_database(self, db_path: str, table_name: str = 'games', batch_size: int = 1000) -> bool:
        """
        Loads data into the properly structured database with error handling and logging.
        
        Args:
            db_path (str): Path to the SQLite database
            table_name (str): Target table name (default: 'games')
            batch_size (int): Number of records to process at once
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            
            # Log the ETL process start
            process_start = datetime.now()
            cursor.execute('''
                INSERT INTO etl_logs (process_name, start_time, status, records_processed)
                VALUES (?, ?, ?, ?)
            ''', ('DATA_LOAD', process_start.isoformat(), 'STARTED', 0))
            log_id = cursor.lastrowid
            
            # Prepare the data for insertion
            records_processed = 0
            total_records = len(self.df)
            
            # Process data in batches
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = self.df.iloc[start_idx:end_idx]
                
                # Insert batch data
                batch_records = []
                for _, row in batch_df.iterrows():
                    # Map DataFrame columns to database columns
                    record = self._map_row_to_database_schema(row)
                    if record:
                        batch_records.append(record)
                
                if batch_records:
                    # Use INSERT OR IGNORE to handle duplicates
                    cursor.executemany('''
                        INSERT OR IGNORE INTO games (
                            url, game_type, name, desc_snippet, recent_reviews, all_reviews,
                            release_date, developer, publisher, popular_tags, game_details,
                            languages, achievements, genre, game_description, mature_content,
                            minimum_requirements, recommended_requirements, original_price,
                            discount_price, final_price, discount_percentage
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', batch_records)
                    
                    records_processed += len(batch_records)
                    
                    # Log progress
                    if records_processed % (batch_size * 5) == 0:
                        self.logger.info(f"Processed {records_processed}/{total_records} records")
            
            # Update ETL log with completion status
            process_end = datetime.now()
            cursor.execute('''
                UPDATE etl_logs 
                SET end_time = ?, status = ?, records_processed = ?
                WHERE log_id = ?
            ''', (process_end.isoformat(), 'COMPLETED', records_processed, log_id))
            
            connection.commit()
            connection.close()
            
            self.logger.info(f"Successfully loaded {records_processed} records to table: {table_name}")
            print(f"Successfully loaded {records_processed} records to database table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to database: {e}")
            
            # Log the error
            try:
                cursor.execute('''
                    UPDATE etl_logs 
                    SET end_time = ?, status = ?, error_message = ?
                    WHERE log_id = ?
                ''', (datetime.now().isoformat(), 'FAILED', str(e), log_id))
                connection.commit()
            except:
                pass
            
            if 'connection' in locals():
                connection.close()
            
            print(f"Error loading data to database: {e}")
            return False
    
    def _map_row_to_database_schema(self, row) -> Optional[tuple]:
        """
        Maps a DataFrame row to the database schema for Steam Games.
        Maps the Steam Games CSV columns to the database schema.
        
        Args:
            row: Pandas Series representing a row of data
            
        Returns:
            Optional[tuple]: Tuple of values for database insertion, or None if invalid
        """
        try:
            # Map Steam Games CSV columns to database schema
            url = str(getattr(row, 'url', ''))
            game_type = str(getattr(row, 'types', ''))
            name = str(getattr(row, 'name', ''))
            desc_snippet = str(getattr(row, 'desc_snippet', ''))
            recent_reviews = str(getattr(row, 'recent_reviews', ''))
            all_reviews = str(getattr(row, 'all_reviews', ''))
            release_date = str(getattr(row, 'release_date', ''))
            developer = str(getattr(row, 'developer', ''))
            publisher = str(getattr(row, 'publisher', ''))
            popular_tags = str(getattr(row, 'popular_tags', ''))
            game_details = str(getattr(row, 'game_details', ''))
            languages = str(getattr(row, 'languages', ''))
            
            # Handle achievements - convert to integer
            achievements_val = getattr(row, 'achievements', 0)
            achievements = int(achievements_val) if pd.notna(achievements_val) else 0
            
            genre = str(getattr(row, 'genre', ''))
            game_description = str(getattr(row, 'game_description', ''))
            mature_content = str(getattr(row, 'mature_content', ''))
            minimum_requirements = str(getattr(row, 'minimum_requirements', ''))
            recommended_requirements = str(getattr(row, 'recommended_requirements', ''))
            
            # Handle prices - clean and convert
            def clean_price(price_str):
                if pd.isna(price_str) or price_str == 'nan' or price_str == '':
                    return 0.0
                try:
                    # Remove currency symbols and convert to float
                    price_clean = str(price_str).replace('$', '').replace(',', '').strip()
                    return float(price_clean) if price_clean else 0.0
                except:
                    return 0.0
            
            original_price = clean_price(getattr(row, 'original_price', 0))
            discount_price = clean_price(getattr(row, 'discount_price', 0))
            
            # Calculate final price and discount percentage
            if discount_price > 0:
                final_price = discount_price
                discount_percentage = ((original_price - discount_price) / original_price * 100) if original_price > 0 else 0
            else:
                final_price = original_price
                discount_percentage = 0.0
            
            # Validate required fields
            if not url or url == 'nan' or not name or name == 'nan':
                return None
            
            # Clean 'nan' strings to None for database
            def clean_nan(val):
                return None if val == 'nan' or val == '' else val
                
            return (
                clean_nan(url),                    # url
                clean_nan(game_type),             # game_type
                clean_nan(name),                  # name
                clean_nan(desc_snippet),          # desc_snippet
                clean_nan(recent_reviews),        # recent_reviews
                clean_nan(all_reviews),           # all_reviews
                clean_nan(release_date),          # release_date
                clean_nan(developer),             # developer
                clean_nan(publisher),             # publisher
                clean_nan(popular_tags),          # popular_tags
                clean_nan(game_details),          # game_details
                clean_nan(languages),             # languages
                achievements,                      # achievements
                clean_nan(genre),                 # genre
                clean_nan(game_description),      # game_description
                clean_nan(mature_content),        # mature_content
                clean_nan(minimum_requirements),  # minimum_requirements
                clean_nan(recommended_requirements), # recommended_requirements
                original_price,                   # original_price
                discount_price if discount_price > 0 else None, # discount_price
                final_price,                      # final_price
                discount_percentage               # discount_percentage
            )
            
        except Exception as e:
            self.logger.warning(f"Error mapping row to schema: {e} - Row data: {getattr(row, 'name', 'Unknown')}")
            return None
    
    def get_load_statistics(self, db_path: str) -> dict:
        """
        Get statistics about the loaded data.
        
        Args:
            db_path (str): Path to the database
            
        Returns:
            dict: Dictionary containing load statistics
        """
        try:
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            
            # Get record count
            cursor.execute("SELECT COUNT(*) FROM rides")
            record_count = cursor.fetchone()[0]
            
            # Get latest ETL log
            cursor.execute('''
                SELECT * FROM etl_logs 
                WHERE process_name = 'DATA_LOAD' 
                ORDER BY created_at DESC 
                LIMIT 1
            ''')
            latest_log = cursor.fetchone()
            
            connection.close()
            
            return {
                'total_records': record_count,
                'latest_load_log': latest_log
            }
            
        except Exception as e:
            self.logger.error(f"Error getting load statistics: {e}")
            return {}
