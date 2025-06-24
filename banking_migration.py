#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Banking Data Migration Tool
Author: Your Name
Date: 2023-10-15
Description: Migrates banking data from CSV to SQLite with data validation
Dependencies: pandas (install via `pip install pandas`)
"""

import pandas as pd
import sqlite3
import logging
import argparse
from pathlib import Path
from datetime import datetime


# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("migration_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("BankingDataMigration")


# Database schema definition
SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_date DATE NOT NULL,
    account_number TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    description TEXT,
    category TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_account ON transactions (account_number);
CREATE INDEX IF NOT EXISTS idx_date ON transactions (transaction_date);
"""

class DatabaseManager:
    def __init__(self, db_path="banking.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.cursor = self.conn.cursor()
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.commit()
            self.conn.close()
            
    def initialize_db(self):
        self.cursor.executescript(SCHEMA)
        logger.info("Database schema initialized")
        
    def insert_data(self, df):
        df.to_sql(
            "transactions", 
            self.conn, 
            if_exists="append", 
            index=False,
            dtype={
                "transaction_date": "DATE",
                "account_number": "TEXT",
                "transaction_type": "TEXT",
                "amount": "REAL",
                "currency": "TEXT",
                "description": "TEXT",
                "category": "TEXT"
            }
        )
        logger.info(f"Inserted {len(df)} records into database")

def validate_data(df):
    """Perform data validation and cleaning"""
    # Validate required columns
    required_cols = [
        "transaction_date", 
        "account_number", 
        "transaction_type", 
        "amount", 
        "currency"
    ]
    
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Convert date column
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], 
        errors="coerce"
    )
    
    # Handle missing dates
    date_missing = df["transaction_date"].isna()
    if date_missing.any():
        logger.warning(f"Found {date_missing.sum()} records with invalid dates")
        df = df[~date_missing]
    
    # Clean account numbers
    df["account_number"] = (
        df["account_number"]
        .astype(str)
        .str.replace(r'\D', '', regex=True)
    )
    
    # Validate transaction types
    valid_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "FEE"]
    invalid_types = ~df["transaction_type"].isin(valid_types)
    if invalid_types.any():
        logger.warning(f"Found {invalid_types.sum()} invalid transaction types")
        df = df[~invalid_types]
    
    # Convert amount to numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])
    
    logger.info(f"Data validation complete. {len(df)} valid records remaining")
    return df

def migrate_data(input_file, db_path="banking.db", batch_size=1000):
    """Main migration function with batch processing"""
    logger.info(f"Starting migration for: {input_file}")
    
    # Read input data
    try:
        df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(df)} records from {input_file}")
    except Exception as e:
        logger.error(f"Error reading input file: {str(e)}")
        return False
    
    # Validate and clean data
    try:
        df = validate_data(df)
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return False
    
    # Process in batches for memory efficiency
    total_rows = len(df)
    processed = 0
    
    with DatabaseManager(db_path) as db:
        db.initialize_db()
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            try:
                db.insert_data(batch)
                processed += len(batch)
                logger.info(f"Progress: {processed}/{total_rows} records migrated")
            except Exception as e:
                logger.error(f"Batch migration failed: {str(e)}")
    
    logger.info(f"Migration completed successfully. {processed}/{total_rows} records migrated")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Banking Data Migration Tool",
        epilog="Note: CSV must contain columns: transaction_date, account_number, transaction_type, amount, currency"
    )
    parser.add_argument("input", help="Path to input CSV file")
    parser.add_argument("--db", default="banking.db", help="SQLite database path")
    parser.add_argument("--batch", type=int, default=1000, help="Batch size for processing")
    
    args = parser.parse_args()
    
    if not Path(args.input).exists():
        logger.error(f"Input file not found: {args.input}")
        exit(1)
    
    start_time = datetime.now()
    logger.info(f"Migration started at {start_time}")
    
    success = migrate_data(
        input_file=args.input,
        db_path=args.db,
        batch_size=args.batch
    )
    
    duration = datetime.now() - start_time
    if success:
        logger.info(f"Migration completed in {duration}")
    else:
        logger.error(f"Migration failed after {duration}")
        exit(1)
