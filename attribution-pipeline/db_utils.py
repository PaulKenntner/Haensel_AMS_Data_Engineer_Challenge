"""
Database utilities for the attribution pipeline.

This module provides functions for interacting with the SQLite database,
including connecting, querying data, and writing results.
"""

import sqlite3
import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from config import logger

def get_db_connection(db_path: str) -> sqlite3.Connection:
    """
    Create a connection to the SQLite database.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        A connection object to the database
        
    Raises:
        sqlite3.Error: If connection to the database fails
    """
    try:
        conn = sqlite3.connect(db_path)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        # Return rows as dictionaries
        conn.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def execute_sql_file(conn: sqlite3.Connection, sql_file_path: str) -> None:
    """
    Execute SQL statements from a file.
    
    Args:
        conn: SQLite connection object
        sql_file_path: Path to the SQL file
        
    Raises:
        FileNotFoundError: If the SQL file is not found
        sqlite3.Error: If execution of SQL statements fails
    """
    try:
        with open(sql_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
            conn.executescript(sql_script)
            conn.commit()
            logger.info(f"Executed SQL script from {sql_file_path}")
    except FileNotFoundError:
        logger.error(f"SQL file not found: {sql_file_path}")
        raise
    except sqlite3.Error as e:
        logger.error(f"Error executing SQL script: {e}")
        conn.rollback()
        raise

def get_conversions(
    conn: sqlite3.Connection, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Query conversions from the database, optionally filtered by date range.
    
    Args:
        conn: SQLite connection object
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Returns:
        DataFrame containing conversion data
    """
    query = "SELECT * FROM conversions"
    params = []
    
    # Add date filters if provided
    if start_date or end_date:
        query += " WHERE "
        if start_date:
            query += "conv_date >= ?"
            params.append(start_date)
        if start_date and end_date:
            query += " AND "
        if end_date:
            query += "conv_date <= ?"
            params.append(end_date)
    
    try:
        logger.info(f"Querying conversions with date range: {start_date} to {end_date}")
        return pd.read_sql_query(query, conn, params=params)
    except pd.io.sql.DatabaseError as e:
        logger.error(f"Error querying conversions: {e}")
        raise

def get_sessions(
    conn: sqlite3.Connection, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Query sessions from the database, optionally filtered by date range.
    
    Args:
        conn: SQLite connection object
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Returns:
        DataFrame containing session data
    """
    query = """
    SELECT ss.*, sc.cost 
    FROM session_sources ss
    LEFT JOIN session_costs sc ON ss.session_id = sc.session_id
    """
    params = []
    
    # Add date filters if provided
    if start_date or end_date:
        query += " WHERE "
        if start_date:
            query += "ss.event_date >= ?"
            params.append(start_date)
        if start_date and end_date:
            query += " AND "
        if end_date:
            query += "ss.event_date <= ?"
            params.append(end_date)
    
    try:
        logger.info(f"Querying sessions with date range: {start_date} to {end_date}")
        return pd.read_sql_query(query, conn, params=params)
    except pd.io.sql.DatabaseError as e:
        logger.error(f"Error querying sessions: {e}")
        raise

def get_sessions_for_user(
    conn: sqlite3.Connection, 
    user_ids: tuple,
    before_timestamp: Optional[str] = None
) -> pd.DataFrame:
    """
    Get all sessions for multiple users.
    
    Args:
        conn: SQLite connection object
        user_ids: Tuple of user identifiers
        before_timestamp: Optional timestamp in format 'YYYY-MM-DD HH:MM:SS'
        
    Returns:
        DataFrame containing session data for all users
    """
    query = """
    SELECT ss.*, sc.cost 
    FROM session_sources ss
    LEFT JOIN session_costs sc ON ss.session_id = sc.session_id
    WHERE ss.user_id IN ({})
    """.format(','.join(['?'] * len(user_ids)))
    
    params = list(user_ids)
    
    if before_timestamp:
        query += " AND datetime(ss.event_date || ' ' || ss.event_time) < datetime(?)"
        params.append(before_timestamp)
    
    query += " ORDER BY ss.event_date, ss.event_time"
    
    try:
        logger.info(f"Querying sessions for {len(user_ids)} users")
        return pd.read_sql_query(query, conn, params=params)
    except pd.io.sql.DatabaseError as e:
        logger.error(f"Error querying sessions for users: {e}")
        raise

def insert_attribution_results(conn: sqlite3.Connection, attribution_results: list) -> None:
    """
    Insert attribution results into the database.
    
    Args:
        conn: SQLite connection object
        attribution_results: List of attribution results from the API
    """
    if not attribution_results:
        logging.info("No attribution results to insert")
        return
    
    try:
        cursor = conn.cursor()
        
        # Use INSERT OR IGNORE to handle potential duplicates
        insert_query = """
        INSERT OR IGNORE INTO attribution_customer_journey (conv_id, session_id, ihc)
        VALUES (?, ?, ?)
        """
        
        # Format data according to the table structure
        data = [
            (
                result['conv_id'],
                result['session_id'],
                result['ihc']
            )
            for result in attribution_results
        ]
        
        # Execute the insert
        cursor.executemany(insert_query, data)
        conn.commit()
        
        # Log the number of rows actually inserted (affected)
        logging.info(f"Inserted {cursor.rowcount} attribution results")
    except Exception as e:
        logging.error(f"Error inserting attribution results: {e}")
        conn.rollback()
        raise

def check_attribution_sums(conn: sqlite3.Connection) -> bool:
    """
    Verify that attribution values sum to 1.0 for each conversion.
    
    Args:
        conn: SQLite connection object
        
    Returns:
        True if all conversions have attribution sums of 1.0, False otherwise
    """
    query = """
    SELECT conv_id, SUM(ihc) as total_ihc
    FROM attribution_customer_journey
    GROUP BY conv_id
    HAVING ABS(total_ihc - 1.0) > 0.001
    """
    
    try:
        cursor = conn.cursor()
        result = cursor.execute(query).fetchall()
        
        if result:
            logger.warning(f"Found {len(result)} conversions with attribution sums not equal to 1.0")
            for row in result:
                logger.warning(f"Conversion {row['conv_id']} has attribution sum of {row['total_ihc']}")
            return False
        else:
            logger.info("All conversions have attribution sums of 1.0")
            return True
    except sqlite3.Error as e:
        logger.error(f"Error checking attribution sums: {e}")
        raise

def check_attribution_exists(
    conn: sqlite3.Connection,
    conv_ids: List[str]
) -> Tuple[bool, List[str]]:
    """
    Check if attribution data already exists for the given conversion IDs.
    
    Args:
        conn: SQLite connection object
        conv_ids: List of conversion IDs to check
        
    Returns:
        Tuple of (all_exist_flag, missing_conv_ids)
    """
    if not conv_ids:
        return True, []
    
    # Convert list to tuple for SQL IN clause
    conv_ids_tuple = tuple(conv_ids)
    
    # Query to find which conversion IDs already have attribution data
    query = """
    SELECT DISTINCT conv_id 
    FROM attribution_customer_journey
    WHERE conv_id IN ({})
    """.format(','.join(['?'] * len(conv_ids_tuple)))
    
    try:
        cursor = conn.cursor()
        existing_ids = [row['conv_id'] for row in cursor.execute(query, conv_ids_tuple).fetchall()]
        
        # Find missing conversion IDs
        missing_ids = list(set(conv_ids) - set(existing_ids))
        
        if missing_ids:
            logger.info(f"Found {len(missing_ids)} conversions without attribution data")
            return False, missing_ids
        else:
            logger.info("All conversions already have attribution data")
            return True, []
    except sqlite3.Error as e:
        logger.error(f"Error checking attribution existence: {e}")
        raise