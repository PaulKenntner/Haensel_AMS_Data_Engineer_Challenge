"""
Reporting module for the attribution pipeline.

This module provides functions for creating and exporting the final reporting table
with marketing performance metrics like CPO and ROAS.
"""

import pandas as pd
import sqlite3
from typing import Optional
import os

from config import logger

def create_channel_reporting(
    conn: sqlite3.Connection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> None:
    """
    Create or update the channel_reporting table with attribution data.
    
    This function joins data from session_sources, session_costs, attribution_customer_journey,
    and conversions tables to create a comprehensive reporting table.
    
    Args:
        conn: SQLite connection object
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Raises:
        sqlite3.Error: If database operations fail
    """
    try:
        # First, clear existing data if date filters are provided
        if start_date or end_date:
            clear_query = "DELETE FROM channel_reporting"
            if start_date:
                clear_query += f" WHERE date >= '{start_date}'"
                if end_date:
                    clear_query += f" AND date <= '{end_date}'"
            elif end_date:
                clear_query += f" WHERE date <= '{end_date}'"
            
            conn.execute(clear_query)
            logger.info(f"Cleared channel_reporting data for date range: {start_date or 'all'} to {end_date or 'all'}")
        else:
            # If no date filters, clear all data
            conn.execute("DELETE FROM channel_reporting")
            logger.info("Cleared all channel_reporting data")
        
        # Build the insert query with date filters if provided
        insert_query = """
        INSERT INTO channel_reporting (
            channel_name, 
            date, 
            cost, 
            ihc, 
            ihc_revenue
        )
        SELECT 
            ss.channel_name,
            ss.event_date as date,
            SUM(COALESCE(sc.cost, 0)) as cost,
            SUM(acj.ihc) as ihc,
            SUM(acj.ihc * c.revenue) as ihc_revenue
        FROM 
            session_sources ss
        LEFT JOIN 
            session_costs sc ON ss.session_id = sc.session_id
        JOIN 
            attribution_customer_journey acj ON ss.session_id = acj.session_id
        JOIN 
            conversions c ON acj.conv_id = c.conv_id
        """
        
        # Add date filters if provided
        where_clauses = []
        if start_date:
            where_clauses.append(f"ss.event_date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"ss.event_date <= '{end_date}'")
        
        if where_clauses:
            insert_query += " WHERE " + " AND ".join(where_clauses)
        
        # Group by channel and date
        insert_query += " GROUP BY ss.channel_name, ss.event_date"
        
        # Execute the query
        conn.execute(insert_query)
        conn.commit()
        
        # Get row count for logging
        count_query = "SELECT COUNT(*) FROM channel_reporting"
        row_count = conn.execute(count_query).fetchone()[0]
        
        logger.info(f"Created channel_reporting table with {row_count} rows")
        
    except sqlite3.Error as e:
        logger.error(f"Error creating channel_reporting table: {e}")
        conn.rollback()
        raise

def export_channel_reporting_with_metrics(
    conn: sqlite3.Connection,
    output_path: str = "channel_reporting.csv",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Export the channel_reporting table to a CSV file with CPO and ROAS metrics.
    
    This function:
    1. Retrieves data from the channel_reporting table
    2. Calculates CPO and ROAS metrics
    3. Exports the results to a CSV file
    
    Args:
        conn: SQLite connection object
        output_path: Path where the CSV file will be saved
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Returns:
        DataFrame containing the channel reporting data with metrics
        
    Raises:
        pd.io.sql.DatabaseError: If query fails
    """
    try:
        # Query the base data with date filters
        query = "SELECT * FROM channel_reporting"
        
        # Add date filters if provided
        where_clauses = []
        if start_date:
            where_clauses.append(f"date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"date <= '{end_date}'")
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Order by channel and date for better readability
        query += " ORDER BY channel_name, date"
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            logger.warning(f"No data found in channel_reporting table for date range: {start_date or 'all'} to {end_date or 'all'}")
            return df
        
        # Calculate CPO (Cost Per Order) = cost / ihc
        df['CPO'] = df['cost'] / df['ihc']
        
        # Calculate ROAS (Return On Ad Spend) = ihc_revenue / cost
        df['ROAS'] = df['ihc_revenue'] / df['cost']
        
        # Handle division by zero
        df.replace([float('inf'), -float('inf')], float('nan'), inplace=True)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        
        # Export to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Exported channel reporting with CPO and ROAS metrics to {output_path}")
        
        # Log summary statistics
        logger.info(f"Total cost: {df['cost'].sum():.2f}")
        logger.info(f"Total revenue: {df['ihc_revenue'].sum():.2f}")
        
        # Avoid division by zero in overall ROAS calculation
        total_cost = df['cost'].sum()
        if total_cost > 0:
            logger.info(f"Overall ROAS: {df['ihc_revenue'].sum() / total_cost:.2f}")
        else:
            logger.info("Overall ROAS: N/A (total cost is zero)")
        
        return df
        
    except pd.io.sql.DatabaseError as e:
        logger.error(f"Error calculating performance metrics: {e}")
        raise
    except ZeroDivisionError as e:
        logger.error(f"Division by zero encountered when calculating metrics: {e}")
        # Continue with NaN values
        return df
