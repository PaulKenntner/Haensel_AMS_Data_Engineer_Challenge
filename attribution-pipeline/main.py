"""
Main pipeline orchestration for the attribution pipeline.

This module serves as the entry point for the attribution pipeline, orchestrating
the execution of each step in sequence and handling command-line arguments.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional, Tuple

# Import utility modules
import db_utils
import journey_builder
import api_utils
import reporting
from config import DB_PATH, REPORT_OUTPUT_PATH, logger


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Namespace containing the parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run the attribution pipeline")
    
    parser.add_argument(
        "--db_path",
        type=str,
        default=DB_PATH,
        help="Path to the SQLite database file"
    )
    
    parser.add_argument(
        "--sql_file",
        type=str,
        default="challenge_db_create.sql",
        help="Path to the SQL file for creating tables"
    )
    
    parser.add_argument(
        "--start_date",
        type=str,
        help="Start date for filtering data (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end_date",
        type=str,
        help="End date for filtering data (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--output_path",
        type=str,
        default=REPORT_OUTPUT_PATH,
        help="Path where the final report will be saved"
    )
    
    parser.add_argument(
        "--rate_limit_delay",
        type=float,
        default=1.0,
        help="Delay between API requests in seconds"
    )
    
    return parser.parse_args()

def validate_dates(start_date: Optional[str], end_date: Optional[str]) -> bool:
    """
    Validate date format and range.
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        
    Returns:
        True if dates are valid, False otherwise
    """
    date_format = "%Y-%m-%d"
    
    # Validate start_date
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, date_format)
        except ValueError:
            logger.error(f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD")
            return False
    
    # Validate end_date
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, date_format)
        except ValueError:
            logger.error(f"Invalid end_date format: {end_date}. Expected YYYY-MM-DD")
            return False
    
    # Validate date range
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, date_format)
        end_dt = datetime.strptime(end_date, date_format)
        
        if start_dt > end_dt:
            logger.error(f"start_date ({start_date}) is after end_date ({end_date})")
            return False
    
    return True

def setup_database(db_path: str, sql_file: str) -> Optional[db_utils.sqlite3.Connection]:
    """
    Set up the database connection and create required tables.
    
    Args:
        db_path: Path to the SQLite database file
        sql_file: Path to the SQL file for creating tables
        
    Returns:
        SQLite connection object or None if setup fails
    """
    try:
        # Connect to the database
        conn = db_utils.get_db_connection(db_path)
        
        # Create required tables
        db_utils.execute_sql_file(conn, sql_file)
        
        return conn
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return None

def process_conversions(
    conn: db_utils.sqlite3.Connection,
    start_date: Optional[str],
    end_date: Optional[str]
) -> Tuple[bool, list, list]:
    """
    Process conversions and build customer journeys.
    
    Args:
        conn: SQLite connection object
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        
    Returns:
        Tuple of (success_flag, journey_chunks, conv_ids)
    """
    try:
        # Get conversions
        logger.info("Fetching conversions from database")
        conversions_df = db_utils.get_conversions(conn, start_date, end_date)
        
        if conversions_df.empty:
            logger.warning("No conversions found for the specified date range")
            return False, [], []
        
        logger.info(f"Found {len(conversions_df)} conversions")
        
        # Build customer journeys
        logger.info("Building customer journeys")
        journeys = journey_builder.build_customer_journeys(conn, conversions_df)
        
        if not journeys:
            logger.warning("No journeys could be built")
            return False, [], []
        
        # Get journey statistics
        stats = journey_builder.get_journey_statistics(journeys)
        logger.info(f"Journey statistics: {stats}")
        
        # Validate journey data
        if not journey_builder.validate_journey_data(journeys):
            logger.error("Journey validation failed")
            return False, [], []
        
        # Chunk journeys for API
        logger.info("Chunking journeys for API")
        journey_chunks = journey_builder.chunk_journeys(journeys)
        logger.info(f"Created {len(journey_chunks)} journey chunks")
        
        # Get conversion IDs
        conv_ids = conversions_df['conv_id'].tolist()
        
        return True, journey_chunks, conv_ids
    except Exception as e:
        logger.error(f"Error processing conversions: {e}")
        return False, [], []

def process_attribution(
    journey_chunks: list,
    rate_limit_delay: float
) -> Tuple[bool, list]:
    """
    Process attribution by sending journeys to the API.
    
    Args:
        journey_chunks: List of journey chunks
        rate_limit_delay: Delay between API requests in seconds
        
    Returns:
        Tuple of (success_flag, attribution_results)
    """
    try:
        # Get API credentials
        logger.info("Getting API credentials")
        try:
            api_key, conv_type_id = api_utils.get_api_credentials_from_env()
        except ValueError as e:
            logger.error(f"API credentials error: {e}")
            return False, []
        
        # Initialize API client
        logger.info("Initializing API client")
        api_client = api_utils.IHCApiClient(api_key, conv_type_id)
        
        # Send journeys to API
        logger.info("Sending journeys to API")
        attribution_results = api_utils.send_journeys_to_api(
            api_client,
            journey_chunks,
            rate_limit_delay
        )
        
        if not attribution_results:
            logger.warning("No attribution results returned from API")
            return False, []
        
        # Validate attribution results
        if not api_utils.validate_api_results(attribution_results):
            logger.error("Attribution results validation failed")
            return False, []
        
        logger.info(f"Received {len(attribution_results)} valid attribution results")
        return True, attribution_results
    except Exception as e:
        logger.error(f"Error processing attribution: {e}")
        return False, []

def store_results_and_report(
    conn: db_utils.sqlite3.Connection,
    attribution_results: list,
    start_date: Optional[str],
    end_date: Optional[str],
    output_path: str
) -> bool:
    """
    Store attribution results and generate the final report.
    
    Args:
        conn: SQLite connection object
        attribution_results: List of attribution results (may be empty)
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        output_path: Path where the final report will be saved
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Insert attribution results if any
        if attribution_results:
            logger.info(f"Inserting {len(attribution_results)} attribution results into database")
            db_utils.insert_attribution_results(conn, attribution_results)
        
        # Check attribution sums
        logger.info("Checking attribution sums")
        if not db_utils.check_attribution_sums(conn):
            logger.warning("Attribution sums validation failed")
            # Continue anyway, as this might be a partial update
        
        # Create channel reporting table
        logger.info("Creating channel reporting table")
        reporting.create_channel_reporting(conn, start_date, end_date)
        
        # Export channel reporting with metrics - pass date parameters
        logger.info("Exporting channel reporting with metrics")
        df = reporting.export_channel_reporting_with_metrics(
            conn, 
            output_path,
            start_date,
            end_date
        )
        
        if df.empty:
            logger.warning("No data in final report")
            return False
        
        logger.info(f"Pipeline completed successfully. Report saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error storing results and generating report: {e}")
        return False

def run_pipeline(args: argparse.Namespace) -> bool:
    """
    Run the complete attribution pipeline.
    
    Args:
        args: Command-line arguments
        
    Returns:
        True if pipeline completed successfully, False otherwise
    """
    logger.info("Starting attribution pipeline")
    logger.info(f"Parameters: start_date={args.start_date}, end_date={args.end_date}")
    
    # Validate dates
    if not validate_dates(args.start_date, args.end_date):
        return False
    
    # Setup database
    conn = setup_database(args.db_path, args.sql_file)
    if not conn:
        return False
    
    try:
        # Process conversions and build journeys
        success, journey_chunks, conv_ids = process_conversions(conn, args.start_date, args.end_date)
        if not success:
            logger.error("Conversion processing failed")
            return False
        
        # Initialize attribution_results
        attribution_results = []
        
        # Check if attribution data already exists
        all_exist, missing_conv_ids = db_utils.check_attribution_exists(conn, conv_ids)
        if all_exist:
            logger.info("Skipping API call - attribution data already exists")
        else:
            logger.info(f"Found {len(missing_conv_ids)} conversions without attribution data")
            
            # Filter journey chunks to only include missing conversions
            filtered_journey_chunks = []
            for chunk in journey_chunks:
                filtered_chunk = [
                    session for session in chunk 
                    if session['conversion_id'] in missing_conv_ids
                ]
                if filtered_chunk:  # Only add non-empty chunks
                    filtered_journey_chunks.append(filtered_chunk)
            
            if not filtered_journey_chunks:
                logger.warning("No journeys to process after filtering")
            else:
                # Process attribution only for missing conversions
                logger.info(f"Sending {len(filtered_journey_chunks)} filtered chunks to API")
                success, attribution_results = process_attribution(filtered_journey_chunks, args.rate_limit_delay)
                if not success:
                    logger.error("Attribution processing failed")
                    return False
        
        # Store results and generate report
        success = store_results_and_report(
            conn,
            attribution_results,  # This may be empty if all conversions already have data
            args.start_date,
            args.end_date,
            args.output_path
        )
        if not success:
            logger.error("Results storage and reporting failed")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        return False
    finally:
        # Close database connection
        if conn:
            conn.close()
            logger.info("Database connection closed")

def main():
    """Main entry point for the attribution pipeline."""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Run the pipeline
    success = run_pipeline(args)
    
    # Exit with appropriate status code
    if success:
        logger.info("Pipeline completed successfully")
        sys.exit(0)
    else:
        logger.error("Pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
