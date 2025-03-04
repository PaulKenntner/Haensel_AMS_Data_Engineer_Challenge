"""
Customer Journey Builder for the attribution pipeline.

This module provides functions to construct customer journeys from session and conversion data,
formatted according to the requirements of the IHC Attribution API.
"""

import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Tuple
import sqlite3
from datetime import datetime
import numpy as np
from db_utils import get_sessions_for_user

from config import logger

def validate_timestamp(timestamp: str) -> bool:
    """Validate timestamp format for IHC API (YYYY-MM-DD HH:MM:SS)."""
    try:
        datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False

def build_customer_journeys(
    conn: sqlite3.Connection,
    conversions_df: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Build customer journeys for each conversion.
    
    For each conversion, find all sessions for the user that occurred before the conversion
    and format them according to the IHC API requirements.
    
    Args:
        conn: SQLite connection object
        conversions_df: DataFrame containing conversion data
        
    Returns:
        List of dictionaries representing customer journey sessions
    """
    all_journey_sessions = []
    
    # Track which session_ids have already been assigned to a conversion
    assigned_session_ids = set()
    
    # Sort conversions by date/time to ensure earlier conversions get priority
    conversions_df['conv_datetime'] = pd.to_datetime(
        conversions_df['conv_date'] + ' ' + conversions_df['conv_time']
    )
    conversions_df = conversions_df.sort_values('conv_datetime')
    
    # Process conversions in batches for better performance
    batch_size = 1000
    for batch_start in range(0, len(conversions_df), batch_size):
        batch_end = min(batch_start + batch_size, len(conversions_df))
        batch_conversions = conversions_df.iloc[batch_start:batch_end]
        
        # Get all relevant user IDs for this batch
        user_ids = tuple(batch_conversions['user_id'].unique())
        
        try:
            # Get all sessions for these users in one query
            sessions_df = get_sessions_for_user(conn, user_ids)
            
            if sessions_df.empty:
                continue
            
            # Process each conversion in the batch
            for _, conversion in batch_conversions.iterrows():
                conv_id = conversion['conv_id']
                user_id = conversion['user_id']
                conv_timestamp = f"{conversion['conv_date']} {conversion['conv_time']}"
                
                if not validate_timestamp(conv_timestamp):
                    logger.error(f"Invalid conversion timestamp format for {conv_id}")
                    continue
                
                # Filter sessions for this user that occurred before the conversion
                user_sessions = sessions_df[
                    (sessions_df['user_id'] == user_id) & 
                    (pd.to_datetime(sessions_df['event_date'] + ' ' + sessions_df['event_time']) < 
                     pd.to_datetime(conv_timestamp))
                ].copy()
                
                if user_sessions.empty:
                    logger.warning(f"No sessions found for user {user_id} before conversion {conv_id}")
                    continue
                
                # Filter out sessions that have already been assigned to another conversion
                unassigned_sessions = user_sessions[
                    ~user_sessions['session_id'].isin(assigned_session_ids)
                ]
                
                if unassigned_sessions.empty:
                    logger.warning(f"All sessions for user {user_id} have already been assigned to other conversions")
                    continue
                
                # Format sessions for this conversion
                journey_sessions = format_sessions_for_api(unassigned_sessions, conv_id, conv_timestamp)
                
                # Track which session_ids have been assigned
                for session in journey_sessions:
                    assigned_session_ids.add(session['session_id'])
                
                all_journey_sessions.extend(journey_sessions)
                
                logger.info(f"Built journey for conversion {conv_id} with {len(journey_sessions)} sessions")
                
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise
    
    return all_journey_sessions

def format_sessions_for_api(
    sessions_df: pd.DataFrame, 
    conv_id: str, 
    conv_timestamp: str
) -> List[Dict[str, Any]]:
    """Format sessions according to the IHC API requirements.
    
    Args:
        sessions_df: DataFrame containing session data
        conv_id: Conversion ID
        conv_timestamp: Conversion timestamp
        
    Returns:
        List of dictionaries representing formatted sessions
    """
    # Sort sessions chronologically
    sessions_df = sessions_df.sort_values(by=['event_date', 'event_time'])
    
    # Create a temporary column to identify the conversion session
    sessions_df = sessions_df.copy()  # Create a copy to avoid SettingWithCopyWarning
    sessions_df['_temp_conversion_flag'] = 0
    
    # Find the session closest to the conversion timestamp
    conversion_timestamp = datetime.strptime(conv_timestamp, '%Y-%m-%d %H:%M:%S')
    
    # Convert session timestamps to datetime for comparison
    sessions_df['_temp_datetime'] = sessions_df.apply(
        lambda row: datetime.strptime(f"{row['event_date']} {row['event_time']}", '%Y-%m-%d %H:%M:%S'), 
        axis=1
    )
    
    # Find the session closest to the conversion timestamp
    if not sessions_df.empty:
        # Get sessions before conversion
        valid_sessions = sessions_df[sessions_df['_temp_datetime'] <= conversion_timestamp]
        
        if not valid_sessions.empty:
            # Find the session closest to conversion time
            closest_idx = valid_sessions['_temp_datetime'].idxmax()
            sessions_df.loc[closest_idx, '_temp_conversion_flag'] = 1
    
    formatted_sessions = []
    
    for _, session in sessions_df.iterrows():
        session_timestamp = f"{session['event_date']} {session['event_time']}"
        
        if not validate_timestamp(session_timestamp):
            logger.warning(f"Skipping session {session['session_id']} due to invalid timestamp")
            continue
        
        # Format session for API
        formatted_session = {
            'conversion_id': conv_id,
            'session_id': session['session_id'],
            'timestamp': session_timestamp,
            'channel_label': session['channel_name'],
            'holder_engagement': int(session['holder_engagement']),
            'closer_engagement': int(session['closer_engagement']),
            'conversion': int(session['_temp_conversion_flag']),
            'impression_interaction': int(session['impression_interaction'])
        }
        
        formatted_sessions.append(formatted_session)
    
    return formatted_sessions

def chunk_journeys(
    journeys: List[Dict[str, Any]], 
    max_journeys_per_chunk: int = 100,
    max_sessions_per_chunk: int = 3000
) -> List[List[Dict[str, Any]]]:
    """
    Split customer journeys into chunks according to API limits.
    
    The IHC API has limits on:
    - Maximum number of customer journeys in a single request (100)
    - Maximum number of sessions in a single request (3000)
    
    This function splits the journeys into chunks that respect these limits.
    
    Args:
        journeys: List of dictionaries representing customer journey sessions
        max_journeys_per_chunk: Maximum number of journeys per chunk
        max_sessions_per_chunk: Maximum number of sessions per chunk
        
    Returns:
        List of chunks, where each chunk is a list of journey sessions
    """
    # Group sessions by conversion_id
    journey_groups = {}
    for session in journeys:
        conv_id = session['conversion_id']
        if conv_id not in journey_groups:
            journey_groups[conv_id] = []
        journey_groups[conv_id].append(session)
    
    # Create chunks
    chunks = []
    current_chunk = []
    current_chunk_journeys = 0
    current_chunk_sessions = 0
    
    for conv_id, sessions in journey_groups.items():
        # If adding this journey would exceed limits, start a new chunk
        if (current_chunk_journeys >= max_journeys_per_chunk or 
            current_chunk_sessions + len(sessions) > max_sessions_per_chunk):
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = []
            current_chunk_journeys = 0
            current_chunk_sessions = 0
        
        # Add this journey to the current chunk
        current_chunk.extend(sessions)
        current_chunk_journeys += 1
        current_chunk_sessions += len(sessions)
    
    # Add the last chunk if it's not empty
    if current_chunk:
        chunks.append(current_chunk)
    
    logger.info(f"Split {len(journey_groups)} journeys into {len(chunks)} chunks")
    return chunks

def validate_journey_data(journeys: List[Dict[str, Any]]) -> bool:
    """
    Validate customer journey data before sending to the API.
    
    Checks:
    - Required fields are present
    - Field types are correct
    - Engagement flags are 0 or 1
    
    Args:
        journeys: List of dictionaries representing customer journey sessions
        
    Returns:
        True if data is valid, False otherwise
    """
    required_fields = [
        'conversion_id', 'session_id', 'timestamp', 'channel_label',
        'holder_engagement', 'closer_engagement', 'conversion'
    ]
    
    for i, session in enumerate(journeys):
        # Check required fields
        for field in required_fields:
            if field not in session:
                logger.error(f"Session {i} missing required field: {field}")
                return False
        
        # Check field types
        if not isinstance(session['conversion_id'], str):
            logger.error(f"Session {i}: conversion_id must be a string")
            return False
        if not isinstance(session['session_id'], str):
            logger.error(f"Session {i}: session_id must be a string")
            return False
        if not isinstance(session['timestamp'], str):
            logger.error(f"Session {i}: timestamp must be a string")
            return False
        if not isinstance(session['channel_label'], str):
            logger.error(f"Session {i}: channel_label must be a string")
            return False
        
        # Check engagement flags
        for flag in ['holder_engagement', 'closer_engagement', 'conversion', 'impression_interaction']:
            if flag in session and session[flag] not in [0, 1]:
                logger.error(f"Session {i}: {flag} must be 0 or 1")
                return False
    
    # Check if there's at least one conversion session per journey
    conversion_ids = set(session['conversion_id'] for session in journeys)
    for conv_id in conversion_ids:
        conv_sessions = [s for s in journeys if s['conversion_id'] == conv_id]
        if not any(s['conversion'] == 1 for s in conv_sessions):
            logger.error(f"Journey {conv_id} has no conversion session")
            return False
    
    return True

def get_conversion_counts(journeys: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Count the number of sessions for each conversion.
    
    Args:
        journeys: List of dictionaries representing customer journey sessions
        
    Returns:
        Dictionary mapping conversion_id to session count
    """
    counts = {}
    for session in journeys:
        conv_id = session['conversion_id']
        if conv_id not in counts:
            counts[conv_id] = 0
        counts[conv_id] += 1
    
    return counts

def get_journey_statistics(journeys: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate statistics about the customer journeys.
    
    Args:
        journeys: List of dictionaries representing customer journey sessions
        
    Returns:
        Dictionary with statistics
    """
    conversion_counts = get_conversion_counts(journeys)
    
    stats = {
        'total_journeys': len(conversion_counts),
        'total_sessions': len(journeys),
        'avg_sessions_per_journey': len(journeys) / len(conversion_counts) if conversion_counts else 0,
        'min_sessions': min(conversion_counts.values()) if conversion_counts else 0,
        'max_sessions': max(conversion_counts.values()) if conversion_counts else 0
    }
    
    return stats