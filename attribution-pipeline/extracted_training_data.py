import sqlite3
import pandas as pd
import json
import random

# Connect to the database
conn = sqlite3.connect('challenge.db')

# Get a sample of conversions
query = """
SELECT * FROM conversions
LIMIT 20
"""
conversions_df = pd.read_sql_query(query, conn)

# Initialize list to store all journeys
training_journeys = []

# For each conversion, get the user's sessions that occurred before the conversion
for _, conversion in conversions_df.iterrows():
    conv_id = conversion['conv_id']
    user_id = conversion['user_id']
    conv_timestamp = f"{conversion['conv_date']} {conversion['conv_time']}"
    
    # Get sessions for this user before the conversion
    session_query = f"""
    SELECT ss.*
    FROM session_sources ss
    WHERE ss.user_id = '{user_id}'
    AND datetime(ss.event_date || ' ' || ss.event_time) < datetime('{conv_timestamp}')
    ORDER BY ss.event_date, ss.event_time
    """
    
    sessions_df = pd.read_sql_query(session_query, conn)
    
    # Skip if no sessions found
    if sessions_df.empty:
        continue
    
    # Mark the last session as the conversion session
    sessions_df['is_conversion_session'] = 0
    sessions_df.iloc[-1, sessions_df.columns.get_loc('is_conversion_session')] = 1
    
    # Format sessions for this journey
    journey_sessions = []
    for _, session in sessions_df.iterrows():
        session_timestamp = f"{session['event_date']} {session['event_time']}"
        
        # Calculate engagement flags based on session data
        # For training data, we'll use the existing flags from the database
        holder_engagement = session['holder_engagement']
        closer_engagement = session['closer_engagement']
        
        formatted_session = {
            'conversion_id': conv_id,
            'session_id': session['session_id'],
            'timestamp': session_timestamp,
            'channel_label': session['channel_name'],
            'holder_engagement': int(holder_engagement),
            'closer_engagement': int(closer_engagement),
            'conversion': int(session['is_conversion_session']),
            'impression_interaction': int(session['impression_interaction'])
        }
        
        journey_sessions.append(formatted_session)
    
    # Add all sessions for this journey to the training data
    training_journeys.extend(journey_sessions)

# Save the training data to a JSON file
with open('ihc_training_data.json', 'w') as f:
    json.dump(training_journeys, f, indent=2)

print(f"Created training data with {len(training_journeys)} sessions across {len(conversions_df)} journeys")

# Close the connection
conn.close()
