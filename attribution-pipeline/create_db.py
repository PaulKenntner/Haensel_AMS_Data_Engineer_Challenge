import sqlite3

# Connect to database (creates it if it doesn't exist)
with sqlite3.connect('challenge.db') as conn:
    # Read and execute the SQL file
    with open('challenge_db_create.sql', 'r') as sql_file:
        conn.executescript(sql_file.read())

        # Check if tables were created successfully
        cursor = conn.cursor()
        
        # Get list of all tables in database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Convert to list of table names
        table_names = [table[0] for table in tables]
        
        # Expected tables from schema
        expected_tables = [
            'conversions',
            'session_costs', 
            'session_sources',
            'attribution_customer_journey',
            'channel_reporting'
        ]
        
        # Check if all expected tables exist
        missing_tables = [table for table in expected_tables if table not in table_names]
        
        if missing_tables:
            print("Warning: The following tables are missing:")
            for table in missing_tables:
                print(f"- {table}")
        else:
            print("All required tables were created successfully!")
            
        # Print schema for verification
        for table in table_names:
            print(f"\nSchema for {table}:")
            cursor.execute(f"PRAGMA table_info({table});")
            schema = cursor.fetchall()
            for col in schema:
                print(f"- {col[1]} ({col[2]})")