import psycopg2
import pandas as pd
from psycopg2 import sql
import os

# Connect to Supabase PostgreSQL database
def connect_to_supabase():
    try:
        # Use your Supabase database connection details
        conn = psycopg2.connect(
            host="db.prwzydmfrcbepgevmqmu.supabase.co",  # Supabase host
            port=5432,  # Default PostgreSQL port
            database="postgres",  # Database name
            user="etl_user1",  # Database username
            password="cyuqD639?TT",  # Database password
            sslmode="require"  # SSL connection for security
        )
        print("Successfully connected to Supabase PostgreSQL.")
        return conn
    except Exception as e:
        print(f"Error: Unable to connect to database - {e}")
        return None

# Function to load data to Supabase
def load_data_to_supabase(df):
    conn = connect_to_supabase()
    
    if conn is None:
        return
    
    try:
        # Create a cursor object
        cur = conn.cursor()

        # The table in Supabase should have the following columns based on your DataFrame
        insert_query = sql.SQL("""
            INSERT INTO iam_policies 
            (policy_id, user_id, role, plan_type, monthly_rate, premium)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (policy_id) DO NOTHING;
        """)

        # Print the DataFrame to see the columns and structure
        print(f"Data to be loaded:\n{df.head()}")  # Debugging: Print the first few rows of the DataFrame

        # Ensure that 'monthly_rate' is numeric and handle NaN values by converting them to None (NULL in DB)
        df['monthly_rate'] = pd.to_numeric(df['monthly_rate'], errors='coerce')  # Convert to numeric, invalid values become NaN
        df['monthly_rate'] = df['monthly_rate'].where(df['monthly_rate'].notna(), None)  # Replace NaN with None

        # Loop through each row in the DataFrame and insert it into the PostgreSQL table
        for _, row in df.iterrows():
            # Ensure the values tuple is packed with the correct number of elements (6 values in total)
            values = (
                row['policy_id'],  # policy_id
                row['user_id'],    # user_id
                row['role'],       # role
                row['plan_type'],  # plan_type
                row['monthly_rate'],  # monthly_rate (now guaranteed to be numeric or None)
                row['premium'],    # premium (True/False)
            )

            # Print the values being inserted to debug
            print(f"Inserting values: {values}")  # Debugging: Print the tuple to see if it's correctly formatted
            
            # Execute the insert query with the values
            cur.execute(insert_query, values)

        # Commit the transaction
        conn.commit()
        print(f"Data loaded successfully into the Supabase database.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

# Main function for testing or execution
def main():
    # Load the transformed CSV to DataFrame (replace with your file path)
    df = pd.read_csv('../data/transformed.csv')

    # Ensure the DataFrame has the right columns that match the table schema
    # Columns must be: policy_id, user_id, role, plan_type, monthly_rate, premium
    df = df[['policy_id', 'user_id', 'role', 'plan_type', 'monthly_rate', 'premium']]

    load_data_to_supabase(df)

if __name__ == "__main__":
    main()
