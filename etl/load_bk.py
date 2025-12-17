import psycopg2
import pandas as pd
from psycopg2 import sql

# --- Step 1: Connect to Supabase PostgreSQL ---
def connect_to_supabase():
    try:
        # Database connection settings (replace with your details)
        conn = psycopg2.connect(
            host="db.prwzydmfrcbepgevmqmu.supabase.co",  # Supabase host
            port=5432,  # Default PostgreSQL port
            database="postgres",  # Database name
            user="etl_user1",  # Database username
            password="cyuqD639?TT",  # Database password
            sslmode="require"  # SSL connection for security
        )
        print("Supabase PostgreSQL Connection established successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


# --- Step 2: Load Transformed Data into Supabase Table ---
def load_data_to_supabase(df):
    # Establish connection to Supabase
    conn = connect_to_supabase()
    if conn is None:
        return  # Exit if connection failed

    # Create a cursor object
    cur = conn.cursor()

    # Prepare the SQL insert query (use actual column names from your Supabase table)
    insert_query = sql.SQL("""
        INSERT INTO iam_policies 
        (policy_id, user_id, role, plan_type, monthly_rate, premium, region, login_count, last_login_days)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (policy_id) 
        DO UPDATE SET 
            user_id = EXCLUDED.user_id,
            role = EXCLUDED.role,
            plan_type = EXCLUDED.plan_type,
            monthly_rate = EXCLUDED.monthly_rate,
            premium = EXCLUDED.premium,
            region = EXCLUDED.region,
            login_count = EXCLUDED.login_count,
            last_login_days = EXCLUDED.last_login_days
        WHERE iam_policies.user_id IS DISTINCT FROM EXCLUDED.user_id
          OR iam_policies.role IS DISTINCT FROM EXCLUDED.role
          OR iam_policies.plan_type IS DISTINCT FROM EXCLUDED.plan_type
          OR iam_policies.monthly_rate IS DISTINCT FROM EXCLUDED.monthly_rate
          OR iam_policies.premium IS DISTINCT FROM EXCLUDED.premium
          OR iam_policies.region IS DISTINCT FROM EXCLUDED.region
          OR iam_policies.login_count IS DISTINCT FROM EXCLUDED.login_count
          OR iam_policies.last_login_days IS DISTINCT FROM EXCLUDED.last_login_days;
    """)

    # Iterate through the DataFrame and insert each row into the database
    for _, row in df.iterrows():
        try:
            # Create a tuple from the DataFrame row to match the SQL query placeholders
            values = (
                row['Policy_ID'],  # policy_id
                row['User_ID'],    # user_id
                row['Role'],       # role
                row['Plan_Type'],  # plan_type
                row['Monthly_Rate'],  # monthly_rate (int)
                row['Is_Premium'],  # premium (boolean)
                row['Region'],     # region
                row['Login_Count'],  # login_count (int)
                row['Last_Login_Days']  # last_login_days (int)
            )
            
            # Execute the insert query with the values
            cur.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting row {row['Policy_ID']}: {e}")

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()
    print("Data loaded successfully to Supabase.")


# --- Main Execution ---
if __name__ == "__main__":
    try:
        # Read the transformed data from CSV (ensure it's in the correct path)
        df_transformed = pd.read_csv("iam_policies_transform.csv")

        # Check if DataFrame is empty
        if df_transformed.empty:
            raise ValueError("The transformed DataFrame is empty, no data to load.")

        # Load the data into Supabase
        load_data_to_supabase(df_transformed)

    except Exception as e:
        print(f"Error during the data loading process: {e}")
