from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd

# --- Importing your custom functions ---
from etl.extract import extract_data  # Assuming you have a function in extract.py
from etl.transform import transform_data  # Assuming you have a function in transform.py
from etl.load import load_data_to_postgres  # Assuming you have a function in load.py

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'etl',  # The name of the DAG
    default_args=default_args,
    description='ETL Pipeline for Supabase',
    schedule_interval='* * * * *',  # Run every minute
    catchup=False,
    start_date=days_ago(1),
) as dag:

    # Step 1: Extract data (Assuming extract.py has a function called `extract_data`)
    def extract():
        try:
            # Extract the data using the function from `extract.py`
            df = extract_data("iam_policies.csv")  # Replace with your actual extract logic
            return df
        except Exception as e:
            print(f"Error in extraction: {e}")
            raise

    extract_task = PythonOperator(
        task_id='extract_data',  # Task ID
        python_callable=extract,  # Function to run
        dag=dag,
    )

    # Step 2: Transform data (Assuming transform.py has a function called `transform_data`)
    def transform(df):
        try:
            # Transform the data using the function from `transform.py`
            df_transformed, df_sampled, df_reshaped = transform_data(df)
            return df_transformed, df_sampled, df_reshaped
        except Exception as e:
            print(f"Error in transformation: {e}")
            raise

    transform_task = PythonOperator(
        task_id='transform_data',  # Task ID
        python_callable=transform,  # Function to run
        op_args=[extract_task.output],  # Pass the output of the extract task to transform
        dag=dag,
    )

    # Step 3: Load data (Assuming load.py has a function called `load_data_to_postgres`)
    def load(df_transformed):
        try:
            # Load the transformed data using the function from `load.py`
            load_data_to_postgres(df_transformed, 'iam_policies')  # Example table name
        except Exception as e:
            print(f"Error in loading data: {e}")
            raise

    load_task = PythonOperator(
        task_id='load_data',  # Task ID
        python_callable=load,  # Function to run
        op_args=[transform_task.output],  # Pass the output of the transform task to load
        dag=dag,
    )

    # Step 4: Define the task dependencies (this ensures the tasks run in the correct order)
    extract_task >> transform_task >> load_task  # Extraction happens first, then transformation, and finally loading.
