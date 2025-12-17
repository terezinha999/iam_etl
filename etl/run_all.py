import argparse
import importlib
import numpy as np
import sys
import os
import pandas as pd  # <-- Add this import for pandas

def run_extract():
    try:
        print("Running Extract Step...")
        # Dynamically import and run the extract function from extract.py
        extract = importlib.import_module('extract')  # assuming extract.py is in the same folder
        df = extract.extract_data()  # No need to pass CSV file path since it's auto-generated
        return df
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None

def run_transform(df):
    try:
        print("Running Transform Step...")
        # Dynamically import and run the transform function from transform.py
        transform = importlib.import_module('transform')  # assuming transform.py is in the same folder
        df_transformed, df_sampled, df_reshaped = transform.transform_data(df)
        return df_transformed, df_sampled, df_reshaped
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None, None, None

def run_load(df_transformed):
    try:
        print("Running Load Step...")
        # Dynamically import and run the load function from load.py
        load = importlib.import_module('load')  # assuming load.py is in the same folder
        load.load_data_to_supabase(df_transformed)  # assuming load_data is the function that loads data
    except Exception as e:
        print(f"Error during loading: {e}")

def main():
    # Set up argument parsing to allow dynamic execution
    parser = argparse.ArgumentParser(description="Run the ETL pipeline steps")
    parser.add_argument('--extract', action='store_true', help='Run the extract step only')
    parser.add_argument('--transform', action='store_true', help='Run the transform step after extract')
    parser.add_argument('--load', action='store_true', help='Run the load step after transform')
    parser.add_argument('--all', action='store_true', help='Run all steps: extract, transform, and load')

    args = parser.parse_args()

    # Set the random seed for reproducibility
    np.random.seed(42)

    # Handle the options
    if args.all or args.extract:
        # Run the extract step which will auto-generate the CSV file
        df = run_extract()
        if df is None:
            print("Extraction failed. Exiting...")
            sys.exit(1)
    
    if args.all or args.transform:
        if 'df' not in locals():
            print("Please run extract first, as transformation depends on extraction.")
            sys.exit(1)
        df_transformed, df_sampled, df_reshaped = run_transform(df)
        if df_transformed is None:
            print("Transformation failed. Exiting...")
            sys.exit(1)

    if args.all or args.load:
        # Check if transformed.csv exists before loading
        if not os.path.exists('../data/transformed.csv'):
            print("Error: Transformed data not found. Please run the full pipeline (extract + transform) before loading.")
            sys.exit(1)
        else:
            # Load the transformed data directly from the CSV
            df_transformed = pd.read_csv('../data/transformed.csv')
            print(f"Loaded transformed data from 'transformed.csv'.")
            run_load(df_transformed)

    print("Pipeline execution completed.")

if __name__ == "__main__":
    main()
