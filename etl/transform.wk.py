import pandas as pd
import base64

# Function to "encode" data (simulating encryption)
def encode_data(data):
    """Simulate encryption by encoding the data using base64."""
    if pd.isna(data):  # Skip NaN values
        print(f"Skipping encryption for NaN value: {data}")  # Debugging
        return data
    try:
        encoded_data = base64.b64encode(str(data).encode('utf-8'))
        return encoded_data.decode('utf-8')  # Return as a string
    except Exception as e:
        print(f"Error encoding data: {data} ({e})")  # Debugging
        return data

def clean_and_convert_column(df, column_name):
    """Helper function to clean and convert a column to numeric."""
    # Skip the conversion of encrypted values (already Base64 encoded)
    if 'encrypted' in df['data_type'].values:
        print(f"Skipping numeric conversion for '{column_name}' due to 'encrypted' data type.")
    else:
        # Convert to numeric, forcing errors to NaN, and display how many invalid entries are there.
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')

    # Count how many NaN values are introduced due to invalid values
    invalid_count = df[column_name].isna().sum()
    if invalid_count > 0:
        print(f"Warning: {invalid_count} invalid entries in column '{column_name}' have been converted to NaN.")
    
    return df

def transform_data(df):
    """Transform the data based on the given rules."""
    
    print(f"Initial DataFrame head:\n{df.head()}")  # Debugging: Check initial data

    # **Reshape data first** (before encryption) to ensure numeric columns for reshaping
    print(f"Rows to be reshaped:\n{df.head()}")

    # Check for enough data to reshape
    print(f"Unique regions before reshaping: {df['region'].nunique()}")  # Number of unique regions
    print(f"Missing values in 'region' column before reshaping: {df['region'].isna().sum()}")  # Check missing regions

    # Only reshape if there is sufficient data
    if df.shape[0] > 1 and df['region'].nunique() > 1:  # Ensure there's enough data left for reshaping
        # Optional: Reshape the data (example: pivoting or aggregating)
        try:
            # Now that the columns are numeric, we can safely compute the mean
            print(f"Attempting to reshape data with {df['region'].nunique()} unique regions.")
            df_reshaped = df.pivot_table(index=['region'], values=['monthly_rate', 'login_count'], aggfunc='mean')
            print(f"Reshaped DataFrame head:\n{df_reshaped.head()}")  # Check reshaped data
        except Exception as e:
            print(f"Error during reshaping: {e}")
            df_reshaped = pd.DataFrame()  # Create an empty DataFrame in case of error
    else:
        df_reshaped = pd.DataFrame()  # No data to reshape if we don't have enough rows

    # **Encrypt the data** after reshaping
    print(f"Encrypting the data...")

    # Encrypt the fields where 'data_type' is 'encrypted' (i.e., simulate encryption)
    df['monthly_rate'] = df.apply(
        lambda row: encode_data(row['monthly_rate']) if row['data_type'] == 'encrypted' else row['monthly_rate'], axis=1)
    
    df['login_count'] = df.apply(
        lambda row: encode_data(row['login_count']) if row['data_type'] == 'encrypted' else row['login_count'], axis=1)
    
    df['last_login_days'] = df.apply(
        lambda row: encode_data(row['last_login_days']) if row['data_type'] == 'encrypted' else row['last_login_days'], axis=1)

    # Debugging: Check data after encryption
    print(f"Data after encryption:\n{df.head()}")

    # **Save the data before cleaning** and converting
    df.to_csv('../data/transform_before_cleaning.csv', index=False)
    print(f"Data before cleaning and conversion saved to '../data/transform_before_cleaning.csv'")

    # **Clean and convert** the columns to numeric (but skip `encrypted` data)
    df = clean_and_convert_column(df, 'monthly_rate')
    df = clean_and_convert_column(df, 'login_count')
    df = clean_and_convert_column(df, 'last_login_days')

    print(f"Data after cleaning and conversion to numeric:\n{df.head()}")  # Check data after conversion

    # **Save the data after cleaning** but before dropping NaNs
    df.to_csv('../data/transform_after_cleaning.csv', index=False)
    print(f"Data after cleaning and conversion saved to '../data/transform_after_cleaning.csv'")

    # **Check how many rows are being dropped** when we remove NaNs
    print(f"Data before dropping NaNs: {df.shape[0]} rows.")
    
    # Remove rows where any of the numeric columns are NaN (check for missing data)
    df = df.dropna(subset=['monthly_rate', 'login_count', 'last_login_days'])

    print(f"Data after dropping NaNs:\n{df.head()}")  # Check data after removing NaNs
    print(f"Remaining data after dropping NaNs: {df.shape[0]} rows.")  # How many rows remain?

    # **Sample the data** (take a 50% sample for example)
    df_sampled = df.sample(frac=0.5, random_state=42)

    # Debugging: Print transformed data
    print(f"Transformed DataFrame head:\n{df.head()}")  # Check the transformation
    print(f"Sampled DataFrame head:\n{df_sampled.head()}")  # Check sampled data
    print(f"Reshaped DataFrame head:\n{df_reshaped.head()}")  # Check reshaped data

    # Save the DataFrames to CSV files
    df.to_csv('../data/transformed.csv', index=False)  # Save transformed data to 'transformed.csv'
    df_sampled.to_csv('../data/sampled_iam_policies.csv', index=False)  # Save sampled data
    df_reshaped.to_csv('../data/reshaped_iam_policies.csv', index=False)  # Save reshaped data

    # Confirmation message for saved files
    print("Data saved to '../data/transformed.csv', '../data/sampled_iam_policies.csv', and '../data/reshaped_iam_policies.csv'.")

    # Return transformed data, sampled data, and reshaped data
    return df, df_sampled, df_reshaped
