import pandas as pd

# --- Step 1: Extract data from CSV ---
def extract_data(csv_file):
    df = pd.read_csv(csv_file)
    return df

# Example usage
if __name__ == "__main__":
    df = extract_data("../data/iam_policies.csv")
    print(df.head())  # Confirm the data has been loaded correctly
