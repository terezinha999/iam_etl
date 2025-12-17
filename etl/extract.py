import pandas as pd
import numpy as np
from sklearn.datasets import make_classification

# Function to generate synthetic data
def extract_data():
    # Set the random seed for reproducibility
    np.random.seed(42)

    # Number of rows to generate
    n_samples = 10

    # Manually generating the data based on the structure you want
    policy_ids = [f'P100{i+1}' for i in range(n_samples)]
    user_ids = [f'U00{i+1}' for i in range(n_samples)]
    roles_data = np.random.choice(['Admin', 'User', 'Manager'], size=n_samples)
    plan_types_data = np.random.choice(['Enterprise', 'Standard', 'Basic'], size=n_samples)
    monthly_rates = np.random.choice([50, 120, 320, 520], size=n_samples)
    premium_values_data = np.random.choice(['Yes', 'No'], size=n_samples)
    regions_data = np.random.choice(['US', 'EU', 'APAC'], size=n_samples)
    login_counts = np.random.randint(20, 800, size=n_samples)
    last_login_days = np.random.randint(1, 400, size=n_samples)

    # Dynamically generate data_type with both 'encrypted' and 'non-encrypted'
    data_types = np.random.choice(['encrypted', 'non-encrypted'], size=n_samples, p=[0.3, 0.7])  # 30% encrypted, 70% non-encrypted

    # Create the DataFrame
    df = pd.DataFrame({
        'policy_id': policy_ids,
        'user_id': user_ids,
        'role': roles_data,
        'plan_type': plan_types_data,
        'monthly_rate': monthly_rates,
        'premium': premium_values_data,
        'region': regions_data,
        'login_count': login_counts,
        'last_login_days': last_login_days,
        'data_type': data_types
    })

    # Ensure the 'premium' column is boolean (True/False)
    df['premium'] = df['premium'].map({'Yes': True, 'No': False})

    # Save the generated data to a CSV file
    output_file = '../data/iam_policies.csv'
    df.to_csv(output_file, index=False)

    # Print a confirmation message to indicate the file has been saved
    print(f"Synthetic data has been saved to {output_file}")

    # Return the DataFrame for further processing
    return df
