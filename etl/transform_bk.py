import pandas as pd
import numpy as np

# --- Step 1: Transform the data ---
def transform_data(df):
    try:
        # Duplicate the dataframe to avoid 'SettingWithCopyWarning'
        df = df.copy()

        # 1. Remove duplicates based on 'policy_id'
        df = df.drop_duplicates(subset=["policy_id"])
        print("After removing duplicates:")
        print(df.head())  # Print the first few rows after duplicates are removed

        # 2. Mapping 'Yes'/'No' to boolean values for 'premium' column
        if 'premium' not in df.columns:
            raise KeyError("Column 'premium' is missing from the dataframe.")
        
        df['premium'] = df['premium'].map({'Yes': True, 'No': False})
        print("\nAfter mapping 'premium' to boolean:")
        print(df[['premium']].head())  # Print 'premium' column to check the mapping

        # 3. Rename columns (axis indexes)
        if any(col not in df.columns for col in ['policy_id', 'user_id', 'role', 'plan_type', 'monthly_rate']):
            raise KeyError("Required columns are missing in the dataframe.")
        
        df.rename(columns={
            'policy_id': 'Policy_ID',
            'user_id': 'User_ID',
            'role': 'Role',
            'plan_type': 'Plan_Type',
            'monthly_rate': 'Monthly_Rate',
            'premium': 'Is_Premium',
            'region': 'Region',
            'login_count': 'Login_Count',
            'last_login_days': 'Last_Login_Days'
        }, inplace=True)
        print("\nAfter renaming columns:")
        print(df.head())  # Print the dataframe after renaming columns

        # 4. Discretization/Binning of 'monthly_rate'
        if df['Monthly_Rate'].isnull().any():
            raise ValueError("Column 'Monthly_Rate' contains missing values.")
        
        df.loc[:, 'Monthly_Rate_Binned'] = pd.cut(df['Monthly_Rate'], bins=[0, 20, 50, 100, 150],
                                                   labels=["Low", "Medium", "High", "Very High"])
        print("\nAfter binning 'Monthly_Rate':")
        print(df[['Monthly_Rate', 'Monthly_Rate_Binned']].head())  # Check the binning results

        # 5. Detecting and filtering outliers based on 'monthly_rate' (using IQR method)
        if df['Monthly_Rate'].isnull().any():
            raise ValueError("Column 'Monthly_Rate' contains missing values. Cannot compute outliers.")
        
        Q1 = df['Monthly_Rate'].quantile(0.25)
        Q3 = df['Monthly_Rate'].quantile(0.75)
        IQR = Q3 - Q1
        df = df[(df['Monthly_Rate'] >= (Q1 - 1.5 * IQR)) & (df['Monthly_Rate'] <= (Q3 + 1.5 * IQR))]
        print("\nAfter filtering outliers:")
        print(df.head())  # Check the result after filtering outliers

        # 6. Random sampling: Sample 50% of the dataset
        df_sampled = df.sample(frac=0.5, random_state=42)
        print("\nAfter sampling 50% of the data:")
        print(df_sampled.head())  # Check the sampled data

        # 7. Categorize 'premium' (e.g., 'Yes' = High Premium, 'No' = Low Premium)
        df['Premium_Category'] = df['Is_Premium'].apply(lambda x: 'High Premium' if x else 'Low Premium')
        print("\nAfter categorizing 'premium':")
        print(df[['Is_Premium', 'Premium_Category']].head())  # Check the premium categories

        # 8. Data wrangling (e.g., combining multiple columns into one)
        df['Plan_Info'] = df['Plan_Type'] + " - " + df['Monthly_Rate_Binned'].astype(str)
        print("\nAfter creating 'Plan_Info' column:")
        print(df[['Plan_Type', 'Monthly_Rate_Binned', 'Plan_Info']].head())  # Check the new 'Plan_Info' column

        # 9. Data join by using `user_id` from an additional DataFrame
        other_df = pd.DataFrame({
            'User_ID': ['U001', 'U002', 'U003', 'U004', 'U005'],
            'Subscription_Status': ['Active', 'Inactive', 'Active', 'Active', 'Inactive']
        })
        df = df.merge(other_df, on='User_ID', how='left')
        print("\nAfter merging with 'other_df':")
        print(df[['User_ID', 'Subscription_Status']].head())  # Check the result of the merge

        # 10. Reshaping data (e.g., pivoting)
        df_reshaped = df.pivot_table(index='User_ID', columns='Plan_Type', values='Login_Count', aggfunc='sum')
        print("\nAfter reshaping (pivoting):")
        print(df_reshaped.head())  # Check the reshaped data

        # Return the transformed DataFrame and reshaped data
        return df, df_sampled, df_reshaped

    # Exception or Error handling
    except KeyError as e:
        print(f"Missing column: {e}")
        return None, None, None
    except ValueError as e:
        print(f"Value error: {e}")
        return None, None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None, None

# Transformation Usage
if __name__ == "__main__":
    print('I am here ')
    try:
        # Read the CSV file
        df = pd.read_csv("iam_policies.csv")
        if df.empty:
            raise ValueError("CSV file is empty.")
        
        # Apply transformation
        df_transformed, df_sampled, df_reshaped = transform_data(df)

        # Display the transformed data (if no error occurred)
        if df_transformed is not None:
            print("\nTransformed Data (first few rows):")
            print(df_transformed.head())  # Print the transformed data

            print("\nSampled Data (50%):")
            print(df_sampled.head())  # Print the sampled data

            print("\nReshaped Data (Pivoted):")
            print(df_reshaped.head())  # Print the reshaped data

    # Error Handling
    except Exception as e:
        print(f"Error reading the CSV file or applying transformations: {e}")
