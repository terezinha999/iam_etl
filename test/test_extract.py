from etl.extract import extract_data

df = extract_data("data/iam_policies.csv")
print(df.head())