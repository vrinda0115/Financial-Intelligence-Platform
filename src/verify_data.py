import pandas as pd

# Load the newly created master file
df = pd.read_csv('data/processed/master_silver.csv')

# 1. Check the total volume of data
print(f"Total rows in Master Silver table: {len(df):,}")

# 2. Verify there are no duplicate rows
print(f"Total duplicate rows: {df.duplicated().sum()}")

# 3. Check if our custom feature engineering worked across all days!
print("\nBalance Mismatch Flag distribution:")
print(df['balance_mismatch_flag'].value_counts())