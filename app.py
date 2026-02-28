import pandas as pd
#bronze layer

#Exploratory Data Analysis
df = pd.read_csv('data.csv')
print(df.info())
print(df.isnull().sum())
print(df.head())

print(df.describe())
print(df['type'].value_counts())
print(df['isFraud'].value_counts())

#Sample Data
sample_df = df.sample(n=200_000, random_state=42)
print("total rows in sample data:", len(sample_df))

#Silver Layer

#data quality rules
#amount should be >0
invalid_amount = sample_df[sample_df['amount']<0]
#old balance should be >=0
invalid_old_balance = sample_df[sample_df['oldbalanceOrg']<0]
#new balance should be >=0
invalid_new_balance = sample_df[sample_df['newbalanceOrig']<0]
#balance consistency: transfer & cash_out
balance_mismatch = sample_df[
    (sample_df["type"].isin(["TRANSFER", "CASH_OUT"])) &
    (abs(sample_df["oldbalanceOrg"] - sample_df["amount"] - sample_df["newbalanceOrig"]) > 1)
]
#fraud flag must be binary
invalid_fraud_flag = sample_df[~sample_df["isFraud"].isin([0, 1])]

dq_summary = {
    "invalid_amount": len(invalid_amount),
    "invalid_old_balance": len(invalid_old_balance),
    "invalid_new_balance": len(invalid_new_balance),
    "balance_mismatch": len(balance_mismatch),
    "invalid_fraud_flag": len(invalid_fraud_flag)
}
print("\nData Quality Summary: ")
for k, v in dq_summary.items():
    print(f"{k}: {v}")

sample_df['balance_mismatch_flag'] = (
    (sample_df['type'].isin(['TRANSFER', 'CASH_OUT'])) &
    (abs(sample_df['oldbalanceOrg'] - sample_df['amount'] - sample_df['newbalanceOrig']) > 1)
).astype(int)

print("\nBalance Mismatch Flag added to DataFrame.")

invalid_amount.to_csv("dq_invalid_amount.csv", index=False)
invalid_old_balance.to_csv("dq_invalid_old_balance.csv", index=False)
invalid_new_balance.to_csv("dq_invalid_new_balance.csv", index=False)
invalid_fraud_flag.to_csv("dq_invalid_fraud_flag.csv", index=False)
print("Samples converted to CSV Files")

#clean dataset
bad_indices = set(
    invalid_amount.index
    .union(invalid_old_balance.index)
    .union(invalid_new_balance.index)
    .union(invalid_fraud_flag.index)
)
df_silver = sample_df.drop(index=bad_indices)
df_silver.to_csv(
    "paysim_silver_with_quality_flags.csv",
    index=False
)

print("\nFiles generated:")
print("- paysim_silver_with_quality_flags.csv")
print("- dq_invalid_amount.csv")
print("- dq_invalid_old_balance.csv")
print("- dq_invalid_new_balance.csv")
print("- dq_invalid_fraud_flag.csv")

#Gold Layer
gold_metrics = df_silver.groupby("type").agg(
    total_txn = ("amount", "count"),
    total_amount = ("amount", "sum"),
    fraud_count = ("isFraud", "sum"),
    mismatch_rate = ("balance_mismatch_flag", "mean")
).reset_index()

gold_metrics.to_csv("paysim_gold_metrics.csv", index=False)
print("\nGold Metrics saved to paysim_gold_metrics.csv")