import pandas as pd
import os
import glob
import shutil

# CONFIGURATION PATHS
LANDING_ZONE = 'data/landing_zone/'
ARCHIVE_DIR = 'data/archive/'
PROCESSED_DIR = 'data/processed/'
MASTER_SILVER_FILE = PROCESSED_DIR + 'master_silver.csv'

def run_incremental_etl():
    # Ensure directories exist
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Find all CSV files waiting in the landing zone
    new_files = glob.glob(LANDING_ZONE + '*.csv')
    
    if len(new_files) == 0:
        print("No new data found in landing zone. Pipeline sleeping... 💤")
        return

    print(f"Found {len(new_files)} new files. Starting Incremental ETL Job...\n")

    for file_path in new_files:
        filename = os.path.basename(file_path)
        print(f"⚙️ Processing {filename}...")
        
        # --- 1. EXTRACT ---
        df = pd.read_csv(file_path)
        
        # --- 2. TRANSFORM (Silver Layer Logic) ---
        # Clean negative numbers
        df = df[df['amount'] >= 0]
        df = df[df['oldbalanceOrg'] >= 0]
        
        # Feature Engineering: Add Balance Mismatch Flag
        df['balance_mismatch_flag'] = (
            (df['type'].isin(['TRANSFER', 'CASH_OUT'])) & 
            (abs(df['oldbalanceOrg'] - df['amount'] - df['newbalanceOrig']) > 1)
        ).astype(int)
        
        # --- 3. LOAD (Append to Master Table) ---
        # If the master file exists, append to it. If not, create it.
        if os.path.exists(MASTER_SILVER_FILE):
            df.to_csv(MASTER_SILVER_FILE, mode='a', header=False, index=False)
        else:
            df.to_csv(MASTER_SILVER_FILE, mode='w', header=True, index=False)
            
        # --- 4. ARCHIVE ---
        # Move the raw file to the archive so we don't process it again tomorrow
        shutil.move(file_path, os.path.join(ARCHIVE_DIR, filename))
        
        print(f"✅ Success: {filename} appended to Master and archived.\n")

    print("🚀 ETL Job Complete! Master Silver table updated.")

if __name__ == "__main__":
    run_incremental_etl()