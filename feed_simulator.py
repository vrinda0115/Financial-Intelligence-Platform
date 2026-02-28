import pandas as pd
import os

# CONFIGURATION
SOURCE_FILE = 'data/raw_source/data.csv'  # Path to your massive dataset
LANDING_ZONE = 'data/landing_zone/'       # Where the daily files will drop

def simulate_daily_feed():
    print(f"Loading source data from {SOURCE_FILE}...")
    df = pd.read_csv(SOURCE_FILE)
    
    os.makedirs(LANDING_ZONE, exist_ok=True)
    
    # The dataset has a 'step' column (1 step = 1 hour).
    # We will convert this to 'Days' (24 steps = 1 Day).
    df['simulated_day'] = (df['step'] // 24) + 1
    
    days = df['simulated_day'].unique()
    print(f"Found {len(days)} days of data. Starting simulation...")

    # Split and Save
    for day in days:
        # Filter data for just this day
        daily_data = df[df['simulated_day'] == day].copy()
        
        # Drop the helper column so the file looks completely raw
        daily_data = daily_data.drop(columns=['simulated_day'])
        
        # Save as a separate CSV
        filename = f"{LANDING_ZONE}transactions_day_{day}.csv"
        daily_data.to_csv(filename, index=False)
        
        print(f"🚀 Generated: transactions_day_{day}.csv ({len(daily_data)} rows)")

    print("\n✅ Simulation Complete! Check your 'data/landing_zone' folder.")

if __name__ == "__main__":
    simulate_daily_feed()