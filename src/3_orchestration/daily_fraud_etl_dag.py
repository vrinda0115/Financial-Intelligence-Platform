from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Define the rules for the pipeline
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 25),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Create the DAG (The Schedule)
# 'schedule_interval' tells it to run at 6:00 AM every day
with DAG(
    'fraud_incremental_etl_pipeline',
    default_args=default_args,
    description='Daily ETL job to process new banking logs',
    schedule_interval='0 6 * * *', 
    catchup=False
) as dag:

    # 3. Define the Tasks
    # Task 1: Check if new files exist (Optional but good practice)
    check_landing_zone = BashOperator(
        task_id='check_landing_zone',
        bash_command='ls /path/to/your/data/landing_zone/ | wc -l'
    )

    # Task 2: Run your exact Python script!
    run_incremental_etl = BashOperator(
        task_id='run_incremental_etl',
        bash_command='python /path/to/your/src/2_processing/incremental_etl.py'
    )

    # 4. Set the Order (Dependencies)
    # This says: Do Task 1 first, then do Task 2
    check_landing_zone >> run_incremental_etl