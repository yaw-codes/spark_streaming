import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Start date for the DAG
start_date = datetime(2025, 4, 16)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,  # <-- Fix: use 'catchup' instead of 'backfill'
    'start_date': start_date
}

# Parameters
num_rows = 100
output_file = './account_dim_large_data.csv'

def generate_random_data(row_num):
    account_id = f"A{row_num:05d}"
    account_type = random.choice(['savings', 'checking', 'credit'])
    status = random.choice(['active', 'inactive'])
    customer_id = f"C{random.randint(1, 1000):05d}"
    balance = round(random.uniform(100.00, 10000.00), 2)
    now = datetime.now()
    random_date = now - timedelta(days=random.randint(1, 365))
    opening_date_millis = int(random_date.timestamp() * 1000)
    return account_id, account_type, status, customer_id, balance, opening_date_millis

def generate_account_dim_data():
    account_ids = []
    account_types = []
    statuses = []
    customer_ids = []
    balances = []
    opening_dates = []

    for row_num in range(1, num_rows + 1):
        account_id, account_type, status, customer_id, balance, opening_date = generate_random_data(row_num)
        account_ids.append(account_id)
        account_types.append(account_type)
        statuses.append(status)
        customer_ids.append(customer_id)
        balances.append(balance)
        opening_dates.append(opening_date)

    df = pd.DataFrame({
        'account_id': account_ids,
        'account_type': account_types,
        'status': statuses,
        'customer_id': customer_ids,
        'balance': balances,
        'opening_date': opening_dates
    })

    df.to_csv(output_file, index=False)
    print(f"CSV file {output_file} with {num_rows} rows has been generated successfully.")

# Define DAG
with DAG(dag_id='account_dim_generator',
         default_args=default_args,
         description='Generates account dimension data in a CSV file',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['schema']) as dag:

    start = EmptyOperator(task_id='start_task')

    generate_account_dim_data_task = PythonOperator(
        task_id='generate_account_dim_data',
        python_callable=generate_account_dim_data
    )

    end = EmptyOperator(task_id='end_task')

    start >> generate_account_dim_data_task >> end
