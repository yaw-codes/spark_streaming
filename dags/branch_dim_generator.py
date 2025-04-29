import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


start_date = datetime(2025, 4, 16)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,  # <-- Fix: use 'catchup' instead of 'backfill'
    'start_date': start_date
}

#parameters
num_rows = 100
output_file = './branch_dim_large_data.csv'

#list of sample UK cities and regions for realistic data generation
cities = ['London', 'Birmingham', 'Manchester', 'Liverpool', 'Newcastle', 'Sheffield',]
regions = ["London", "West Midlands", "Scotlant", "Greater Manchester", "Yorkshire", "South East"]
postcodes = ["SW1A 1AA", "B1 1AA", "M1 1AE", "L1 1AA", "NE1 1AA", "S1 2AA"]


def generate_random_data(row_num):
    branch_id= f"B{row_num:04d}"
    branch_name = f"Branch {row_num}"
    branch_address = f"{random.randint(1, 999)} {random.choice(['High St', 'Main St','Queen St','Church Rd', 'Broadway'])}"
    city = random.choice(cities)
    region = random.choice(regions)
    postcode = random.choice(postcodes)

    #Generate opening date in milliseconds
    now = datetime.now()
    random_date = now - timedelta(days=random.randint(1, 3650)) #random date within the last 10 years
    opening_date_millis = int(random_date.timestamp() * 1000)# convert to milliseconds since epoch

    return branch_id, branch_name, branch_address, city, region, postcode, opening_date_millis



#initialize empty lists to store data
branch_ids = []
branch_names = []
branch_addresses = []
branch_cities = []
branch_regions = []
postcodes_list = []
opening_dates = []


def generate_branch_dim_data():
    row_num = 1
    while row_num <= num_rows:
        branch_id, branch_name, branch_address, city, region, postcode, opening_date = generate_random_data(row_num)
        branch_ids.append(branch_id)
        branch_names.append(branch_name)
        branch_addresses.append(branch_address)
        branch_cities.append(city)
        branch_regions.append(region)
        postcodes_list.append(postcode)
        opening_dates.append(opening_date)


        row_num += 1
    df = pd.DataFrame({
        'branch_id': branch_ids,
        'branch_name': branch_names,
        'branch_address': branch_addresses,
        'branch_city': branch_cities,
        'branch_region': branch_regions,
        'postcode': postcodes_list,
        'opening_date': opening_dates
    })

    df.to_csv(output_file, index=False)
    print(f"Csv file with {output_file} with {num_rows} has been generated succesfully.")

with DAG(dag_id='branch_dim_generator',
         default_args=default_args,
         description='to generate branch dimension data in a csv file',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['schema']) as dag:

    start = EmptyOperator(
        task_id='start_task'
    )

    generate_branch_dim_data_task = PythonOperator(
        task_id='generate_branch_dim_data',
        python_callable=generate_branch_dim_data
    )

    end = EmptyOperator(
        task_id='end_task'
    )

    start >>  generate_branch_dim_data_task >> end