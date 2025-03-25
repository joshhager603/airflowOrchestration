import sys
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# ------- Constants -------
PROJECT_DIR = '/Users/joshhager/josh/classes/csds397/indivAssign5/'

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from scripts.load_raw_data import load_raw_data
from scripts.data_cleaning import clean_data
from scripts.load_clean_data import load_clean_data

dag = DAG('employee_data_dag', schedule_interval='@daily', start_date=datetime(2025, 3, 25))

load_raw_data_task = PythonOperator(task_id='load_raw_data_task', python_callable=load_raw_data, dag=dag)

clean_data_task = PythonOperator(task_id='clean_data', python_callable=lambda: load_clean_data(clean_data()), dag=dag)

load_raw_data_task >> clean_data_task