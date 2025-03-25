import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# ------- Constants -------
PROJECT_DIR = '/Users/joshhager/josh/classes/csds397/indivAssign5'

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from scripts.load_raw_data import load_raw_data
from scripts.data_cleaning import clean_data
from scripts.load_clean_data import load_clean_data

dag = DAG('employee_data_dag', schedule_interval='@daily', start_date=datetime(2025, 3, 25))

load_raw_data_task = PythonOperator(
    task_id='load_raw_data_task', 
    python_callable=load_raw_data, 
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data_task', 
    python_callable=lambda: load_clean_data(clean_data()), 
    dag=dag
)

with open(os.path.join(PROJECT_DIR, 'scripts/1_average_salary_by_department.sql')) as avg_salary_by_dept_query:
    avg_salary_by_dept_task = SQLExecuteQueryOperator(
        task_id='avg_salary_by_dept_task',
        sql=avg_salary_by_dept_query.read(),
        conn_id='postgres_employee_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/2_average_salary_by_yoe.sql')) as avg_salary_by_yoe_query:
    avg_salary_by_yoe_task = SQLExecuteQueryOperator(
        task_id='avg_salary_by_yoe_task',
        sql=avg_salary_by_yoe_query.read(),
        conn_id='postgres_employee_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/3_average_salary_by_performance.sql')) as avg_salary_by_performance_query:
    avg_salary_by_performance_task = SQLExecuteQueryOperator(
        task_id='avg_salary_by_performance_task',
        sql=avg_salary_by_performance_query.read(),
        conn_id='postgres_employee_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/4_average_salary_by_country.sql')) as avg_salary_by_country_query:
    avg_salary_by_country_task = SQLExecuteQueryOperator(
        task_id='avg_salary_by_country_task',
        sql=avg_salary_by_country_query.read(),
        conn_id='postgres_employee_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/5_average_yoe_by_performance.sql')) as avg_yoe_by_performance_query:
    avg_yoe_by_performance_task = SQLExecuteQueryOperator(
        task_id='avg_yoe_by_performance_task',
        sql=avg_yoe_by_performance_query.read(),
        conn_id='postgres_employee_db',
        autocommit=True,
        dag=dag
    )

with open(os.path.join(PROJECT_DIR, 'scripts/6_employees_per_department.sql')) as employees_per_department_query:
    employee_per_department_task = SQLExecuteQueryOperator(
        task_id='employee_per_department_task',
        sql=employees_per_department_query.read(),
        conn_id='postgres_employee_db',
        autocommit=True,
        dag=dag
    )

load_raw_data_task >> clean_data_task
clean_data_task >> avg_salary_by_dept_task
clean_data_task >> avg_salary_by_yoe_task
clean_data_task >> avg_salary_by_performance_task
clean_data_task >> avg_salary_by_country_task
clean_data_task >> avg_yoe_by_performance_task
clean_data_task >> employee_per_department_task