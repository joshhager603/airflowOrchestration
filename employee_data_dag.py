import sys
import os
# ------- Project Directory -------
PROJECT_DIR = '/Users/joshhager/josh/classes/csds397/indivAssign5'

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from scripts.load_raw_data import load_raw_data
from scripts.data_cleaning import clean_data
from scripts.load_clean_data import load_clean_data
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(PROJECT_DIR, '.env'))

# Email configuration
SMTP_SERVER = "smtp.gmail.com"  
SMTP_PORT = 587
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")


def send_email(context):

    try:
        # Create the email
        task_instance = context.get('task_instance')
        subject = f"Task {task_instance.task_id} failed in DAG {task_instance.dag_id}"
        body = f"The task failed. Here's the error details:\n\n{str(context)}"
        
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECIPIENT_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Connect to the SMTP server and send the email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()  # Upgrade the connection to secure
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

default_args = { 
    'owner': 'admin',
    'start_date': datetime(2025, 3, 25),
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'on_failure_callback': send_email,
}

dag = DAG(
    'employee_data_dag',
    default_args=default_args,
    description='A DAG to run an employee data pipeline',
    schedule_interval='@daily'
)

load_raw_data_task = PythonOperator(
    task_id='load_raw_data_task', 
    python_callable=load_raw_data,
    retries=5,
    retry_delay=timedelta(seconds=10),
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data_task', 
    python_callable=lambda: load_clean_data(clean_data()), 
    retries=5,
    retry_delay=timedelta(seconds=10),
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