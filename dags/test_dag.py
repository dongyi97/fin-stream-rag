# dags/test_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# 기본 인수 설정
default_args = {
    'owner': 'test_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'test_simple_dag',
    default_args=default_args,
    description='간단한 테스트용 DAG',
    schedule=timedelta(minutes=10),  # 10분마다 실행
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'example'],
)

# Task 1: 간단한 Python 함수
def print_hello():
    print("Hello from Airflow!")
    print(f"Current time: {datetime.now()}")
    return "Task completed successfully"

task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 2: Bash 명령어 실행
task_bash = BashOperator(
    task_id='bash_task',
    bash_command='echo "This is a bash task" && date',
    dag=dag,
)

# Task 3: Python 함수로 간단한 계산
def simple_calculation():
    result = sum(range(1, 101))
    print(f"Sum of 1 to 100: {result}")
    return result

task_calc = PythonOperator(
    task_id='simple_calculation',
    python_callable=simple_calculation,
    dag=dag,
)

# Task 의존성 설정
task_hello >> task_bash >> task_calc