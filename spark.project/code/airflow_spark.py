from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 6),
    'retries': 0,
}
test_dag = DAG(
    'test_bash_script_dag',
    default_args=default_args,
    schedule_interval=None
)
# Define the BashOperator task
bash_task = BashOperator(
    task_id='bash_task_execute_script',
    bash_command='/home/schronak/git.source.code/spark.project/code/run_pyspark_code.sh ',
    dag=test_dag
)
# Set task dependencies
bash_task
