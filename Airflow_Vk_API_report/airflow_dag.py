from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'dkuznetsov',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 2),
    'retries': 0
    }

dag = DAG(dag_id='metrics_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval='00 12 * * 1')

t1 = BashOperator(task_id='metrics_counter',
                  bash_command='python metrics_script.py',
                  dag=dag)