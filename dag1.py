from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import DAG
import random
from airflow.operators.python_operator import PythonOperator


# start_dt = '''{{ (execution_date + macros.timedelta(days=-92)).strftime('%Y-%m-%d') }}'''
end_dt = '''{{ (execution_date + macros.timedelta(days=-1)).strftime('%Y-%m-%d') }}'''
print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
# print('''{{ execution_date }}''')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def my_function(x):
    print("%%%%%%%%%%%%%%%%%%%%%%%%")
    print(end_dt)
# def return_branch(**kwargs):
#     branches = ['branch_0']
#     return random.choice(branches)



# with DAG("branch_operator_guide", default_args=default_args, schedule_interval=None) as dag:
#     kick_off_dag = DummyOperator(task_id='run_this_first')

#     branching = BranchPythonOperator(
#         task_id='branching',
#         python_callable=return_branch,
#         provide_context=True)

#     kick_off_dag >> branching
#     count=0
#     for i in range(0, 4):
#         d = DummyOperator(task_id='branch_{0}'.format(i))
#         for j in range(0, 3):
#             m = DummyOperator(task_id='branch_{0}_{1}'.format(i, j))
#             for k in range(0,3):
#                 n=DummyOperator(task_id='branch_{0}_{1}_{2}'.format(i,j,k))
#                 d >> m >>n
#         branching >> d
