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



dag = DAG("test2", catchup=True, default_args=default_args )


# def exec_backfill_script(start_date,end_date):
#     for i in range(12):
#         d = DummyOperator(dag=dag,task_id='branch_{0}{1}'.format(i,start_date))
#         d
#     return d >> DummyOperator(dag=dag,task_id='dummy'+"_"+start_date)

# from datetime import datetime,timedelta
# start_date="2021-01-07"
# end_date="2021-02-13"
# # end_date=(datetime.strptime(end_date, "%Y-%m-%d") + timedelta(3)).strftime('%Y-%m-%d')
# while start_date < end_date:
#     t1_date=(datetime.strptime(start_date, "%Y-%m-%d") + timedelta(0)).strftime('%Y-%m-%d')
#     t2_date=(datetime.strptime(start_date, "%Y-%m-%d") + timedelta(6)).strftime('%Y-%m-%d')
#     exec_backfill_script(t1_date,t2_date)
#     start_date=(datetime.strptime(start_date, "%Y-%m-%d") + timedelta(7)).strftime('%Y-%m-%d')

# from datetime import datetime, timedelta

# import airflow
# from airflow.operators.dummy_operator import DummyOperator

# args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2018, 1, 8),
#     'email': ['myemail@gmail.com'],
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': timedelta(seconds=5)
# }

# dag = airflow.DAG(
#     'parallel_tasks_v1',
#     schedule_interval="@daily",
#     catchup=False,
#     default_args=args)

# # You can read this from variables
# parallel_tasks_total_number = 10

# start_task = DummyOperator(
#     task_id='start_task',
#     dag=dag
# )


# # Creates the tasks dynamically.
# # Each one will elaborate one chunk of data.
# def create_dynamic_task(current_task_number):
#     return DummyOperator(
#         provide_context=True,
#         task_id='parallel_task_' + str(current_task_number),
#         python_callable=parallelTask,
#         # your task will take as input the total number and the current number to elaborate a chunk of total elements
#         op_args=[current_task_number, int(parallel_tasks_total_number)],
#         dag=dag)


t1 = PythonOperator(
    task_id='print',
    python_callable= my_function,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t1
# for page in range(int(parallel_tasks_total_number)):
#     created_task = create_dynamic_task(page)
#     start_task >> created_task
#     created_task >> end



# dag = DAG('pipeline', ...)
# list_of_files = [......]
# with dag:
#     import itertools
#     for file in itertools.product(range(12), repeat=4):
#        t1 = DummyOperator(task_id='start_task'+"_"+str(file[0]),dag=dag)
#        t2 = DummyOperator(task_id='process'+"_"+str(file[1]),dag=dag)
#        t1.set_downstream(t2)
