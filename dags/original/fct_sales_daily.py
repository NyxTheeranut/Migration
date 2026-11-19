# BEFORE the migration (Airflow 1.x). Kept as found, only the server paths
# were made generic. See docs/migration_pattern.md for what changed and why.
import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from configparser import ConfigParser

config_object = ConfigParser()
config_object.read(f'/opt/etl/airflow/dags/path_config.ini')
util_obj = config_object['util']
utils_path = util_obj['path']
config_file_name = util_obj['config_file_name']

sys.path.insert(1, '/opt/etl/utils/discord_noti')
from discord_noti import sendNoti

sys.path.insert(1, utils_path)
from database_config import Config
config = Config(f'{utils_path}/{config_file_name}')
ip, port, username, password, database = config.get_database_config()

env = {
    'ip': ip,
    'port': port,
    'username': username,
    'password': password,
    'database': database
}


def on_dag_failure(context):
    message = f"Job failure:\nJob name: {context['dag_run'].dag_id}\ntask name: {context['task'].task_id}"
    sendNoti(message)

def on_dag_success(context):
    message = f"Job success:\njob name: {context['dag_run'].dag_id}\ntask name: {context['task'].task_id}"
    sendNoti(message)


default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'email': ['data_team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'on_success_callback': on_dag_success,
    'on_failure_callback': on_dag_failure
}

DAG_NAME = 'fct_sales_daily'
dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="30 5 * * *")

main_path = f"/opt/etl/airflow/scripts/{DAG_NAME}"

step1_load_data = BashOperator(
      task_id='step1_load_data'
    , bash_command=f'{main_path}/step1_load_data.sh $ip $port $username $password $database'
    , env=env
    , dag=dag)

step2_update_table = BashOperator(
      task_id='step2_update_table'
    , bash_command=f'{main_path}/step2_update_table.sh $ip $port $username $password $database'
    , env=env
    , dag=dag)

# JOB DEPENDENCIES
step2_update_table.set_upstream(step1_load_data)
