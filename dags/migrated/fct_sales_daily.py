# AFTER the migration (Airflow 3.x, Task SDK).
# Daily incremental load of FCT_SALES_DAILY: copy the last 15 days of changes
# into a temp table, then replace those sales in the fact table.
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

REPO_DIR = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent

sys.path.insert(1, str(REPO_DIR / "utils"))
from teams_notify import send_teams_message  # noqa: E402


def on_task_failure(context):
    send_teams_message(
        f"Job: **{context['dag_run'].dag_id}** task: **{context['task'].task_id}** Failed!",
        channel="job-fail",
    )


def on_dag_success(context):
    send_teams_message(
        f"Job: **{context['dag_run'].dag_id}** Done!",
        channel="job-success",
    )


# default_args are copied onto every task, so retries and the failure alert
# apply to both steps.
default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

DAG_NAME = "fct_sales_daily"

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=CronDataIntervalTimetable(cron="30 5 * * *", timezone="Asia/Bangkok"),
    catchup=False,
    tags=["airflow-migrate", "prod"],
)

# The trailing space after ".sh" is on purpose: without it Airflow treats a
# command ending in ".sh" as a Jinja template file and tries to load it.
step1_load_data = BashOperator(
    task_id="step1_load_data",
    bash_command=f"{SCRIPT_DIR}/step1_load_data.sh ",
    dag=dag,
)

step2_update_table = BashOperator(
    task_id="step2_update_table",
    bash_command=f"{SCRIPT_DIR}/step2_update_table.sh ",
    on_success_callback=on_dag_success,  # one "done" message per run, not one per task
    dag=dag,
)

step1_load_data >> step2_update_table
