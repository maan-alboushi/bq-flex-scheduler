from __future__ import annotations
import pendulum
import logging
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators import bigquery
from airflow.utils.trigger_rule import TriggerRule
# google-cloud-bigquery-reservation==1.7.0 compatible with other libraries in airflow 2.4.3 used by Cloud Composer.
from google.cloud.bigquery_reservation_v1 import *

# GCP Project id
project_id = "your-project-id"

# Slots location See: https://cloud.google.com/bigquery/docs/locations for a list of available locations.
location = "US"

# Reservation id with unix time stamp without a decimal point.
reservation_id = "slots-reservation" + str(int(time.time()))

# Number of slots to be reserved.
slot_capacity = 100

# Choose transport to use. Either "grpc" or "rest"
transport = "grpc"

# Dataset and table name.
DATASET_ID = "simple_dataset"
TABLE_ID = "simple_table"

# Reservation request builder.
reservation_client = ReservationServiceClient()
parent = reservation_client.common_location_path(project_id, location)
logger = logging.getLogger("logger")


# Deletes the reservation, assignment and the commitment of the bq flex.
def delete_reservation(ti, **context):
    reservation_client.delete_assignment(name=ti.xcom_pull(
        task_ids="create_assignment", dag_id="bq-flex-reservation", key="assignment_name"))
    logger.info("assignment deleted")

    reservation_client.delete_reservation(name=ti.xcom_pull(
        task_ids="create_reservation", dag_id="bq-flex-reservation", key="reservation_name"))
    logger.info("reservation deleted")

    reservation_client.delete_capacity_commitment(name=ti.xcom_pull(
        task_ids="create_commitment",dag_id="bq-flex-reservation", key="commitment_name"))
    logger.info("commitment deleted")


# Create a reservation with a slot capacity e.g. 100.
def create_reservation(ti, **context):
    reservation = Reservation(slot_capacity=slot_capacity)
    reserve = reservation_client.create_reservation(parent=parent, reservation=reservation,
                                                    reservation_id=reservation_id, )
    ti.xcom_push(key="reservation_name", value=reserve.name)
    logging.info(reserve.name)


# Commits to a bq flex plan with a slot count e.g. 100.
def create_commitment(ti, **context):
    capacity_commitment = CapacityCommitment(plan="FLEX", slot_count=slot_capacity)
    commitment = reservation_client.create_capacity_commitment(parent=parent,
                                                               capacity_commitment=capacity_commitment)
    ti.xcom_push(key="commitment_name", value=commitment.name)
    logging.info(commitment.name)


# Assign the reservation to a specific project.
def create_assignment(ti, **context):
    logging.info(ti.xcom_pull(task_ids='create_reservation', dag_id='bq-flex-reservation', key='reservation_name'))
    assignment = Assignment(job_type="QUERY", assignee="projects/{}".format(project_id))
    assign = reservation_client.create_assignment(
        parent=ti.xcom_pull(task_ids='create_reservation', dag_id='bq-flex-reservation', key='reservation_name'),
        assignment=assignment)
    # Wait for assignment to be reserved.
    time.sleep(60)
    ti.xcom_push(key="assignment_name", value=assign.name)
    logging.info(assign.name)


# DAG default args.
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": pendulum.today('UTC').add(days=-1),
    # "end_date": specify date
    # "email": ["example@example.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=20)
}

dag = DAG(
    # Set DAG settings.
    dag_id="bq-flex-reservation",
    default_args=default_args,
    schedule="@daily",
    max_active_runs=1,
    description="use case of bq flex reservation in airflow",
    # dagrun_timeout=timedelta(minutes=5)
)

# Create empty dataset in bq.
create_empty_dataset = bigquery.BigQueryCreateEmptyDatasetOperator(
    task_id="create_empty_dataset",
    project_id=project_id,
    dataset_id=DATASET_ID,
    location=location,
    dag=dag
)

# Create empty table in bq.
create_empty_table = bigquery.BigQueryCreateEmptyTableOperator(
    task_id="create_empty_table",
    project_id=project_id,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=[
        {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    dag=dag
)

# Create bq commitment
commit_flex = PythonOperator(
    task_id="create_commitment",
    python_callable=create_commitment,
    dag=dag
)

# Reserve bq flex slots
reserve_slots = PythonOperator(
    task_id="create_reservation",
    python_callable=create_reservation,
    dag=dag
)

# Assign reservation to a project
assign_to_project = PythonOperator(
    task_id="create_assignment",
    python_callable=create_assignment,
    dag=dag
)

# Get bq data, for more examples check:
get_data = bigquery.BigQueryGetDataOperator(
    task_id="get_data",
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    max_results=1,
    selected_fields="emp_name,salary",
    location=location,
    dag=dag
)

# Delete reservation slots
delete_slots = PythonOperator(
    task_id="release_slots",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    python_callable=delete_reservation,
    dag=dag
)

# delete dataset including the tables in bq.
delete_dataset = bigquery.BigQueryDeleteDatasetOperator(
    task_id="delete_dataset",
    project_id=project_id,
    dataset_id=DATASET_ID,
    delete_contents=True,
    dag=dag
)

# Tasks sequence
create_empty_dataset >> create_empty_table >> commit_flex >> reserve_slots >> assign_to_project >> get_data >> delete_slots >> delete_dataset
