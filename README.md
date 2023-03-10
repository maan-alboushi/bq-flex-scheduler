# Shedule a reservation of BigQuery Flex slots:
 This code is an Airflow DAG that creates a BigQuery Flex commitment, reservation and assignment then query a simple table and finally deletes all slots.
 
 ## GCP infrastructure:
 * Create a Cloud Composer version 2.
 * Use version image "composer-2.1.8-airflow-2.4.3".
 * Add "google-cloud-bigquery-reservation==1.7.0".
 * Copy "main.py" to the bucket created by the Cloud Composer.
 
 ## Airflow code:
  Airflow code has tasks to create empty table and a dataset then delete them for the sake of demo.
 * Add project id.
 * Change tasks sequence as needed, see https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
 * Change slots capacity amount.
 * By default Tasks are entirely isolated and may be running on entirely different machines therefore XCom must be used, see https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
