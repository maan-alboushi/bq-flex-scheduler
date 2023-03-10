# bq-flex-scheduler
 This code is an Airflow DAG that creates a BigQuery Flex commitment, reservation and assignment then query a simple table and finally deletes all slots.
 
 ## GCP infrastructure:
 * Create a Cloud Composer version 2.
 * Use version image "composer-2.1.8-airflow-2.4.3".
 * Add "google-cloud-bigquery-reservation==1.7.0".
 * Copy "main.py" to the bucket created by the Cloud Composer.
 
 ## Airflow code:
  Airflow code has tasks to create empty table and a dataset then delete them for the sake of demo.
 * Add project id.
 * Change tasks sequence as needed.
 * Change slots capacity amount.
 
