# bq-flex-scheduler
 This code is an Airflow DAG that creates a BigQuery Flex commitment, reservation and assignment then query a simple table and finally deletes all slots.
 
 ## GCP infrastructure:
 1- Create a Cloud Composer version 2.
 2- Use version image "composer-2.1.8-airflow-2.4.3".
 3- Add "google-cloud-bigquery-reservation==1.7.0".
 4- copy "main.py" to the bucket created by the Cloud Composer.
 
 ## Airflow code:
 1- Add project id.
 2- Change tasks sequence as needed.
 3- Change slots capacity amount.
 Airdlow code also adds tasks to create empty table and a dataset then delete them for the sake of demo.
 
