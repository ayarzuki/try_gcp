import datetime
import os
from airflow import models
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator, DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.utils import trigger_rule

# Path to PySpark script in GCS
PYSPARK_FILE = 'gs://dibimbing_bucket-1/word-count.py'

# Output file for PySpark job
output_file = os.path.join(
    models.Variable.get('gcs_bucket'), 'pyspark_output',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep

# Arguments to pass to PySpark job
pyspark_args = [output_file]

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'composer_pyspark_tutorial',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    
    create_dataproc_cluster = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-pyspark-tutorial-cluster-{{ ds_nodash }}',
        num_workers=2,
        region='asia-southeast1',
        zone=models.Variable.get('gce_zone'),
        image_version='2.0',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2')

    run_dataproc_pyspark = DataProcPySparkOperator(
        task_id='run_dataproc_pyspark',
        main=PYSPARK_FILE,
        arguments=pyspark_args,
        cluster_name='composer-pyspark-tutorial-cluster-{{ ds_nodash }}',
        region='asia-southeast1',
        dag=dag)

    delete_dataproc_cluster = DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region='asia-southeast1',
        cluster_name='composer-pyspark-tutorial-cluster-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster