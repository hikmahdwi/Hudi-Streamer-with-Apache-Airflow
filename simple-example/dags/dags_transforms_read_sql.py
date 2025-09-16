import datetime
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
  dag_id="transform_release",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
):
  SparkSubmitOperator(
    task_id="transform_release_task",
    application="jobs/job_transform_release.py",
    conn_id="spark_default",
    jars="jobs/jar/hudi-utilities-slim-bundle_2.12-1.0.2.jar",
    packages=(
      'org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2,'  
      'org.apache.hadoop:hadoop-aws:3.3.4,'
      'com.amazonaws:aws-java-sdk-bundle:1.12.262'
    ),
    conf={
      'spark.executor.cores': '1',
      'spark.cores.max': '1'
    }
  ) 


