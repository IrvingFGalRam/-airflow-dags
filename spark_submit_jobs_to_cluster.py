"""Spark cluster and job subbmiter DAG
Author: Irving FGR
Description: Creates an ephimeral dataproc spark cluster to submit jobs.
"""

import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

# General constants
DAG_ID = "dataproc_submit_job"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
JOB_SELECT_ENV = os.environ.get("JOB_NAME_SELECTOR", "")
CLUSTER_NAME = "cluster-dataproc-spark-deb"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"
REGION = "us-central1"
ZONE = "us-central1-a"

TIMEOUT = {"seconds": 1 * 2 * 60 * 60}

# Jobs definitions
SPARK_JOB_TEST = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TestSparkSession",
    },
}
SPARK_JOB_SHOW_TABLE = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TestSparkSession",
    },
    "arguments": {
        "gs://capstone-project-wzl-storage/silver/classified_movie_review",
        "avro",
        "25"
    }
}
SPARK_JOB_T_CMR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformClassifiedMovieReview",
    },
}
SPARK_JOB_T_RL = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformReviewLogs",
    },
}
SPARK_JOB_T_UP = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformUserPurchase",
    },
}
SPARK_JOB_OBT = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.GoldOBT",
    },
}

JOB_DICT = {
    "test" : SPARK_JOB_TEST,
    "show" : SPARK_JOB_SHOW_TABLE,
    "rl" : SPARK_JOB_T_RL,
    "crm" : SPARK_JOB_T_CMR,
    "uo" : SPARK_JOB_T_UP,
    "obt" : SPARK_JOB_OBT
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, CLUSTER],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task",
        job=JOB_DICT[JOB_SELECT_ENV],
        region=REGION,
        project_id=PROJECT_ID
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> spark_task >> end_workflow
