"""Spark cluster and job subbmiter DAG
Author: Irving FGR
Description: Creates an ephimeral dataproc spark cluster to submit jobs.
"""

import os
from airflow.models import Variable
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

# General constants
DAG_ID = "dataproc_submit_job"
CLUSTER_NAME = "cluster-dataproc-spark-deb"
CLOUD_PROVIDER = "gcp"
CLUSTER = "dataproc"
REGION = "us-central1"
ZONE = "us-central1-a"

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")
# "test", "show", "rl", "crm", "up", "obt"
JOB_NAME_SELECTOR = Variable.get("JOB_NAME_SELECTOR")
# ENVs to Arguments
ARG_TABLE_NAME = Variable.get("ARG_TABLE_NAME")
ARG_FORMAT = Variable.get("ARG_FORMAT")
ARG_N_RECORDS = Variable.get("ARG_N_RECORDS")

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
        "main_class": "org.example.ShowTable",
        "args": [
            "gs://capstone-project-wzl-storage/silver/" + ARG_TABLE_NAME,
            ARG_FORMAT,
            ARG_N_RECORDS
        ]
    }
}
SPARK_JOB_T_CMR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformClassifiedMovieReview",
        "args": [
            "gs://capstone-project-wzl-storage/bronze/movie_review.csv",
            "gs://capstone-project-wzl-storage/silver/classified_movie_review",
            ARG_FORMAT
        ]
    }
}
SPARK_JOB_T_RL = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformReviewLogs",
        "args": {
            "gs://capstone-project-w/zl-storage/bronze/log_reviews.csv",
            "gs://capstone-project-wzl-storage/silver/review_logs",
            ARG_FORMAT
        }
    }
}
SPARK_JOB_T_UP = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.TransformUserPurchase",
        "args": {
            "gs://capstone-project-wzl-storage/tmp/user_purchase_psql.csv",
            "gs://capstone-project-wzl-storage/silver/user_purchase",
            ARG_FORMAT
        }
    }
}
SPARK_JOB_OBT = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://capstone-project-wzl-storage/jars/scala-jobs_2.12-0.1.1.jar"],
        "main_class": "org.example.GoldOBT",
        "args": {
            "gs://capstone-project-wzl-storage/gold/movie_analytics"
        }
    }
}

JOB_DICT = {
    "test": SPARK_JOB_TEST,
    "show": SPARK_JOB_SHOW_TABLE,
    "rl": SPARK_JOB_T_RL,
    "crm": SPARK_JOB_T_CMR,
    "up": SPARK_JOB_T_UP,
    "obt": SPARK_JOB_OBT
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=[CLOUD_PROVIDER, CLUSTER],
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")

    spark_custom_task = DataprocSubmitJobOperator(
        task_id="spark_custom_task",
        job=JOB_DICT[str(JOB_NAME_SELECTOR)],
        region=REGION,
        project_id=PROJECT_ID
    )

    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> spark_task >> end_workflow
