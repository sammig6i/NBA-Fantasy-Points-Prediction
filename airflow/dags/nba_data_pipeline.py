import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 1, 1),
  "email_on_failure": False,
  "email_on_retry": False,
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}

docker_env = {
  "DB_USER": os.getenv("DB_USER"),
  "DB_PASSWORD": os.getenv("DB_PASSWORD"),
  "DB_HOST": os.getenv("DB_HOST"),
  "DB_PORT": os.getenv("DB_PORT"),
  "DB_NAME": os.getenv("DB_NAME"),
  "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
  "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
  "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT"),
  "MINIO_BUCKET_NAME": os.getenv("MINIO_BUCKET_NAME"),
  "MINIO_SECURE": os.getenv("MINIO_SECURE"),
}


@dag(
  default_args=default_args,
  description="NBA data ingestion and processing pipeline",
  schedule_interval=timedelta(days=1),
  catchup=False,
  tags=["nba", "data-pipeline"],
)
def nba_data_pipeline():
  data_ingestion = DockerOperator(
    task_id="run_data_ingestion",
    image="us-west1-docker.pkg.dev/nba-fantasy-ml/nba-data-pipeline-repo/data-ingestion:latest",
    command="python data_ingestion/main.py",
    network_mode="nba_network",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    environment=docker_env,
  )

  data_processing = DockerOperator(
    task_id="run_data_processing",
    image="us-west1-docker.pkg.dev/nba-fantasy-ml/nba-data-pipeline-repo/data-processing:latest",
    command="python data_processing/main.py",
    network_mode="nba_network",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    environment=docker_env,
  )

  data_ingestion >> data_processing


nba_data_pipeline()
