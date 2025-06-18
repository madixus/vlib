from airflow import DAG  # type: ignore
from airflow.providers.docker.operators.docker import DockerOperator  # type: ignore
from datetime import datetime, timedelta
from docker.types import Mount  # type: ignore
from dotenv import load_dotenv
import os

load_dotenv()

local_jobs_path = os.getenv("LOCAL_JOBS_PATH")

if not local_jobs_path:
    raise ValueError("❌ La variable LOCAL_JOBS_PATH est manquante. Ajoutez-la dans le fichier .env")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='velib_getdata_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG qui exécute getData.py dans le container Spark',
    tags=['velib', 'spark', 'docker'],
) as dag:

    run_getdata = DockerOperator(
        task_id='run_spark_getdata',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/getDataVelo.py',
        docker_url="tcp://host.docker.internal:2375",  # Pour Windows avec Docker Desktop
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=local_jobs_path,
                target='/opt/spark-jobs',
                type='bind'
            )
        ]
    )

    run_getdata
