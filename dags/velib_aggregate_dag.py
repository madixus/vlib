from airflow import DAG # type: ignore
from airflow.providers.docker.operators.docker import DockerOperator # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor # type: ignore
from datetime import datetime, timedelta
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
    dag_id='velib_aggregate_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG qui exécute aggregateData.py dans le container Spark',
    tags=['velib', 'spark', 'etl'],
) as dag:
    
    wait_getdata = ExternalTaskSensor(
        task_id='wait_clean',
        external_dag_id='velib_clean_dag',
        external_task_id=None,
        mode='poke',
        timeout=600,
        poke_interval=30
    )

    

    run_aggregate = DockerOperator(
        task_id='run_spark_aggregate',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/aggregateData.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[
            {
                'source':local_jobs_path,
                'target': '/opt/spark-jobs',
                'type': 'bind'
            }
        ]
    )

    run_aggregate
