from airflow import DAG  # type: ignore
from airflow.providers.docker.operators.docker import DockerOperator  # type: ignore
from datetime import datetime, timedelta
from docker.types import Mount  # type: ignore
import os
from dotenv import load_dotenv

load_dotenv()
local_jobs_path = os.getenv("LOCAL_Validate_PATH")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='velib_validate_dag',
    default_args=default_args,
    schedule_interval=None,  # Ne tourne pas automatiquement
    catchup=False,
    description='Validation des données Vélib via PySpark',
    tags=['velib', 'validation', 'spark'],
) as dag:

    validate_availability = DockerOperator(
        task_id='validate_availability_data',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/validate_velib_data.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[Mount(source=local_jobs_path, target='/opt/spark-jobs', type='bind')]
    )

    validate_stations = DockerOperator(
        task_id='validate_station_data',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/validate_velib_stations.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[Mount(source=local_jobs_path, target='/opt/spark-jobs', type='bind')]
    )

    validate_cleaned_data = DockerOperator(
        task_id='validate_cleaned_data',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/validate_cleaned_data.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[Mount(source=local_jobs_path, target='/opt/spark-jobs', type='bind')]
    )

    validate_aggregate_data = DockerOperator(
        task_id='validate_aggregate_data',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/validate_aggregate_data.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[Mount(source=local_jobs_path, target='/opt/spark-jobs', type='bind')]
    )

    validate_loaded_data = DockerOperator(
        task_id='validate_loaded_data',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/validate_loaded_data.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[Mount(source=local_jobs_path, target='/opt/spark-jobs', type='bind')]
    )

    validate_postgres_data = DockerOperator(
        task_id='validate_postgres_data',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/validate_postgres_data.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[Mount(source=local_jobs_path, target='/opt/spark-jobs', type='bind')]
    )

        # Dépendances
    [validate_availability, validate_stations] >> validate_cleaned_data
    validate_cleaned_data >> validate_aggregate_data >> validate_loaded_data
    validate_loaded_data >> validate_postgres_data

