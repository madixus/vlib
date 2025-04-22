from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='velib_getstation_dag',
    default_args=default_args,
    schedule_interval='@weekly',  # ou '@daily'
    catchup=False,
    description='DAG qui exécute getStationData.py dans le container Spark',
    tags=['velib', 'spark', 'docker'],
) as dag:

    run_getstationdata = DockerOperator(
        task_id='run_spark_getSationData',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/getStationData.py',
        docker_url="tcp://host.docker.internal:2375",
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[
            {
                'source': 'C:/Users/USER/Desktop/mspr/vlib/Jobs',
                'target': '/opt/spark-jobs',
                'type': 'bind'
            }
        ]
    )

    run_getstationdata
