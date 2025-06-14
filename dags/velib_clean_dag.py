from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='velib_clean_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Nettoyage après récupération des données',
    tags=['velib', 'spark'],
) as dag:

    wait_getdata = ExternalTaskSensor(
        task_id='wait_getdata',
        external_dag_id='velib_getdata_dag',
        external_task_id=None,
        mode='poke',
        timeout=600,
        poke_interval=30
    )

    run_clean = DockerOperator(
        task_id='run_spark_clean',
        image='my-spark-custom',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/cleanData.py',
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

    wait_getdata >> run_clean
