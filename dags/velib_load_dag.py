from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from docker.types import Mount

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='velib_load_dag',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['velib']
) as dag:

    wait_agg = ExternalTaskSensor(
        task_id='wait_agg',
        external_dag_id='velib_aggregate_dag',
        external_task_id='run_spark_aggregate',
        execution_date_fn=lambda dt: dt,
        mode='poke',
        poke_interval=60,
        timeout=600
    )

    run_spark_load = DockerOperator(
        task_id='run_spark_load',
        image='my-spark-custom',
        api_version='auto',
        auto_remove=True,
        command='spark-submit --master spark://spark-master:7077 /opt/spark-jobs/loadData.py',
        docker_url='tcp://host.docker.internal:2375',  
        network_mode='my-network',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='/c/Users/USER/Desktop/mspr/vlib/Jobs',
                target='/opt/spark-jobs',
                type='bind'
            )
        ]
    )

    wait_agg >> run_spark_load
