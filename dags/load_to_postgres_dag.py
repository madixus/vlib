from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='velib_load_postgres_dag',
    default_args=default_args,
    description='Charger les données finales depuis HDFS vers PostgreSQL',
    schedule_interval='@daily',  # ou @once
    catchup=False
)

load_to_postgres = BashOperator(
    task_id='spark_submit_to_postgres',
    bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --jars /extra-jars/postgresql-42.7.5.jar \
        /opt/spark-jobs/loadDataPostgresql.py
    """,
    dag=dag
)
