from airflow import DAG # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor # type: ignore
from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="velib_master_dag",
    description="Orchestre l'ensemble du pipeline Vélib (getdata, getstation, clean, aggregate, load, load_postgres)",
    schedule_interval="*/20 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["velib", "orchestration"]
) as dag:

    # --- Déclenchement des DAGs initiaux ---
    trigger_getdata = TriggerDagRunOperator(
        task_id="trigger_getdata",
        trigger_dag_id="velib_getdata_dag",
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
        wait_for_completion=False
    )

    # trigger_getstation = TriggerDagRunOperator(
    #     task_id="trigger_getstation",
    #     trigger_dag_id="velib_getStation_dag",
    #     execution_date="{{ execution_date }}",
    #     reset_dag_run=True,
    #     wait_for_completion=False
    # )

    # --- Attente de fin des DAGs initiaux ---
    wait_getdata = ExternalTaskSensor(
        task_id="wait_getdata",
        external_dag_id="velib_getdata_dag",
        external_task_id="run_spark_getdata",  # adapte si le task_id est différent
        mode="poke",
        poke_interval=60,
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    # wait_getstation = ExternalTaskSensor(
    #     task_id="wait_getstation",
    #     external_dag_id="velib_getStation_dag",
    #     external_task_id="run_spark_getStation",  
    #     mode="poke",
    #     poke_interval=60,
    #     timeout=600,
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"]
    # )

    # --- Déclenchement du nettoyage ---
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_clean",
        trigger_dag_id="velib_clean_dag",
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
        wait_for_completion=False
    )

    wait_clean = ExternalTaskSensor(
        task_id="wait_clean",
        external_dag_id="velib_clean_dag",
        external_task_id="run_spark_clean",
        mode="poke",
        poke_interval=60,
        timeout=720,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    # --- Agrégation ---
    trigger_aggregate = TriggerDagRunOperator(
        task_id="trigger_aggregate",
        trigger_dag_id="velib_aggregate_dag",
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
        wait_for_completion=False
    )

    wait_agg = ExternalTaskSensor(
        task_id="wait_agg",
        external_dag_id="velib_aggregate_dag",
        external_task_id="run_spark_aggregate",
        mode="poke",
        poke_interval=60,
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    # --- Chargement dans HDFS ---
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="velib_load_dag",
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
        wait_for_completion=False
    )

    wait_load = ExternalTaskSensor(
        task_id="wait_load",
        external_dag_id="velib_load_dag",
        external_task_id="run_spark_load",
        mode="poke",
        poke_interval=60,
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    # --- Chargement dans PostgreSQL ---
    trigger_postgres_load = TriggerDagRunOperator(
        task_id="trigger_postgres_load",
        trigger_dag_id="velib_load_postgres_dag",
        execution_date="{{ execution_date }}",
        reset_dag_run=True,
        wait_for_completion=False
    )

    # --- Orchestration complète ---
    trigger_getdata >> wait_getdata
    # trigger_getstation >> wait_getstation

    # [wait_getdata, wait_getstation] >> trigger_clean >> wait_clean
    wait_getdata >> trigger_clean >> wait_clean
    wait_clean >> trigger_aggregate >> wait_agg
    wait_agg >> trigger_load >> wait_load >> trigger_postgres_load
