from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from jobs.postgres_to_stage import export_postgres_to_stage
from jobs.stage_to_snowflake_dw import load_stage_to_snowflake_dw

with DAG(
    dag_id="goodgame_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 */2 * * *",
    catchup=False,
    tags=["etl", "spark", "snowflake"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_postgres_data",
        python_callable=export_postgres_to_stage,
    )

    load_task = PythonOperator(
        task_id="load_incremental_dw",
        python_callable=load_stage_to_snowflake_dw,
    )

    extract_task >> load_task
