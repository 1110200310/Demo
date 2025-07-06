from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pendulum

# Cấu hình DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="postgres_daily_data_import",
    default_args=default_args,
    description="ETL tự động import dữ liệu từ Excel/CSV vào PostgreSQL hàng ngày",
    schedule_interval="0 10 * * *",  # Chạy lúc 10h sáng mỗi ngày
    start_date=days_ago(1),
    catchup=False,
    tags=["postgres", "etl", "daily"]
) as dag:

       run_etl_script = BashOperator(
        task_id="run_etl_import_to_postgres",
        bash_command="/usr/local/bin/python /opt/airflow/dags/data_import_job/import_to_postgres.py",
    )

