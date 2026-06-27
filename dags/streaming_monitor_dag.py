"""Airflow DAG: hourly rollup of streamed events + dbt build + data freshness check."""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "aditya",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="realtime_events_hourly",
    description="Hourly rollup of streamed Kafka events → Delta → dbt marts",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["streaming", "kafka", "spark", "delta", "dbt"],
) as dag:

    rollup = BashOperator(
        task_id="hourly_rollup",
        bash_command="cd /opt/project && python -m src.batch.hourly_rollup",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/project/dbt && dbt build --profiles-dir .",
    )

    freshness_check = BashOperator(
        task_id="freshness_check",
        bash_command=(
            "cd /opt/project && python -c \""
            "import sys; sys.path.append('.'); "
            "from src.config import spark_session, EVENTS_RAW; "
            "from pyspark.sql import functions as F; "
            "s=spark_session('freshness'); "
            "df=s.read.format('delta').load(EVENTS_RAW); "
            "latest=df.agg(F.max('event_ts')).collect()[0][0]; "
            "print('latest event_ts:', latest); s.stop()\""
        ),
    )

    rollup >> dbt_build >> freshness_check
