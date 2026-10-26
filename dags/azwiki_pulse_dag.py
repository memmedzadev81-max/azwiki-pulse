"""Apache Airflow DAG: micro-batch the live stream every 15 minutes.

    stream (14 min) >> process (silver -> gold -> quality)

The checkpoint makes each stream task continue exactly where the previous one
stopped, and the file manifest makes `process` pick up only new bronze files.
Note: Airflow does not run natively on Windows; use WSL2 or Docker.
"""
import sys
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:  # Airflow 3.x
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="azwiki_pulse",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Baku"),
    catchup=False,
    max_active_runs=1,          # never two consumers on the same checkpoint
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["streaming", "wikipedia", "scd2", "portfolio"],
)
def azwiki_pulse():

    @task(execution_timeout=pendulum.duration(minutes=15))
    def stream() -> dict:
        from pulse.ingest import consume
        return consume(minutes=14)

    @task
    def process(_ingest_stats: dict) -> dict:
        from pulse.process import process as run_process
        return run_process()

    process(stream())


azwiki_pulse()
