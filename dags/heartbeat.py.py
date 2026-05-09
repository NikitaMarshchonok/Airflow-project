from datetime import timedelta
import pendulum
import psycopg2
from airflow.decorators import dag, task

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="heartbeat",
    schedule="*/5 * * * *",  # каждые 5 минут
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
)
def heartbeat_pipeline():

    @task()
    def write_heartbeat():
        conn = psycopg2.connect(
            host="postgres",
            database="pipeline_db",
            user="airflow",
            password="airflow",
            port=5432,        # ← port, не post
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO heartbeat (checked_at) VALUES (%s)",
                    (pendulum.now("UTC"),)
                )
        conn.close()
        print("Heartbeat записан")

dag_instance = heartbeat_pipeline()