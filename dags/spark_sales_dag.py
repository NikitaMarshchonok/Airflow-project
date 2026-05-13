from datetime import timedelta
import pendulum 
import subprocess
from airflow.decorators import dag, task

default_args = {
    'owner':'data_team',
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

@dag(
    dag_id='spark_sales_pipeline',
    schedule='0 3 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=['spark', 'sales'],
)

def spark_sales_pipeline():

    @task()
    def run_spark_job():
        result = subprocess.run(
            [
                "/opt/spark/bin/spark-submit",
                '--master', 'spark://spark-master:7077',
                'opt/spark/jobs/sales_analysis.py',

            ],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode !=0:
            raise Exception(f"Spark job fall: {result.stderr}")

    run_spark_job()

dag_instance = spark_sales_pipeline()

        