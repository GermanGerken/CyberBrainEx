# import datetime
import os
import re
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}



# path = f"data/{datetime.date.today()}.logsite.csv.gz"

with DAG(
        dag_id="sensor_spark_pipeline_dag",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['cb-ex'],
) as dag:
    dag.doc_md = """
        Dag will go as many times as numbers of files in folder
        With decorator @task we convert function spark_pipline in 
        airflow operator. The function will work with the filename passed in to run on that file.
        """


    def spark_pipeline():
        dirname = '/Users/germangerken/airflow/data'  # Dir where our files stored
        files = os.listdir(dirname)  # Get the list of files in Dir

        regex = re.compile(r".*.logsite.csv.gz")
        filtered = [i for i in files if regex.match(i)]

        spark = SparkSession.builder.appName("CyberBrain").getOrCreate()
        for file in filtered:
            path = f"data/{file}"
            df = spark.read.option("header", "true").csv(path)
            df = df.withColumn("date", F.to_date(F.col("datetime")))
            output = (
                df
                .filter(F.col('user_id') != '0')
                .groupBy("date")
                .agg(
                    F.countDistinct("user_id").alias("count_users"),
                    F.countDistinct("google_id_first").alias("count_google_id"),
                    F.countDistinct("yandex_id_first").alias("count_yandex_id")
                )
            )
            output.write.mode("append").format("csv").save("output.csv")

    monitoring_task = FileSensor(
            task_id='monitoring_task',
            filepath='*.logsite.csv.gz',
            fs_conn_id='data',
            poke_interval=10
        )

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=spark_pipeline,
    )

    monitoring_task >> extract_data_task
