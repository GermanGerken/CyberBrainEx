from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
        dag_id="spark_pipeline_dag",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['cb-ex'],
) as dag:
    def extract_data(**kwargs):
        spark = SparkSession.builder.appName("CyberBrain").getOrCreate()
        path = "data/2022-05-17.logsite.csv.gz"
        return spark.read.option("header", "true").csv(path)


    def transform_data(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_data_task')
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
        return output


    def save_data(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='transform_data_task')
        df.write.mode("overwrite").format("csv").save("output.csv")


    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data,
    )

    save_data_task = PythonOperator(
        task_id='save_data_task',
        python_callable=save_data,
    )

    extract_data_task >> transform_data_task >> save_data_task
