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
    def spark_pipeline():
        spark = SparkSession.builder.appName("CyberBrain").getOrCreate()
        path = "data/2022-05-17.logsite.csv.gz"
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
        output.write.mode("overwrite").format("csv").save("output.csv")


    spark_pipeline_task = PythonOperator(
        task_id='spark_pipeline_task',
        python_callable=spark_pipeline,
    )

