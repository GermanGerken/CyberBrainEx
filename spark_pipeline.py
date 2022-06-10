from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def extract_data() -> DataFrame:
    spark = SparkSession.builder.appName("CyberBrain").getOrCreate()
    path = "airflow/data/2022-05-17.logsite.csv.gz"
    return spark.read.option("header", "true").csv(path)


def transform_data(df: DataFrame) -> DataFrame:
    df = df.withColumn("date", F.to_date(F.col("datetime")))
    output = (
        df
        .filter(F.col('user_id') != '0')
        .groupBy("date")
        .agg(
            F.countDistinct("user_id").alias("count_users"),
            F.countDistinct("google_id_first").alias("count_google_id"),
            F.countDistinct("yandex_id_first").alias("count_yandex_id"),
        )
    )
    return output


def save_data(df: DataFrame) -> None:
    df.write.mode("append").option('header', 'true').format("csv").save("output.csv")


def main():

    df = extract_data()
    output = transform_data(df)
    save_data(output)

main()