from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def extract_data() -> DataFrame:
    """Start Spark Session
       Extract the DATA from file in the folder in DIR - Path

            Returns:
                DataFrame: data from the file
    """

    spark = SparkSession.builder.appName("CyberBrain").getOrCreate()
    path = "airflow/data/2022-05-17.logsite.csv.gz"
    return spark.read.option("header", "true").csv(path)


def transform_data(df: DataFrame) -> DataFrame:
    """Get from DataFrame distinct number of users and distinct number of Google and Yandex IDs for specific date
            Args:
                df (DataFrame): The initial DataFrame

            Returns:
                DataFrame: Output
    """

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
    """Save the given DataFrame in to folder in csv format with header
                Args:
                    df (DataFrame): The result DataFrame

                Returns:
                    None
    """

    df.write.mode("append").option('header', 'true').format("csv").save("output.csv")


def main():
    df = extract_data()
    output = transform_data(df)
    save_data(output)


main()
