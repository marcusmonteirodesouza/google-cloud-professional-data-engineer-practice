from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def main():
    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

    mnm_dataset_url = "https://raw.githubusercontent.com/databricks/LearningSparkV2/master/chapter2/py/src/data/mnm_dataset.csv"

    spark.sparkContext.addFile(mnm_dataset_url)

    mnm_df = spark.read.csv(
        f"file://{SparkFiles.get('mnm_dataset.csv')}", header=True, inferSchema=True
    )

    count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    count_mnm_df.show()

    print(f"Total rows: {count_mnm_df.count()}")

    ca_count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    ca_count_mnm_df.show()

    spark.stop()
