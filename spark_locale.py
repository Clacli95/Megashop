"""Configurazione condivisa per l'esecuzione locale."""
from pyspark.sql import SparkSession


def session(name):
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

