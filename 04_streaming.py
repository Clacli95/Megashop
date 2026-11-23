"""Mostra il totale cumulativo delle transazioni per regione."""
import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_locale import session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data_local/json"))
    parser.add_argument("--region-column", default="region")
    parser.add_argument("--once", action="store_true", help="Elabora i file presenti ed esce")
    args = parser.parse_args()
    args.input.mkdir(parents=True, exist_ok=True)

    spark = session("MegaShop Streaming")
    query = None
    try:
        schema = StructType([StructField(args.region_column, StringType())])
        sales = (
            spark.readStream.schema(schema)
            .option("pathGlobFilter", "*.json")
            .option("mode", "FAILFAST")
            .json(args.input.resolve().as_uri())
        )
        totals = (
            sales.select(F.col(args.region_column).alias("region"))
            .where(F.col("region").isNotNull())
            .groupBy("region").count().withColumnRenamed("count", "totale_transazioni")
        )
        writer = totals.writeStream.format("console").outputMode("complete").option("truncate", False)
        if args.once:
            writer = writer.trigger(availableNow=True)
        query = writer.start()
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

