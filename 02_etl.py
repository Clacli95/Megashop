"""Estrae, unisce e salva le vendite partizionate per anno."""
import argparse
from pathlib import Path

from pyspark.sql import functions as F

from spark_locale import session


def locate(root, explicit, pattern, folders):
    if explicit is not None:
        return [path.resolve().as_uri() for path in explicit]
    for folder in folders:
        paths = sorted((root / folder).glob(pattern))
        if paths:
            return [path.resolve().as_uri() for path in paths]
    raise FileNotFoundError(f"{pattern} assente in {root}, nelle cartelle {', '.join(folders)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=Path("dataset"))
    parser.add_argument("--transactions", type=Path, nargs="+")
    parser.add_argument("--products", type=Path)
    parser.add_argument("--regions", type=Path)
    parser.add_argument("--date-column", help="Colonna data da usare se manca year")
    parser.add_argument("--output", type=Path, default=Path("data_local/processed_sales"))
    args = parser.parse_args()

    root = args.dataset
    batches = locate(root, args.transactions, "transactions_batch_*.parquet",
                     ("transazioni_parquet", "parquet", "."))
    products = locate(root, [args.products] if args.products else None,
                      "products.parquet", ("anagrafiche", "parquet", "transazioni_parquet", "."))
    regions = locate(root, [args.regions] if args.regions else None,
                     "regions.parquet", ("anagrafiche", "parquet", "transazioni_parquet", "."))

    spark = session("MegaShop ETL")
    try:
        sales = spark.read.parquet(*batches)
        if "year" not in sales.columns:
            if not args.date_column:
                parser.error("Manca year: indica la colonna data con --date-column.")
            sales = sales.withColumn("year", F.year(F.col(args.date_column).cast("date")))
        product_dim = spark.read.parquet(*products).select("product_id", "category")
        region_dim = spark.read.parquet(*regions).select("region_id", "region_name")

        # Chiavi duplicate nelle anagrafiche gonfierebbero il fatturato.
        for dimension, key in [(product_dim, "product_id"), (region_dim, "region_id")]:
            if dimension.groupBy(key).count().where(F.col("count") > 1).take(1):
                raise ValueError(f"Chiave duplicata nell'anagrafica: {key}")

        clean = (
            sales.select("transaction_id", "product_id", "region_id", "amount", "year")
            .join(product_dim, "product_id", "inner")
            .join(region_dim, "region_id", "inner")
            .select(
                "transaction_id", "region_name", "category",
                F.col("amount").cast("decimal(20, 2)").alias("amount"),
                F.col("year").cast("int").alias("year"),
            )
            .dropna()
        )
        if not clean.take(1):
            raise ValueError("Nessuna transazione valida dopo i join e la pulizia.")
        clean.write.mode("errorifexists").partitionBy("year").parquet(args.output.resolve().as_uri())
        print(f"Vendite salvate in {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

