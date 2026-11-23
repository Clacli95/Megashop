"""Calcola il fatturato per categoria e salva un grafico a barre."""
import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
from pyspark.sql import functions as F

from spark_locale import session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data_local/processed_sales"))
    parser.add_argument("--output", type=Path, default=Path("fatturato_per_categoria.png"))
    args = parser.parse_args()

    spark = session("MegaShop Reporting")
    try:
        clean = spark.read.parquet(args.input.resolve().as_uri())
        totals = (
            clean.groupBy("category")
            .agg(F.sum("amount").alias("fatturato"))
            .orderBy(F.desc("fatturato"), "category")
            .toPandas()
        )
    finally:
        spark.stop()
    if totals.empty:
        raise ValueError("Il Parquet elaborato non contiene vendite.")

    print(totals.to_string(index=False))
    fig, ax = plt.subplots(figsize=(max(8, len(totals) * 0.8), 5), layout="constrained")
    ax.bar(totals["category"], totals["fatturato"].astype(float), color="#176b75")
    ax.set(title="Fatturato totale per categoria", xlabel="Categoria", ylabel="Fatturato")
    ax.tick_params(axis="x", labelrotation=30)
    ax.set_axisbelow(True)
    ax.grid(axis="y", alpha=0.25)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=160)
    plt.close(fig)
    print(f"Grafico salvato in {args.output}")


if __name__ == "__main__":
    main()

