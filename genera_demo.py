"""Crea un dataset sintetico per provare il progetto senza i dati del docente."""
import argparse
import json
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("demo_dataset"))
    args = parser.parse_args()
    root = args.output
    root.mkdir(parents=True, exist_ok=False)
    logs = root / "transazioni_json"
    parquet = root / "transazioni_parquet"
    dimensions = root / "anagrafiche"
    for folder in (logs, parquet, dimensions):
        folder.mkdir()

    pd.DataFrame({"product_id": [1, 2], "category": ["Elettronica", "Casa"]}).to_parquet(
        dimensions / "products.parquet", index=False
    )
    pd.DataFrame({"region_id": [1, 2], "region_name": ["Nord", "Sud"]}).to_parquet(
        dimensions / "regions.parquet", index=False
    )
    rows = [
        (1, 1, 1, 120.0, 2024, "carta", "Nord"),
        (2, 2, 2, 40.0, 2024, "contanti", "Sud"),
        (3, 1, 1, 80.0, 2025, "carta", "Nord"),
        (4, 2, 2, 60.0, 2025, "contanti", "Sud"),
        (5, 2, 1, 30.0, 2025, "carta", "Nord"),
        (6, 1, 2, 170.0, 2025, "contanti", "Sud"),
    ]
    fields = ["transaction_id", "product_id", "region_id", "amount", "year", "payment_type", "region"]
    for number, start in enumerate(range(0, len(rows), 3), 1):
        records = [dict(zip(fields, row)) for row in rows[start:start + 3]]
        with (logs / f"batch_{number}.json").open("w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record) + "\n")
        pd.DataFrame(records).to_parquet(
            parquet / f"transactions_batch_{number}.parquet", index=False
        )
    print(f"Dataset dimostrativo in {root}")


if __name__ == "__main__":
    main()

