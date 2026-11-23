"""Totale vendite con Pandas e media per pagamento con Dask."""
import argparse
from pathlib import Path

import dask.dataframe as dd
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-dir", type=Path, default=Path("dataset/transazioni_json"))
    parser.add_argument("--array", action="store_true", help="I file contengono array JSON")
    args = parser.parse_args()
    files = sorted(args.json_dir.glob("*.json"))
    if not files:
        parser.error(f"Nessun file .json in {args.json_dir}")

    total = 0.0
    for path in files:
        batch = pd.read_json(path, lines=not args.array)
        total += pd.to_numeric(batch["amount"], errors="raise").sum()
        del batch
    print(f"Totale generale (Pandas): {total:.2f}")

    sales = dd.read_json(
        (args.json_dir.resolve() / "*.json").as_posix(),
        orient="records",
        lines=not args.array,
        encoding="utf-8",
        dtype={"amount": "float64", "payment_type": "string"},
    )
    averages = sales.groupby("payment_type")["amount"].mean().compute().sort_index()
    print("Importo medio per metodo di pagamento (Dask)")
    print(averages.to_string())


if __name__ == "__main__":
    main()

