from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    write_csv(
        DATA_DIR / "first_pipeline_source.csv",
        ["customer_id", "customer_name", "city"],
        [
            [1, "Ada Lovelace", "London"],
            [2, "Grace Hopper", "New York"],
            [3, "Katherine Johnson", "White Sulphur Springs"],
        ],
    )

    write_csv(
        DATA_DIR / "customers.csv",
        ["customer_id", "name", "email", "phone", "country", "order_amount"],
        [
            [101, "Alice Meyer", "ALICE@EXAMPLE.COM", "0049123456789", "de", 100.00],
            [102, "Bob Singh", "bob@example.com", "00919876543210", "in", 50.00],
            [103, "Charlie Rossi", "charlie@example.com", "0039061234567", "it", 30.00],
            [101, "Alice Meyer", "ALICE@EXAMPLE.COM", "0049123456789", "de", 100.00],
            [104, "Dana Null", "", "0015550104", "us", 75.00],
        ],
    )

    write_csv(
        DATA_DIR / "orders.csv",
        ["order_id", "customer_id", "product_id", "status", "amount"],
        [
            [1, 101, "P1", "NEW", 100.00],
            [2, 102, "P2", "NEW", 50.00],
            [3, 103, "P3", "CANCELED", 30.00],
            [4, 101, "P2", "NEW", 80.00],
        ],
    )

    write_csv(
        DATA_DIR / "products.csv",
        ["product_id", "product_name", "category"],
        [
            ["P1", "Laptop", "Electronics"],
            ["P2", "Smartphone", "Electronics"],
            ["P3", "Headphones", "Accessories"],
        ],
    )

    print(f"Wrote lab data to {DATA_DIR}")


if __name__ == "__main__":
    main()
