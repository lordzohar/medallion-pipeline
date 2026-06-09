from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, cleaned_orders, ensure_output_dirs, read_bronze_orders, require_source_data, spark_session, write_csv_dir, write_parquet


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab06SilverCleaning")

    clean_candidate = cleaned_orders(read_bronze_orders(spark))
    silver_path = LAKE_DIR / "silver" / "orders_clean_candidate"
    write_parquet(clean_candidate, silver_path, mode="overwrite")

    preview = clean_candidate.select(
        "event_id", "order_id", "customer_id", "product_id", "status", "amount", "currency", "event_time_ts"
    ).orderBy("event_id")
    write_csv_dir(preview, OUTPUT_DIR / "lab_06_clean_candidate_preview")

    print("LAB 06 COMPLETE")
    print(f"Clean candidate rows: {clean_candidate.count()}")
    print("Concepts: type casting, timestamp parsing, standardizing codes and strings.")
    spark.stop()


if __name__ == "__main__":
    main()
