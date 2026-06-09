from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, clean_lab_dir, ensure_output_dirs, read_order_events, require_source_data, spark_session, with_bronze_metadata, write_csv_dir, read_parquet, write_parquet


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab04BronzeRawIngestion")

    bronze_path = clean_lab_dir(LAKE_DIR / "bronze" / "orders_raw")
    bronze = with_bronze_metadata(read_order_events(spark), "manual-full-load-001")
    write_parquet(bronze, bronze_path, mode="overwrite")

    by_file = (
        read_parquet(spark, bronze_path)
        .groupBy("_source_file")
        .count()
        .orderBy("_source_file")
    )
    write_csv_dir(by_file, OUTPUT_DIR / "lab_04_bronze_counts_by_file")

    print("LAB 04 COMPLETE")
    print(f"Bronze rows written: {bronze.count()}")
    print(f"Bronze path: {bronze_path}")
    print("Concepts: append-only raw zone, source metadata, ingestion batch id.")
    spark.stop()


if __name__ == "__main__":
    main()
