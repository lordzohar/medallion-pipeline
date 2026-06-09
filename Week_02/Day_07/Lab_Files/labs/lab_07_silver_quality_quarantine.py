from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, cleaned_orders, ensure_output_dirs, quality_checked_orders, read_bronze_orders, require_source_data, spark_session, write_csv_dir, write_parquet

from pyspark.sql import functions as F


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab07QualityQuarantine")

    checked = quality_checked_orders(cleaned_orders(read_bronze_orders(spark)))
    valid = checked.filter(F.col("is_valid"))
    invalid = checked.filter(~F.col("is_valid"))

    valid_path = LAKE_DIR / "silver" / "orders_valid"
    quarantine_path = LAKE_DIR / "quarantine" / "orders_invalid"
    write_parquet(valid, valid_path, mode="overwrite")
    write_parquet(invalid, quarantine_path, mode="overwrite")

    control_table = checked.groupBy("is_valid").count().withColumnRenamed("count", "row_count").orderBy("is_valid")
    write_csv_dir(control_table, OUTPUT_DIR / "lab_07_quality_control_table")
    write_csv_dir(
        invalid.select(
            "event_id",
            "order_id",
            "status",
            "amount",
            "currency",
            F.concat_ws("|", F.col("quality_errors")).alias("quality_errors"),
        ).orderBy("event_id"),
        OUTPUT_DIR / "lab_07_quarantine_records",
    )

    print("LAB 07 COMPLETE")
    print(f"Valid rows: {valid.count()}; quarantined rows: {invalid.count()}")
    print("Concepts: data contracts, quarantine design, quality error arrays.")
    spark.stop()


if __name__ == "__main__":
    main()
