from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, ORDER_SOURCE_FILES, ensure_output_dirs, read_order_events, require_source_data, spark_session, write_csv_dir, read_parquet, write_parquet

from pyspark.sql import functions as F


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab03StorageFormats")

    orders = (
        read_order_events(spark, [ORDER_SOURCE_FILES[0]])
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_time_ts"))
    )

    parquet_path = LAKE_DIR / "playground" / "orders_by_status_parquet"
    write_parquet(orders, parquet_path, mode="overwrite", partition_by="status")

    partition_counts = (
        read_parquet(spark, parquet_path)
        .groupBy("status")
        .count()
        .orderBy("status")
    )
    write_csv_dir(partition_counts, OUTPUT_DIR / "lab_03_partition_counts")

    print("LAB 03 COMPLETE")
    print(f"Wrote partitioned Parquet to {parquet_path}")
    print("Concepts: Parquet, partitionBy, reading back lake files.")
    spark.stop()


if __name__ == "__main__":
    main()
