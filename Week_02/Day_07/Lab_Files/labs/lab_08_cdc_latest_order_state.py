from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, deduplicate_order_events, ensure_output_dirs, latest_order_state, require_source_data, spark_session, write_csv_dir, read_parquet, write_parquet


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab08CdcLatestState")

    valid = read_parquet(spark, LAKE_DIR / "silver" / "orders_valid")
    deduplicated = deduplicate_order_events(valid)
    write_parquet(deduplicated, LAKE_DIR / "silver" / "orders_deduplicated", mode="overwrite")
    current = latest_order_state(deduplicated)
    current_path = LAKE_DIR / "silver" / "orders_current"
    write_parquet(current, current_path, mode="overwrite")

    write_csv_dir(
        deduplicated.select("order_id", "event_id", "status", "amount", "currency", "event_time_ts").orderBy(
            "order_id", "event_time_ts", "event_id"
        ),
        OUTPUT_DIR / "lab_08_deduplicated_events_preview",
    )
    write_csv_dir(
        current.select("order_id", "event_id", "status", "amount", "currency", "event_time_ts").orderBy("order_id"),
        OUTPUT_DIR / "lab_08_orders_current_preview",
    )

    print("LAB 08 COMPLETE")
    print(f"Valid rows before deduplication: {valid.count()}")
    print(f"Rows after explicit Silver deduplication: {deduplicated.count()}")
    print(f"Current order rows: {current.count()}")
    print("Concepts: explicit deduplication, CDC, window functions, latest-state tables.")
    spark.stop()


if __name__ == "__main__":
    main()
