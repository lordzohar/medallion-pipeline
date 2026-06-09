from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, STATE_DIR, cleaned_orders, deduplicate_order_events, enriched_orders, ensure_output_dirs, gold_frames, latest_order_state, quality_checked_orders, read_order_events, require_source_data, reset_dir, spark_session, with_bronze_metadata, write_csv_dir, write_json_report, read_parquet, write_parquet

from pyspark.sql import functions as F


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab12FullMedallionPipeline")

    reset_dir(LAKE_DIR / "bronze")
    reset_dir(LAKE_DIR / "silver")
    reset_dir(LAKE_DIR / "gold")
    reset_dir(LAKE_DIR / "quarantine")

    bronze = with_bronze_metadata(read_order_events(spark), "orchestrated-run-001")
    bronze_path = LAKE_DIR / "bronze" / "orders_raw"
    write_parquet(bronze, bronze_path, mode="overwrite")

    checked = quality_checked_orders(cleaned_orders(read_parquet(spark, bronze_path)))
    valid = checked.filter(F.col("is_valid"))
    invalid = checked.filter(~F.col("is_valid"))
    write_parquet(valid, LAKE_DIR / "silver" / "orders_valid", mode="overwrite")
    write_parquet(invalid, LAKE_DIR / "quarantine" / "orders_invalid", mode="overwrite")
    write_csv_dir(
        checked.groupBy("is_valid").count().withColumnRenamed("count", "row_count").orderBy("is_valid"),
        OUTPUT_DIR / "lab_12_quality_control_table",
    )
    write_csv_dir(
        invalid.select(
            "event_id",
            "order_id",
            "status",
            "amount",
            "currency",
            F.concat_ws("|", F.col("quality_errors")).alias("quality_errors"),
        ).orderBy("event_id"),
        OUTPUT_DIR / "lab_12_quarantine_records",
    )

    deduplicated = deduplicate_order_events(valid)
    write_parquet(deduplicated, LAKE_DIR / "silver" / "orders_deduplicated", mode="overwrite")

    current = latest_order_state(deduplicated)
    write_parquet(current, LAKE_DIR / "silver" / "orders_current", mode="overwrite")

    enriched = enriched_orders(spark, current)
    write_parquet(enriched, LAKE_DIR / "silver" / "orders_enriched", mode="overwrite")

    frames = gold_frames(enriched)
    for name, frame in frames.items():
        write_parquet(frame, LAKE_DIR / "gold" / name, mode="overwrite")
        write_csv_dir(frame, OUTPUT_DIR / f"lab_12_{name}")

    manifest = {
        "bronze_rows": bronze.count(),
        "silver_valid_rows": valid.count(),
        "silver_deduplicated_rows": deduplicated.count(),
        "quarantine_rows": invalid.count(),
        "current_order_rows": current.count(),
        "enriched_rows": enriched.count(),
        "gold_tables": sorted(frames.keys()),
        "expected_learning_path": "Spark basics -> Bronze raw -> Silver quality/dedup/current/enriched -> Gold KPIs",
    }
    write_json_report(STATE_DIR / "lab_12_run_manifest.json", manifest)

    assert manifest["bronze_rows"] == 12, manifest
    assert manifest["silver_valid_rows"] == 10, manifest
    assert manifest["silver_deduplicated_rows"] == 9, manifest
    assert manifest["quarantine_rows"] == 2, manifest
    assert manifest["current_order_rows"] == 7, manifest

    print("LAB 12 COMPLETE")
    print(manifest)
    print("Concepts: orchestration, layer contracts, validation gates, production-style manifest.")
    spark.stop()


if __name__ == "__main__":
    main()
