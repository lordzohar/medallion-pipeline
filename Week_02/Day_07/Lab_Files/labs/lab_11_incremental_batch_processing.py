from __future__ import annotations

import json
from pathlib import Path

from day7_common import LAKE_DIR, ORDER_SOURCE_FILES, OUTPUT_DIR, STATE_DIR, cleaned_orders, deduplicate_order_events, enriched_orders, ensure_output_dirs, gold_frames, latest_order_state, metric_table, quality_checked_orders, read_order_events, require_source_data, spark_session, with_bronze_metadata, write_csv_dir, write_json_report, read_parquet, write_parquet

from pyspark.sql import functions as F


def load_manifest(path: Path) -> dict[str, object]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"processed_files": [], "batches": []}


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab11IncrementalProcessing")

    incremental_lake = LAKE_DIR / "incremental"
    incremental_lake.mkdir(parents=True, exist_ok=True)
    manifest_path = STATE_DIR / "lab_11_incremental_manifest.json"

    manifest = load_manifest(manifest_path)
    bronze_path = incremental_lake / "bronze" / "orders_raw"
    processed_this_run = 0

    for index, source_file in enumerate(ORDER_SOURCE_FILES, start=1):
        processed_files = set(manifest["processed_files"])
        if source_file.name in processed_files:
            print(f"Skipping already processed file: {source_file.name}")
            continue

        batch_id = f"incremental-batch-{index:02d}"
        batch = with_bronze_metadata(read_order_events(spark, [source_file]), batch_id)
        write_parquet(batch, bronze_path, mode="append")
        row_count = batch.count()
        manifest["processed_files"].append(source_file.name)
        manifest["batches"].append({"batch_id": batch_id, "source_file": source_file.name, "rows": row_count})
        write_json_report(manifest_path, manifest)
        processed_this_run += 1
        print(f"Processed {source_file.name}: {row_count} Bronze rows")

    if not bronze_path.exists():
        raise FileNotFoundError(
            "No incremental Bronze data found. Run the lab once with source files available, "
            "or delete the stale manifest and rerun."
        )

    bronze = read_parquet(spark, bronze_path)
    checked = quality_checked_orders(cleaned_orders(bronze))
    valid = checked.filter(F.col("is_valid"))
    deduplicated = deduplicate_order_events(valid)
    current = latest_order_state(deduplicated)
    enriched = enriched_orders(spark, current)

    write_parquet(valid, incremental_lake / "silver" / "orders_valid", mode="overwrite")
    write_parquet(deduplicated, incremental_lake / "silver" / "orders_deduplicated", mode="overwrite")
    write_parquet(current, incremental_lake / "silver" / "orders_current", mode="overwrite")
    write_parquet(enriched, incremental_lake / "silver" / "orders_enriched", mode="overwrite")

    for name, frame in gold_frames(enriched).items():
        write_parquet(frame, incremental_lake / "gold" / name, mode="overwrite")

    summary = metric_table(
        spark,
        [
            ("bronze_rows", bronze.count()),
            ("silver_valid_rows", valid.count()),
            ("silver_deduplicated_rows", deduplicated.count()),
            ("current_order_rows", current.count()),
            ("processed_files", len(manifest["processed_files"])),
            ("files_processed_this_run", processed_this_run),
        ],
    )
    write_csv_dir(summary, OUTPUT_DIR / "lab_11_incremental_summary")

    print("LAB 11 COMPLETE")
    print(f"Manifest: {manifest_path}")
    print("Concepts: file checkpoints, idempotent ingestion, incremental medallion updates.")
    spark.stop()


if __name__ == "__main__":
    main()
