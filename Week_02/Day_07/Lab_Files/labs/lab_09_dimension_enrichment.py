from __future__ import annotations

from day7_common import LAKE_DIR, OUTPUT_DIR, enriched_orders, ensure_output_dirs, require_source_data, spark_session, write_csv_dir, read_parquet, write_parquet


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab09DimensionEnrichment")

    current = read_parquet(spark, LAKE_DIR / "silver" / "orders_current")
    enriched = enriched_orders(spark, current)
    enriched_path = LAKE_DIR / "silver" / "orders_enriched"
    write_parquet(enriched, enriched_path, mode="overwrite")

    write_csv_dir(
        enriched.select(
            "order_id",
            "customer_name",
            "region",
            "product_name",
            "product_category",
            "status",
            "amount_usd",
            "customer_match_status",
        ).orderBy("order_id"),
        OUTPUT_DIR / "lab_09_enriched_orders_preview",
    )

    print("LAB 09 COMPLETE")
    print(f"Enriched current orders: {enriched.count()}")
    print("Concepts: dimension joins, broadcast lookup tables, currency normalization.")
    spark.stop()


if __name__ == "__main__":
    main()
