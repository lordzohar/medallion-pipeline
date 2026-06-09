from __future__ import annotations

from day7_common import OUTPUT_DIR, ensure_output_dirs, read_bronze_orders, require_source_data, schema_profile, spark_session, write_csv_dir


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab05BronzeSchemaDrift")

    bronze = read_bronze_orders(spark)
    profile = schema_profile(bronze)
    write_csv_dir(profile, OUTPUT_DIR / "lab_05_bronze_schema_profile")

    print("LAB 05 COMPLETE")
    print(f"Profiled {len(bronze.schema.fields)} Bronze columns across {bronze.count()} rows.")
    print("Concepts: schema drift, sparse optional fields, data profiling before cleaning.")
    spark.stop()


if __name__ == "__main__":
    main()
