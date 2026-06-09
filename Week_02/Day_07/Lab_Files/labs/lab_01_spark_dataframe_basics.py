from __future__ import annotations

from day7_common import OUTPUT_DIR, ensure_output_dirs, read_customers, require_source_data, spark_session, write_csv_dir

from pyspark.sql import functions as F


def main() -> None:
    require_source_data()
    ensure_output_dirs()
    spark = spark_session("Day7Lab01DataFrameBasics")

    customers = read_customers(spark)
    preview = (
        customers.withColumn("email", F.lower(F.trim("email")))
        .withColumn("signup_date", F.to_date("signup_date"))
        .select("customer_id", "customer_name", "email", "region", "loyalty_tier", "signup_date")
        .orderBy("customer_id")
    )

    write_csv_dir(preview, OUTPUT_DIR / "lab_01_customers_preview")
    print("LAB 01 COMPLETE")
    print(f"Rows read from customers.csv: {customers.count()}")
    print("Concepts: SparkSession, explicit schema, lazy DataFrame transformations, action via count().")
    spark.stop()


if __name__ == "__main__":
    main()
