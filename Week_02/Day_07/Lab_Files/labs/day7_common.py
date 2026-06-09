from __future__ import annotations

import csv
from datetime import date, datetime
import json
import shutil
import sys
from pathlib import Path
import types
import typing
from uuid import uuid4

# PySpark 3.4 imports typing.io, which is no longer a real module in
# Python 3.14. Keep the Day 7 local notebooks/scripts usable there.
if "typing.io" not in sys.modules:
    typing_io = types.ModuleType("typing.io")
    typing_io.IO = typing.IO
    typing_io.TextIO = typing.TextIO
    typing_io.BinaryIO = typing.BinaryIO
    sys.modules["typing.io"] = typing_io

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SOURCE_DIR = DATA_DIR / "source"
LAKE_DIR = BASE_DIR / "lake"
OUTPUT_DIR = BASE_DIR / "output"
STATE_DIR = BASE_DIR / "state"

ORDER_SOURCE_FILES = [
    SOURCE_DIR / "order_events_batch_1.jsonl",
    SOURCE_DIR / "order_events_batch_2.jsonl",
]

ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("event_time", StringType(), True),
        # Batch 2 introduces these nullable fields; keeping them in the contract
        # makes schema drift visible without relying on Spark schema inference.
        StructField("coupon_code", StringType(), True),
        StructField("delivery_promise", StringType(), True),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_cost", DoubleType(), True),
    ]
)

FX_SCHEMA = StructType(
    [
        StructField("currency", StringType(), False),
        StructField("rate_to_usd", DoubleType(), True),
        StructField("as_of_date", StringType(), True),
    ]
)


def spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.memory.fraction", "0.6")
        .config("spark.driver.maxResultSize", "256m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def require_source_data() -> None:
    missing = [
        path
        for path in [
            SOURCE_DIR / "customers.csv",
            SOURCE_DIR / "products.csv",
            SOURCE_DIR / "fx_rates.csv",
            *ORDER_SOURCE_FILES,
        ]
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing Day 7 source data. Run `python generate_data.py` from Lab_Files first. "
            f"Missing: {', '.join(str(path) for path in missing)}"
        )


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def ensure_output_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def clean_lab_dir(path: Path) -> Path:
    reset_dir(path)
    return path


def read_customers(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .schema(CUSTOMER_SCHEMA)
        .csv(str(SOURCE_DIR / "customers.csv"))
    )


def read_products(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .schema(PRODUCT_SCHEMA)
        .csv(str(SOURCE_DIR / "products.csv"))
    )


def read_fx_rates(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .schema(FX_SCHEMA)
        .csv(str(SOURCE_DIR / "fx_rates.csv"))
    )


def read_order_events(spark: SparkSession, files: list[Path] | None = None) -> DataFrame:
    paths = files or ORDER_SOURCE_FILES
    return spark.read.schema(ORDER_EVENT_SCHEMA).json([str(path) for path in paths])


def with_bronze_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file_path", F.input_file_name())
        .withColumn("_source_file", F.regexp_extract(F.col("_source_file_path"), r"([^/\\]+)$", 1))
        .withColumn("_ingestion_batch_id", F.lit(batch_id))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .drop("_source_file_path")
    )


def is_local_windows_hadoop_error(exc: Exception) -> bool:
    message = str(exc)
    return "winutils.exe" in message or "HADOOP_HOME" in message or "hadoop.home.dir" in message


def json_safe(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    return value


def safe_partition_value(value: object) -> str:
    if value is None:
        return "__HIVE_DEFAULT_PARTITION__"
    return str(value).replace("\\", "_").replace("/", "_").replace(":", "_")


def partition_columns(partition_by: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if partition_by is None:
        return []
    if isinstance(partition_by, str):
        return [partition_by]
    return list(partition_by)


def write_parquet(
    df: DataFrame,
    path: Path,
    mode: str = "overwrite",
    partition_by: str | list[str] | tuple[str, ...] | None = None,
) -> None:
    columns = partition_columns(partition_by)
    try:
        writer = df.write.mode(mode)
        if columns:
            writer = writer.partitionBy(*columns)
        writer.parquet(str(path))
    except Exception as exc:
        if not is_local_windows_hadoop_error(exc):
            raise

        if mode == "overwrite":
            reset_dir(path)
        else:
            path.mkdir(parents=True, exist_ok=True)

        grouped: dict[Path, list[dict[str, object]]] = {}
        for row in df.collect():
            record = {key: json_safe(value) for key, value in row.asDict(recursive=True).items()}
            output_dir = path
            for column in columns:
                output_dir = output_dir / f"{column}={safe_partition_value(record.get(column))}"
            grouped.setdefault(output_dir, []).append(record)

        for output_dir, records in grouped.items():
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"part-{uuid4().hex}.jsonl"
            with output_file.open("w", encoding="utf-8") as handle:
                for record in records:
                    handle.write(json.dumps(record, sort_keys=True) + "\n")

        (path / "_DAY7_LOCAL_JSON_FALLBACK").write_text(
            "Spark Parquet write fell back to JSON because local Windows Hadoop winutils.exe is missing.\n",
            encoding="utf-8",
        )
        (path / "_SUCCESS").write_text("", encoding="utf-8")


def read_parquet(spark: SparkSession, path: Path) -> DataFrame:
    if (path / "_DAY7_LOCAL_JSON_FALLBACK").exists():
        json_files = [str(item) for item in path.rglob("part-*.jsonl")]
        if not json_files:
            raise FileNotFoundError(f"No fallback JSON files found under {path}")
        return spark.read.json(json_files)
    return spark.read.parquet(str(path))


def schema_profile(df: DataFrame) -> DataFrame:
    metrics = df.agg(
        F.count(F.lit(1)).alias("__total_rows"),
        *[
            F.count(F.col(field.name)).alias(f"__non_null_{index}")
            for index, field in enumerate(df.schema.fields)
        ],
    )

    frames = [
        metrics.select(
            F.lit(field.name).alias("column_name"),
            F.lit(field.dataType.simpleString()).alias("data_type"),
            F.col(f"__non_null_{index}").cast("long").alias("non_null_rows"),
            (F.col("__total_rows") - F.col(f"__non_null_{index}")).cast("long").alias("null_rows"),
        )
        for index, field in enumerate(df.schema.fields)
    ]
    profile = frames[0]
    for frame in frames[1:]:
        profile = profile.unionByName(frame)
    return profile.orderBy("column_name")


def metric_table(spark: SparkSession, rows: list[tuple[str, int]]) -> DataFrame:
    base = spark.range(1).select()
    frames = [
        base.select(
            F.lit(metric).alias("metric"),
            F.lit(int(value)).cast("long").alias("value"),
        )
        for metric, value in rows
    ]
    table = frames[0]
    for frame in frames[1:]:
        table = table.unionByName(frame)
    return table


def write_csv_dir(df: DataFrame, path: Path, mode: str = "overwrite") -> None:
    reset_dir(path) if mode == "overwrite" else path.mkdir(parents=True, exist_ok=True)
    try:
        df.coalesce(1).write.mode(mode).option("header", "true").csv(str(path))
    except Exception as exc:
        if not is_local_windows_hadoop_error(exc):
            raise

        # Local Windows notebooks may not have Hadoop winutils.exe installed.
        # The lab CSV outputs are small inspection artifacts, so write them
        # directly with Python while keeping Spark writes for normal environments.
        reset_dir(path)
        output_file = path / "part-00000.csv"
        with output_file.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(df.columns)
            for row in df.collect():
                writer.writerow([row[column] for column in df.columns])
        (path / "_SUCCESS").write_text("", encoding="utf-8")


def write_json_report(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_bronze_orders(spark: SparkSession, base_path: Path | None = None) -> DataFrame:
    path = base_path or LAKE_DIR / "bronze" / "orders_raw"
    return read_parquet(spark, path)


def cleaned_orders(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("event_time_ts", F.to_timestamp(F.col("event_time")))
        .withColumn("event_date", F.to_date(F.col("event_time_ts")))
    )


def quality_checked_orders(df: DataFrame) -> DataFrame:
    raw_errors = F.array(
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
        F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
        F.when(F.col("product_id").isNull() | (F.length("product_id") == 0), F.lit("missing_product_id")),
        F.when(F.col("event_time_ts").isNull(), F.lit("invalid_event_time")),
        F.when(F.col("amount").isNull(), F.lit("missing_amount")),
        F.when(F.col("amount") <= 0, F.lit("non_positive_amount")),
        F.when(~F.col("status").isin("NEW", "PAID", "SHIPPED", "CANCELLED"), F.lit("invalid_status")),
        F.when(~F.col("currency").isin("USD", "EUR", "INR"), F.lit("unsupported_currency")),
    )
    return (
        df.withColumn("_quality_errors_raw", raw_errors)
        .withColumn("quality_errors", F.expr("filter(_quality_errors_raw, x -> x is not null)"))
        .withColumn("is_valid", F.size(F.col("quality_errors")) == 0)
        .drop("_quality_errors_raw")
    )


def deduplicate_order_events(df: DataFrame) -> DataFrame:
    deduplication_keys = [
        "order_id",
        "customer_id",
        "product_id",
        "status",
        "amount",
        "currency",
        "channel",
        "event_time_ts",
    ]
    available_keys = [column for column in deduplication_keys if column in df.columns]
    return df.dropDuplicates(available_keys)


def latest_order_state(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("order_id").orderBy(F.col("event_time_ts").desc(), F.col("event_id").desc())
    return (
        df.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .orderBy("order_id")
    )


def enriched_orders(spark: SparkSession, current_orders: DataFrame) -> DataFrame:
    customers = read_customers(spark).select(
        "customer_id", "customer_name", "email", "country", "region", "loyalty_tier"
    )
    products = read_products(spark).select(
        "product_id",
        "product_name",
        F.upper(F.col("category")).alias("product_category"),
        "unit_cost",
    )
    fx_rates = read_fx_rates(spark).select("currency", "rate_to_usd")

    return (
        current_orders.join(F.broadcast(customers), "customer_id", "left")
        .join(F.broadcast(products), "product_id", "left")
        .join(F.broadcast(fx_rates), "currency", "left")
        .withColumn(
            "customer_match_status",
            F.when(F.col("customer_name").isNull(), F.lit("MISSING_CUSTOMER")).otherwise(F.lit("MATCHED")),
        )
        .withColumn(
            "product_match_status",
            F.when(F.col("product_name").isNull(), F.lit("MISSING_PRODUCT")).otherwise(F.lit("MATCHED")),
        )
        .withColumn("amount_usd", F.round(F.col("amount") * F.col("rate_to_usd"), 2))
        .withColumn("gross_margin_usd", F.round(F.col("amount_usd") - F.col("unit_cost"), 2))
        .fillna({"region": "UNKNOWN", "loyalty_tier": "UNKNOWN", "product_category": "UNKNOWN"})
    )


def gold_frames(enriched: DataFrame) -> dict[str, DataFrame]:
    revenue_orders = enriched.filter(F.col("status") != "CANCELLED")

    daily_revenue = (
        revenue_orders.groupBy(F.col("event_date").alias("order_date"))
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.round(F.sum("amount_usd"), 2).alias("total_revenue_usd"),
            F.round(F.avg("amount_usd"), 2).alias("avg_order_value_usd"),
        )
        .orderBy("order_date")
    )

    category_revenue = (
        revenue_orders.groupBy("product_category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("amount_usd"), 2).alias("total_revenue_usd"),
            F.round(F.sum("gross_margin_usd"), 2).alias("gross_margin_usd"),
        )
        .orderBy(F.desc("total_revenue_usd"))
    )

    segment_revenue = (
        revenue_orders.groupBy("region", "loyalty_tier")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("amount_usd"), 2).alias("total_revenue_usd"),
        )
        .orderBy("region", "loyalty_tier")
    )

    return {
        "daily_revenue": daily_revenue,
        "category_revenue": category_revenue,
        "segment_revenue": segment_revenue,
    }


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return len([item for item in path.rglob("*") if item.is_file() and not item.name.startswith(".")])
