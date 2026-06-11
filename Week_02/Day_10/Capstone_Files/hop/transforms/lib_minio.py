"""Small MinIO/S3 helpers shared by the bronze→silver and silver→gold transforms.

Reads Avro from `bronze/`, reads/writes Avro from/to `silver/`, writes Parquet
to `gold/`. Path style access (MinIO defaults).
"""
from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone
from typing import Iterator

import boto3
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
SECRET    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE    = os.environ.get("BRONZE_BUCKET", "bronze")
SILVER    = os.environ.get("SILVER_BUCKET", "silver")
GOLD      = os.environ.get("GOLD_BUCKET",   "gold")


def s3() -> "boto3.client":
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_keys(bucket: str, prefix: str) -> Iterator[str]:
    client = s3()
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            yield o["Key"]
        if not resp.get("IsTruncated"):
            return
        token = resp.get("NextContinuationToken")


def recent_keys(bucket: str, prefix: str, hours: int = 24) -> list[str]:
    """List keys whose `LastModified` falls within the last `hours` hours."""
    client = s3()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out: list[str] = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            if o["LastModified"] >= cutoff:
                out.append(o["Key"])
        if not resp.get("IsTruncated"):
            return out
        token = resp.get("NextContinuationToken")


def read_avro(bucket: str, key: str) -> tuple[list[dict], dict]:
    """Return (records, writer-schema)."""
    body = s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    reader = fastavro.reader(io.BytesIO(body))
    records = list(reader)
    return records, reader.writer_schema


def write_avro(bucket: str, key: str, schema: dict, records: list[dict]) -> int:
    if not records:
        return 0
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records, codec="snappy")
    buf.seek(0)
    s3().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return len(records)


def write_parquet(bucket: str, key: str, table: pa.Table) -> int:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    s3().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return table.num_rows


def ensure_bucket(name: str) -> None:
    client = s3()
    try:
        client.head_bucket(Bucket=name)
    except Exception:
        client.create_bucket(Bucket=name)
