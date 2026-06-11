#!/usr/bin/env python
"""Create the bronze/silver/gold buckets on MinIO. Idempotent."""
from __future__ import annotations

import os
import sys
import time

from minio import Minio
from minio.error import S3Error

ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").replace("http://", "").replace("https://", "")
ACCESS   = os.environ.get("MINIO_ROOT_USER", "minioadmin")
SECRET   = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKETS  = [os.environ.get("BRONZE_BUCKET", "bronze"),
            os.environ.get("SILVER_BUCKET", "silver"),
            os.environ.get("GOLD_BUCKET",   "gold")]


def main() -> None:
    deadline = time.time() + 60
    last_err = None
    while time.time() < deadline:
        try:
            client = Minio(ENDPOINT, access_key=ACCESS, secret_key=SECRET, secure=False)
            client.list_buckets()
            break
        except Exception as e:
            last_err = e
            print(f"[wait] MinIO not ready: {e}")
            time.sleep(3)
    else:
        sys.exit(f"[err] MinIO not reachable after 60s: {last_err}")

    for b in BUCKETS:
        try:
            if not client.bucket_exists(b):
                client.make_bucket(b)
                print(f"[ok] created bucket {b}")
            else:
                print(f"[skip] bucket exists: {b}")
        except S3Error as e:
            print(f"[err] {b}: {e}")


if __name__ == "__main__":
    main()
