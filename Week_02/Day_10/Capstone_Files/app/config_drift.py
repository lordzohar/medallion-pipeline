#!/usr/bin/env python
"""Config drift simulator — performs occasional UPDATE/INSERT/DELETE on the
reference tables (regions, alert_thresholds, subscriber_watchlist) so that
Debezium has steady CDC traffic to show. Triggered every 2 min by Airflow.
"""
from __future__ import annotations

import argparse
import os
import random
import uuid

import psycopg2

PG_HOST = os.environ.get("CONFIG_PG_HOST", "config-db")
PG_DB   = os.environ.get("CONFIG_PG_DB",   "config")
PG_USER = os.environ.get("CONFIG_PG_USER", "postgres")
PG_PASS = os.environ.get("CONFIG_PG_PASSWORD", "postgres")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mutations", type=int, default=20,
                    help="How many UPDATE/INSERT/DELETE rows to push (default 20)")
    args = ap.parse_args()

    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    conn.autocommit = False
    cur = conn.cursor()

    for _ in range(args.mutations):
        op = random.choices(["update_threshold", "toggle_region",
                             "insert_watchlist", "delete_watchlist"],
                            weights=[5, 4, 2, 1], k=1)[0]
        if op == "update_threshold":
            cur.execute("""
                UPDATE alert_thresholds
                   SET threshold = threshold + (random() - 0.5),
                       updated_at = NOW()
                 WHERE threshold_id = (SELECT threshold_id FROM alert_thresholds
                                        ORDER BY random() LIMIT 1)
            """)
        elif op == "toggle_region":
            cur.execute("""
                UPDATE regions
                   SET is_active = NOT is_active,
                       updated_at = NOW()
                 WHERE region_id = (SELECT region_id FROM regions
                                     ORDER BY random() LIMIT 1)
            """)
        elif op == "insert_watchlist":
            cur.execute("""
                INSERT INTO subscriber_watchlist (region_id, source, channel, recipient)
                SELECT region_id,
                       (ARRAY['ogn','noaa','seismic','*'])[1 + (random()*3.99)::int],
                       (ARRAY['email','slack','webhook'])[1 + (random()*2.99)::int],
                       %s
                  FROM regions ORDER BY random() LIMIT 1
            """, (f"drift-{uuid.uuid4().hex[:8]}@example.org",))
        elif op == "delete_watchlist":
            cur.execute("""
                DELETE FROM subscriber_watchlist
                 WHERE watchlist_id = (SELECT watchlist_id FROM subscriber_watchlist
                                        WHERE recipient LIKE 'drift-%'
                                        ORDER BY random() LIMIT 1)
            """)

    conn.commit()
    cur.close(); conn.close()
    print(f"[config-drift] pushed {args.mutations} mutations")


if __name__ == "__main__":
    main()
