"""
db_seeder.py
------------
Creates the `drivers` reference table in Postgres and seeds it.

The simulator picks driver IDs from this table, so trip events
reference *real* rows that Debezium will later stream into Kafka
as the `cdc.public.drivers` topic.

Run once after the stack is up:
    python db_seeder.py
"""
from __future__ import annotations

import random

import psycopg2

from config import POSTGRES_DSN

NUM_DRIVERS = 100

VEHICLES = [
    ("Toyota", "Camry"), ("Honda", "Accord"), ("Ford", "Fusion"),
    ("Hyundai", "Sonata"), ("Tesla", "Model 3"), ("Chevrolet", "Malibu"),
    ("Nissan", "Altima"), ("Kia", "Optima"),
]

FIRST = ["Alex","Maria","John","Aisha","Carlos","Priya","Wei","Lena","Mohamed","Sara"]
LAST  = ["Garcia","Smith","Patel","Kim","Brown","Khan","Singh","Lopez","Wong","Davis"]


DDL = """
CREATE TABLE IF NOT EXISTS drivers (
    driver_id      TEXT PRIMARY KEY,
    full_name      TEXT NOT NULL,
    license_number TEXT NOT NULL,
    rating         NUMERIC(3,2) NOT NULL,
    vehicle_make   TEXT NOT NULL,
    vehicle_model  TEXT NOT NULL,
    vehicle_year   INT  NOT NULL,
    hire_date      DATE NOT NULL,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

-- Debezium needs the table in a publication
ALTER TABLE drivers REPLICA IDENTITY FULL;
"""


def main():
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("SELECT COUNT(*) FROM drivers")
            (n,) = cur.fetchone()
            if n >= NUM_DRIVERS:
                print(f"drivers already seeded ({n} rows)")
                return
            print(f"seeding {NUM_DRIVERS} drivers...")
            rows = []
            for i in range(NUM_DRIVERS):
                make, model = random.choice(VEHICLES)
                rows.append((
                    f"DRV-{i:04d}",
                    f"{random.choice(FIRST)} {random.choice(LAST)}",
                    f"NY-{random.randint(100000, 999999)}",
                    round(random.uniform(4.2, 5.0), 2),
                    make, model,
                    random.randint(2015, 2024),
                    f"20{random.randint(18, 24)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                ))
            cur.executemany(
                """INSERT INTO drivers
                   (driver_id, full_name, license_number, rating, vehicle_make,
                    vehicle_model, vehicle_year, hire_date)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (driver_id) DO NOTHING""",
                rows,
            )
        conn.commit()
    print("done.")


if __name__ == "__main__":
    main()
