-- =====================================================================
-- Lab 11 — ksqlDB on the Taxi Pipeline
-- =====================================================================
-- Run these inside the ksqlDB CLI:
--   docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
--
-- This file is a TAXI-flavored rewrite of the original orders ksqlDB demo.
-- Same SQL concepts (STREAM, TABLE, windowed aggregation, joins) but
-- against the taxi-trips and gps-pings topics the rest of the pipeline
-- is already producing.
-- =====================================================================

SET 'auto.offset.reset' = 'earliest';

-- ---------------------------------------------------------------------
-- 1) Raw stream over the taxi-trips topic produced by taxi_simulator.py
-- ---------------------------------------------------------------------
CREATE STREAM trips_raw (
  trip_id          STRING KEY,
  driver_id        STRING,
  pickup_zone      STRING,
  dropoff_zone     STRING,
  distance_miles   DOUBLE,
  duration_minutes DOUBLE,
  passenger_count  INT,
  payment_type     STRING,
  fare_amount      DOUBLE,
  tip_amount       DOUBLE,
  total_amount     DOUBLE,
  surge_multiplier DOUBLE,
  pickup_time      STRING
) WITH (
  KAFKA_TOPIC  = 'taxi-trips',
  VALUE_FORMAT = 'JSON'
);

-- ---------------------------------------------------------------------
-- 2) Filter: surge-priced trips (multiplier > 1.5)
--    Demonstrates push query + derived stream/topic.
-- ---------------------------------------------------------------------
CREATE STREAM surge_trips
  WITH (KAFKA_TOPIC='surge_trips', PARTITIONS=6) AS
  SELECT *
  FROM trips_raw
  WHERE surge_multiplier > 1.5
  EMIT CHANGES;

-- ---------------------------------------------------------------------
-- 3) Tumbling-window aggregate: revenue per pickup zone per minute.
--    This is the same logic as surge_detector.py, but in SQL.
-- ---------------------------------------------------------------------
CREATE TABLE revenue_per_zone_per_minute
  WITH (KAFKA_TOPIC='taxi_zone_revenue_1m', PARTITIONS=6) AS
  SELECT pickup_zone,
         WINDOWSTART AS window_start,
         WINDOWEND   AS window_end,
         COUNT(*)               AS trip_count,
         SUM(total_amount)      AS revenue,
         AVG(fare_amount)       AS avg_fare,
         AVG(distance_miles)    AS avg_distance
  FROM trips_raw
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY pickup_zone
  EMIT CHANGES;

-- ---------------------------------------------------------------------
-- 4) Hopping window: 5-minute revenue, advancing every 1 minute.
--    Shows the difference between TUMBLING and HOPPING windows.
-- ---------------------------------------------------------------------
CREATE TABLE revenue_per_zone_5m_hop
  WITH (KAFKA_TOPIC='taxi_zone_revenue_5m', PARTITIONS=6) AS
  SELECT pickup_zone,
         WINDOWSTART AS window_start,
         WINDOWEND   AS window_end,
         SUM(total_amount) AS revenue_5m
  FROM trips_raw
  WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
  GROUP BY pickup_zone
  EMIT CHANGES;

-- ---------------------------------------------------------------------
-- 5) Per-driver session: identify a driver's "shift" as activity bursts.
--    SESSION windows close after 10 min of inactivity.
-- ---------------------------------------------------------------------
CREATE TABLE driver_shifts
  WITH (KAFKA_TOPIC='taxi_driver_shifts') AS
  SELECT driver_id,
         WINDOWSTART AS shift_start,
         WINDOWEND   AS shift_end,
         COUNT(*)              AS trips_in_shift,
         SUM(total_amount)     AS shift_earnings
  FROM trips_raw
  WINDOW SESSION (10 MINUTES)
  GROUP BY driver_id
  EMIT CHANGES;

-- ---------------------------------------------------------------------
-- 6) Pull query examples (run interactively in the CLI)
-- ---------------------------------------------------------------------
-- Latest revenue snapshot for one zone:
--   SELECT * FROM revenue_per_zone_per_minute
--   WHERE pickup_zone='TIMES_SQUARE';
--
-- Top earners right now:
--   SELECT driver_id, SUM(shift_earnings) AS total
--   FROM driver_shifts GROUP BY driver_id EMIT CHANGES LIMIT 10;
-- =====================================================================
