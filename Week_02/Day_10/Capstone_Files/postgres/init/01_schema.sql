-- =============================================================================
-- Day 10 capstone — pipeline runtime CONFIG schema (Debezium source-of-truth).
--
-- This database does NOT hold business events. Real events come from the
-- public streams: OGN (aircraft), NOAA (weather), Seismic Portal (quakes).
--
-- What lives here are tiny, slowly-changing **config / reference** tables
-- that downstream consumers join against. They are watched by Debezium so
-- runtime config changes flow as CDC into Kafka and ultimately the bronze
-- layer — a realistic, narrowly-scoped use of CDC.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------- regions ----------
-- A region is a geographic bbox we care about. Streams (OGN/NOAA/Seismic) are
-- joined to regions in the gold layer to compute per-region density / counts.
CREATE TABLE IF NOT EXISTS regions (
    region_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT        NOT NULL,
    country       TEXT        NOT NULL,
    kind          TEXT        NOT NULL CHECK (kind IN ('airport','weather_station','seismic_zone','metro')),
    min_lat       DOUBLE PRECISION NOT NULL,
    max_lat       DOUBLE PRECISION NOT NULL,
    min_lon       DOUBLE PRECISION NOT NULL,
    max_lon       DOUBLE PRECISION NOT NULL,
    -- For convenience when joining noaa.observations station_id.
    station_code  TEXT,
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE regions REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS regions_kind_idx        ON regions(kind);
CREATE INDEX IF NOT EXISTS regions_station_idx     ON regions(station_code);

-- ---------- alert_thresholds ----------
-- Rule rows that the gold/serving layer evaluates. CDC fires when ops
-- change a threshold; consumers reload without restart.
CREATE TABLE IF NOT EXISTS alert_thresholds (
    threshold_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source        TEXT  NOT NULL CHECK (source IN ('ogn','noaa','seismic')),
    metric        TEXT  NOT NULL,
    op            TEXT  NOT NULL CHECK (op IN ('>','>=','<','<=','=')),
    threshold     DOUBLE PRECISION NOT NULL,
    severity      TEXT  NOT NULL CHECK (severity IN ('info','warning','critical')),
    description   TEXT,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, metric, op, threshold)
);
ALTER TABLE alert_thresholds REPLICA IDENTITY FULL;

-- ---------- subscriber_watchlist ----------
-- Who wants to be told when a region trips a threshold.
CREATE TABLE IF NOT EXISTS subscriber_watchlist (
    watchlist_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    region_id     UUID NOT NULL REFERENCES regions(region_id) ON DELETE CASCADE,
    source        TEXT NOT NULL,                  -- ogn|noaa|seismic|*
    channel       TEXT NOT NULL CHECK (channel IN ('email','slack','webhook')),
    recipient     TEXT NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE subscriber_watchlist REPLICA IDENTITY FULL;
CREATE INDEX IF NOT EXISTS watchlist_region_idx ON subscriber_watchlist(region_id);

-- ---------- Debezium publication ----------
DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR TABLE
    regions, alert_thresholds, subscriber_watchlist;
