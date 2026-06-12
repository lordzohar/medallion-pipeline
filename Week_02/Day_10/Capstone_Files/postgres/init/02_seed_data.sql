-- =============================================================================
-- Seed the config reference tables. Volume targets:
--   regions:              ~1000 rows (mix of airports, weather stations, seismic zones)
--   alert_thresholds:     ~30 rows
--   subscriber_watchlist: ~200 rows
-- =============================================================================

-- ---------- regions: ~1000 row mix ----------

-- A small hand-curated "starter" set of well-known regions so the demo joins
-- against actual data right after boot. Synthetic regions follow.
INSERT INTO regions (name, country, kind, min_lat, max_lat, min_lon, max_lon, station_code) VALUES
  ('JFK New York',          'US', 'airport',         40.55, 40.78, -73.95, -73.65, 'KJFK'),
  ('Los Angeles LAX',       'US', 'airport',         33.85, 34.05, -118.55, -118.30, 'KLAX'),
  ('London Heathrow',       'GB', 'airport',         51.40, 51.60, -0.65, -0.30,   'EGLL'),
  ('Frankfurt EDDF',        'DE', 'airport',         49.95, 50.10, 8.45, 8.70,     'EDDF'),
  ('Munich EDDM',           'DE', 'airport',         48.30, 48.45, 11.65, 11.95,   'EDDM'),
  ('Paris CDG',             'FR', 'airport',         48.95, 49.05, 2.45, 2.65,     'LFPG'),
  ('Tokyo Haneda',          'JP', 'airport',         35.45, 35.65, 139.65, 139.95, 'RJTT'),
  ('Sydney Kingsford-Smith','AU', 'airport',         -34.00, -33.85, 150.95, 151.25,'YSSY'),
  ('San Francisco SFO',     'US', 'airport',         37.55, 37.75, -122.50, -122.30,'KSFO'),
  ('Chicago ORD',           'US', 'airport',         41.85, 42.05, -87.95, -87.75, 'KORD'),
  ('Pacific Ring (Japan)',  'JP', 'seismic_zone',    30.0, 46.0, 128.0, 146.0, NULL),
  ('California Faults',     'US', 'seismic_zone',    32.0, 42.0, -125.0, -114.0, NULL),
  ('Mediterranean',         'IT', 'seismic_zone',    35.0, 47.0, 5.0, 25.0, NULL),
  ('Indonesia Arc',         'ID', 'seismic_zone',    -11.0, 6.0, 95.0, 141.0, NULL),
  ('Chile/Peru Trench',     'CL', 'seismic_zone',    -45.0, -2.0, -82.0, -68.0, NULL),
  ('New York Metro',        'US', 'metro',           40.45, 41.05, -74.30, -73.55, NULL),
  ('Berlin Metro',          'DE', 'metro',           52.30, 52.70, 13.05, 13.80, NULL),
  ('São Paulo Metro',       'BR', 'metro',           -23.85, -23.30, -46.85, -46.30, NULL),
  ('Bangalore Metro',       'IN', 'metro',           12.80, 13.20, 77.40, 77.85, NULL),
  ('Cape Town',             'ZA', 'metro',           -34.10, -33.70, 18.30, 18.80, NULL);

-- Generate ~980 more synthetic regions distributed across the globe so the
-- table reaches the 1000-row target you specified for the capstone.
WITH g AS (
  SELECT
    i,
    (random() * 160 - 80)::float AS clat,        -- -80 .. 80
    (random() * 360 - 180)::float AS clon,       -- -180 .. 180
    (random() * 0.4 + 0.1)::float AS half_lat,   -- box half-height
    (random() * 0.6 + 0.1)::float AS half_lon,   -- box half-width
    (ARRAY['airport','weather_station','seismic_zone','metro'])[1 + floor(random()*4)::int] AS kind
  FROM generate_series(1, 980) i
)
INSERT INTO regions (name, country, kind, min_lat, max_lat, min_lon, max_lon, station_code)
SELECT
    'auto-region-' || lpad(i::text, 4, '0'),
    (ARRAY['US','DE','FR','BR','IN','JP','GB','CN','ZA','AU','MX','CA','IT','ES','AR'])[1 + floor(random()*15)::int],
    kind,
    greatest(clat - half_lat, -89),
    least(clat + half_lat, 89),
    greatest(clon - half_lon, -180),
    least(clon + half_lon, 180),
    CASE WHEN kind = 'weather_station'
         THEN 'AUTO' || lpad(i::text, 4, '0')
         ELSE NULL END
FROM g;


-- ---------- alert_thresholds ----------

INSERT INTO alert_thresholds (source, metric, op, threshold, severity, description) VALUES
  ('seismic', 'magnitude',        '>=', 5.5,  'warning',  'Magnitude 5.5+ — notify regional ops.'),
  ('seismic', 'magnitude',        '>=', 6.5,  'critical', 'Magnitude 6.5+ — escalate to disaster desk.'),
  ('seismic', 'depth_km',         '<=', 10.0, 'warning',  'Shallow quake (<=10km) — higher surface impact risk.'),
  ('seismic', 'events_per_hour',  '>=', 5,    'warning',  'Cluster: 5+ events in same region within an hour.'),
  ('noaa',    'wind_gust_kmh',    '>=', 90,   'warning',  'Strong wind gusts.'),
  ('noaa',    'wind_gust_kmh',    '>=', 120,  'critical', 'Hurricane-force gusts.'),
  ('noaa',    'temperature_c',    '>=', 40,   'warning',  'Extreme heat.'),
  ('noaa',    'temperature_c',    '<=', -25,  'warning',  'Extreme cold.'),
  ('noaa',    'visibility_m',     '<=', 200,  'warning',  'Severely reduced visibility.'),
  ('noaa',    'precip_last_1h_mm','>=', 30,   'warning',  'Heavy rainfall — flooding risk.'),
  ('ogn',     'altitude_m',       '>=', 6000, 'info',     'Aircraft above typical glider ceiling.'),
  ('ogn',     'climb_rate_mps',   '>=', 10,   'info',     'Vigorous thermal — soaring opportunity.'),
  ('ogn',     'climb_rate_mps',   '<=', -10,  'warning',  'Strong sink — windshear risk.'),
  ('ogn',     'ground_speed_kmh', '>=', 400,  'info',     'Fast jet (likely not a glider).');

-- Pad to ~30 rows with auto-thresholds covering the metric x severity space.
WITH t AS (
  SELECT
    s.source,
    m.metric,
    (ARRAY['>','>=','<','<='])[1 + floor(random()*4)::int] AS op,
    (random() * 100)::float AS threshold,
    (ARRAY['info','warning','critical'])[1 + floor(random()*3)::int] AS severity
  FROM (VALUES ('ogn'), ('noaa'), ('seismic')) AS s(source)
  CROSS JOIN (VALUES ('lag_ms'), ('rate_per_min'), ('null_rate_pct')) AS m(metric)
  CROSS JOIN generate_series(1,3)
)
INSERT INTO alert_thresholds (source, metric, op, threshold, severity, description)
SELECT source, metric, op, round(threshold::numeric, 2)::float8, severity,
       'auto-generated baseline rule'
FROM t
ON CONFLICT (source, metric, op, threshold) DO NOTHING;


-- ---------- subscriber_watchlist ----------

-- Subscribe a few hand-picked recipients to our hand-curated regions.
INSERT INTO subscriber_watchlist (region_id, source, channel, recipient)
SELECT r.region_id, s.source, s.channel, s.recipient
FROM regions r,
     (VALUES
        ('seismic','email','quake-ops@example.org'),
        ('noaa',   'email','weather-ops@example.org'),
        ('ogn',    'slack','#glider-ops'),
        ('*',      'webhook','http://quality-dashboard:5001/webhook/regional')
     ) AS s(source, channel, recipient)
WHERE r.station_code IS NOT NULL OR r.kind = 'seismic_zone';

-- Pad with auto subscriptions until we reach ~200 rows.
INSERT INTO subscriber_watchlist (region_id, source, channel, recipient)
SELECT
    r.region_id,
    (ARRAY['ogn','noaa','seismic','*'])[1 + floor(random()*4)::int],
    (ARRAY['email','slack','webhook'])[1 + floor(random()*3)::int],
    'auto-sub-' || lpad((row_number() OVER ())::text, 4, '0') || '@example.org'
FROM regions r
ORDER BY random()
LIMIT 200;

-- ---------- audit ----------
SELECT 'regions', count(*) FROM regions
UNION ALL SELECT 'alert_thresholds', count(*) FROM alert_thresholds
UNION ALL SELECT 'subscriber_watchlist', count(*) FROM subscriber_watchlist;
