-- =============================================================================
-- WAL / logical replication sanity checks for Debezium.
-- =============================================================================

DO $$
BEGIN
    IF current_setting('wal_level') <> 'logical' THEN
        RAISE EXCEPTION 'wal_level must be logical for Debezium, got %', current_setting('wal_level');
    END IF;
END $$;

SELECT pubname FROM pg_publication WHERE pubname = 'dbz_publication';

-- Pre-create the slot so Debezium starts fast on first boot.
SELECT pg_create_logical_replication_slot('pipeline_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pipeline_slot'
);

SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'pipeline_slot';
