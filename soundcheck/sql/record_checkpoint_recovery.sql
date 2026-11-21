CREATE TABLE IF NOT EXISTS stg_.checkpoint_recovery (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    recovered_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

INSERT INTO stg_.checkpoint_recovery VALUES (true, current_timestamp, NULL)
ON CONFLICT (singleton) DO UPDATE SET
    recovered_at = excluded.recovered_at,
    completed_at = NULL;
