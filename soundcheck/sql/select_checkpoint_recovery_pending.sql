SELECT EXISTS (
    SELECT 1 FROM stg_.checkpoint_recovery WHERE completed_at IS NULL
);
