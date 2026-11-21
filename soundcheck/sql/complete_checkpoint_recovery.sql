UPDATE stg_.checkpoint_recovery SET completed_at = current_timestamp
WHERE completed_at IS NULL;
