-- Only reproducible entity-resolution state may be rebuilt. Never reset raw data.
DROP TABLE IF EXISTS stg_.post_artist_links;
DROP TABLE IF EXISTS stg_.post_artist_ambiguities;
DROP TABLE IF EXISTS stg_.post_artist_resolution_attempts;
