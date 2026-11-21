-- COUNT alone can use metadata without reading damaged column segments.
SELECT count(*), bit_xor(hash(COLUMNS(*))) FROM query_table(?);
