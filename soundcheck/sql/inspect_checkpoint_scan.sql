SELECT count(*), bit_xor(hash(COLUMNS(*))) FROM query_table(?);
