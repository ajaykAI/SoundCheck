SELECT schema_name, table_name
FROM duckdb_tables()
WHERE NOT internal
ORDER BY schema_name, table_name;
