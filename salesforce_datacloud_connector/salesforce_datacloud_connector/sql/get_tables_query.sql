SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE c.relkind
        WHEN 'r' THEN 'TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'm' THEN 'MATERIALIZED VIEW'
        WHEN 'f' THEN 'FOREIGN TABLE'
        WHEN 'p' THEN 'PARTITIONED TABLE'
        ELSE 'UNKNOWN'
    END AS table_type,
    d.description AS remarks
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass)
WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
    AND n.nspname NOT LIKE 'pg_%'
    AND n.nspname != 'information_schema'
    AND n.nspname NOT LIKE 'tableau%'
