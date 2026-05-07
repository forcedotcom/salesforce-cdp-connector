SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    a.attname AS column_name,
    a.attnum AS ordinal_position,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable,
    pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS column_default,
    dsc.description AS description
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)
JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum)
LEFT JOIN pg_catalog.pg_description dsc ON (c.oid = dsc.objoid AND a.attnum = dsc.objsubid)
WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
    AND a.attnum > 0
    AND NOT a.attisdropped
