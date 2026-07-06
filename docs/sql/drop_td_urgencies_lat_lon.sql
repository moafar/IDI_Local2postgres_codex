ALTER TABLE test.td_urgencies DROP COLUMN IF EXISTS latitud;
ALTER TABLE test.td_urgencies DROP COLUMN IF EXISTS longitud;
ALTER TABLE prd.td_urgencies DROP COLUMN IF EXISTS latitud;
ALTER TABLE prd.td_urgencies DROP COLUMN IF EXISTS longitud;

SELECT table_schema, table_name, ordinal_position, column_name, data_type
FROM information_schema.columns
WHERE table_schema IN ('test', 'prd')
  AND table_name = 'td_urgencies'
ORDER BY table_schema, table_name, ordinal_position;
