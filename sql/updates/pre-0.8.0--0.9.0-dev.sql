-- Tablespace changes
DROP FUNCTION _timescaledb_internal.select_tablespace(integer, integer[]);
DROP FUNCTION _timescaledb_internal.select_tablespace(integer, integer);
DROP FUNCTION _timescaledb_internal.select_tablespace(integer);

-- Chunk functions
DROP FUNCTION _timescaledb_internal.chunk_create(integer, integer, name, name);
DROP FUNCTION _timescaledb_internal.drop_chunk_metadata(int);

-- Chunk constraint functions
DROP FUNCTION _timescaledb_internal.create_chunk_constraint(integer, oid);
DROP FUNCTION _timescaledb_internal.drop_constraint(integer, name);
DROP FUNCTION _timescaledb_internal.drop_chunk_constraint(integer, name, boolean);
DROP FUNCTION _timescaledb_internal.chunk_constraint_drop_table_constraint(_timescaledb_catalog.chunk_constraint);

-- Dimension functions
DROP FUNCTION _timescaledb_internal.change_column_type(int, name, regtype);
DROP FUNCTION _timescaledb_internal.rename_column(int, name, name);

DROP FUNCTION _timescaledb_internal.time_to_internal(anyelement, regtype);
DROP FUNCTION create_hypertable(regclass, name, name, integer, name, name, anyelement, boolean, boolean, regproc);
DROP FUNCTION _timescaledb_internal.create_hypertable(regclass, name, name, name, name, integer, name, name, bigint, name, boolean, regproc);

ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN chunk_sizing_func_schema NAME;
ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN chunk_sizing_func_name NAME;
ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN chunk_target_size BIGINT CHECK (chunk_target_size >= 0);
ALTER TABLE _timescaledb_catalog.hypertable ADD CONSTRAINT hypertable_check CHECK (
        (chunk_sizing_func_schema IS NULL AND chunk_sizing_func_name IS NULL) OR
        (chunk_sizing_func_schema IS NOT NULL AND chunk_sizing_func_name IS NOT NULL)
);
UPDATE _timescaledb_catalog.hypertable SET chunk_target_size = 0;
ALTER TABLE _timescaledb_catalog.hypertable ALTER COLUMN chunk_target_size SET NOT NULL;
