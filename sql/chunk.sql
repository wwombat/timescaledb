CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_open(
        dimension_value   BIGINT,
        interval_length   BIGINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '@MODULE_PATHNAME@', 'dimension_calculate_open_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(
        dimension_value   BIGINT,
        num_slices        SMALLINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '@MODULE_PATHNAME@', 'dimension_calculate_closed_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.validate_chunk_sizing_func(
        chunk_sizing_func REGPROC) RETURNS VOID
    AS '@MODULE_PATHNAME@', 'chunk_adaptive_validate_chunk_sizing_func' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_initial_chunk_target_size() RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'chunk_adaptive_calculate_initial_chunk_target_size' LANGUAGE C STABLE;
