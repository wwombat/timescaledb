CREATE OR REPLACE FUNCTION _timescaledb_internal.median_finalfunc(state INTERNAL, var ANYELEMENT)
RETURNS ANYELEMENT
AS '@MODULE_PATHNAME@', 'median_finalfunc'
LANGUAGE C IMMUTABLE;

-- Tell Postgres how to use the new function
DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    SFUNC = array_agg_transfn,
    STYPE = INTERNAL,
    FINALFUNC = _timescaledb_internal.median_finalfunc,
    FINALFUNC_EXTRA
);
