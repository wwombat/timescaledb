#include <postgres.h>

#include "median_quickselect.h"

#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/numeric.h>
#include <utils/typcache.h>

#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

#include "compat.h"

#include <stddef.h>

/*
 * The postgres aggregate takes advantage of existing functions for
 * collecting array data- since the computation of the median requires seeing
 * all rows at once, we can use the existing array_append accumulate function
 * and simply provide a 'finalfunc' that computes the median given the
 * complete dataset.
 *
 * ---------- 
 * CREATE AGGREGATE avg (float8) 
 * ( 
 *     sfunc = array_append,
 *     stype = anyarray,
 *     finalfunc = medianfinalfunc
 * );
 * ----------
 */

/* a bare bones wrapper around an array of Numerics */
typedef struct DatumCArray
{
	Size size;
	Datum *data;
} DatumCArray;

/*
 * Create a DatumCArray allocated using the specified 'agg_context' from the
 * specified 'array', IGNORING any null values.
 *
 * On error, this function will call elog(ERROR, ...)
 */
static
DatumCArray pg_array_to_c_array(ArrayType *array,
									  MemoryContext *agg_context)
{
	Assert(array);
	Assert(agg_context);

	/* result */
	DatumCArray result;

	int			number_of_dimensions = 0;
	int		   *array_of_dim_lengths = 0;
	size_t		length = 0;

	/* for iterator creation */
	int			slice_ndim = 0; /* iterate item by item */
	ArrayMetaState *meta_state = NULL;
	ArrayIterator iterator;

	/* for iterating through array */
	Datum		value;
	bool		is_null = false;
	int			i = 0;
	size_t		actual_length = 0;

	/* validate input */
	number_of_dimensions = ARR_NDIM(array);
	array_of_dim_lengths = ARR_DIMS(array);
	if (number_of_dimensions != 1)
	{
		elog(ERROR, "median undefined on an array column");
	}
	length = array_of_dim_lengths[0];

	result.data = MemoryContextAllocZero(*agg_context, length * sizeof(Datum));

	iterator = array_create_iterator(array, slice_ndim, meta_state);

	while (array_iterate(iterator, &value, &is_null))
	{
		result.data[i] = value;
		++actual_length;
		++i;
	}
	array_free_iterator(iterator);

	result.size = actual_length;

	return result;
}

/*
 * The final function for the median aggregate, specialized for Numeric
 * values. Takes as an argument an ArrayType of Numeric values, returning the
 * median.
 *
 * Unpacks the array into a c-style array, taking O(N) extra space.
 *
 * Uses the Quickselect algorithm, taking O(N) on average, O(N^2) in the
 * worst case.
 */
TS_FUNCTION_INFO_V1(median_finalfunc);
Datum
median_finalfunc(PG_FUNCTION_ARGS)
{
	MemoryContext   agg_context;
	ArrayBuildState *state = NULL;
	Datum           array_datum;
	DatumCArray     c_array = {0};
	ArrayType       *array;
    
    /* For getting comparison operator */
    Oid                     elem_type = 0;
    TypeCacheEntry          *type_cache_entry = NULL;
    Oid                     collation = PG_GET_COLLATION();

	Datum result = {0};

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		elog(ERROR,
             "timescale medianfinalfunc called "
			 "in non-aggregate context");
	}

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}

	state = (ArrayBuildState *) PG_GETARG_POINTER(0);

	if (state == NULL)
	{
		PG_RETURN_NULL();
	}

	array_datum = makeArrayResult(state, agg_context);
	array = DatumGetArrayTypeP(array_datum);

    /* fetch comparison operator */
    elem_type = ARR_ELEMTYPE(array);
    type_cache_entry = lookup_type_cache(elem_type, TYPECACHE_CMP_PROC_FINFO);
    if (type_cache_entry->cmp_proc_finfo.fn_oid == InvalidOid)
    {
        elog(ERROR,
             "could not find comparison function for type %u", elem_type);
    }

    /* create c array */
	c_array = pg_array_to_c_array(array, &agg_context);

	if (c_array.size == 0)
	{
		PG_RETURN_NULL();
	}

	result = median_quickselect(c_array.data, c_array.size,
                                &type_cache_entry->cmp_proc_finfo,
                                collation);

	PG_RETURN_DATUM(result);
}
