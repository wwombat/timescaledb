/*
 * This median function implements the 'quickselect' algorithm, aka Hoare's
 * "Algorithm 65: Find".
 *
 * https://en.wikipedia.org/wiki/Quickselect
 *
 * The typedef 'Datum' and macro 'TIMESCALE_MEDIAN_COMPARE' are in
 * place to lay the groundwork for refactoring this code to provide multiple
 * specializations for postgres datatypes: right now, only Numeric is
 * supported, which provides good generality, since all numeric types can be
 * converted into Numeric values, but poor performance, since the creation
 * and comparison of Numeric values is expensive.
 */

#include <postgres.h>
#include <utils/builtins.h>
#include <utils/numeric.h>

#include "median_quickselect.h"

typedef Datum Datum;

static inline void
quickselect_swap(Datum * a, Datum * b)
{
	Datum temp = *a;

	*a = *b;
	*b = temp;
}

/*
 * Group in place the sublist of the specified 'list' delimited by the
 * indices 'left' and 'right' into two parts, those less than than the item
 * at the specified 'pivot_index' and those greater than the item at the
 * specified 'pivot_index.
 */
static size_t
quickselect_partition(Datum * list, size_t left, size_t right, 
                      FmgrInfo * cmp_opr,
                      Oid collation,
                      size_t pivot_index)
{
	Datum pivot_value = list[pivot_index];
	size_t		store_index = left;
	size_t		i = 0;

	quickselect_swap(&list[pivot_index], &list[right]);

	for (i = left; i < right; ++i)
	{
		if (DatumGetInt32(FunctionCall2Coll(cmp_opr, collation,
                                            list[i], pivot_value)) < 0)
		{
			quickselect_swap(&list[store_index], &list[i]);
			++store_index;
		}
	}
	quickselect_swap(&list[right], &list[store_index]);
	return store_index;
}

/*
 * Iteratively partitions the 'list' in place, selecting at each point the
 * half we know the median to be in, until the partition we are in is has
 * just one item, which is the median.
 */
static Datum
quickselect_select(Datum * list,
                   size_t arr_size,
                   FmgrInfo * cmp_opr,
                   Oid collation)
{
	size_t		left = 0;
	size_t		right = arr_size - 1;
	size_t		k = ((arr_size - 1) / 2);	/* the median index */
	size_t		pivot_index = 0;

	for (;;)
	{
		if (left == right)
		{
			return list[left];
		}
		pivot_index = right - 1;
		/* to do - randomly select value between left and right */
		pivot_index = quickselect_partition(list, left, right,
                                            cmp_opr, collation,
                                            pivot_index);
		if (k == pivot_index)
		{
			return list[k];
		}
		else if (k < pivot_index)
		{
			right = pivot_index - 1;
		}
		else
		{
			left = pivot_index + 1;
		}
	}
}

/*
 * Find the median in the specified 'arr' of 'arr_size'. NOTE: Partially
 * sorts 'arr' in the process of computing the median.
 */
Datum
median_quickselect(Datum * arr, size_t arr_size,
                    FmgrInfo * cmp_opr, Oid collation)
{
	Assert(arr != NULL);
	Assert(arr_size > 0);
    Assert(cmp_opr != NULL);
	if (arr_size == 1)
	{
		return *arr;
	}

    return quickselect_select(arr, arr_size, cmp_opr, collation);
}
