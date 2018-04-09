/*
 * This median function implements the 'quickselect' algorithm, aka Hoare's
 * "Algorithm 65: Find", with the 'median of medians' algorithm used to
 * find a quickselect pivot that guarantees linear time selection.
 *
 * https://en.wikipedia.org/wiki/Quickselect
 * https://en.wikipedia.org/wiki/Median_of_medians
 *
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
 * Compare the two specified Datum 'a' and 'b' using 'cmp_opr' and 'collation'
 */
static inline int 
quickselect_compare(Datum a, Datum b, FmgrInfo * cmp_opr, Oid collation)
{
    return DatumGetInt32(FunctionCall2Coll(cmp_opr, collation, a, b));
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
		if (quickselect_compare(list[i], pivot_value, cmp_opr, collation) < 0)
		{
			quickselect_swap(&list[store_index], &list[i]);
			++store_index;
		}
	}
	quickselect_swap(&list[right], &list[store_index]);
	return store_index;
}

/*
 * Perform a 'brute force' selection via insertion sort on the
 * sub-array of the specified 'list' delimited by the specified
 * indices 'left' and 'right' using the 'cmp_opr' and 'collation'
 * to compare the items.
 */
static size_t
quickselect_brute_force_select(Datum *list, size_t left, size_t right,
                               FmgrInfo *cmp_opr, Oid collation)
{
    size_t i = left + 1;
    size_t j = 0;

    while (i < right)
    {
        j = i;
        while ((j > left) &&
               (quickselect_compare(list[j-1], list[j],
                                    cmp_opr, collation) > 0))
        {
            quickselect_swap(&list[j], &list[j-1]);
            --j;
        }
        ++i;
    }
    
    return ((right - left) / 2);
}

/*
 * Return a random pivot between left and right.
 */

/*
static size_t
quickselect_get_pivot_random(size_t left, size_t right)
{
    return (left + (random() % (right - left)));
}
*/

/* Forward declaration for quickselect_get_pivot_median_of_medians */
static size_t
quickselect_select(Datum * list,
                   size_t arr_size,
                   FmgrInfo * cmp_opr,
                   Oid collation);

/*
 * The "median of medians" algorithm - return a suitable pivot
 * index to use in the subarray of the specified 'list' delimited
 * by the indices 'left' and 'right'.
 */
static size_t
quickselect_get_pivot_median_of_medians(Datum * list, size_t left, size_t right,
                                        FmgrInfo *cmp_opr, Oid collation)
{
    size_t sub_left     = 0;
    size_t sub_right    = 0;
    size_t median_index = 0;
    size_t number_of_medians = 0;

    size_t len_subarray = 0;

    Datum result = { 0 };

    size_t i = 0;

    Datum temp_array[5] = { 0 };
    Datum *temp_medians = NULL;

    /* base case */
    if ((right - left) <= 5)
    {
        len_subarray = right - left;
        memcpy(temp_array, list + left, len_subarray * sizeof(Datum));
        result = temp_array[quickselect_brute_force_select(temp_array, 0, len_subarray,
                                                           cmp_opr, collation)];
    }

    else
    {
        temp_medians = (Datum *) palloc(sizeof(Datum) * (((right - left) / 5) + 1));

        /* collect the medians of every group of 5 records
         * and move them into temp_medians */
        sub_left  = left;
        number_of_medians = 0;
        while (sub_right < right)
        {
            sub_right = sub_left + 4;
            if (sub_right > right)
            {
                sub_right = right;
            }

            len_subarray = sub_right - sub_left;
            memcpy(temp_array, list + sub_left, len_subarray * sizeof(Datum));
            median_index = quickselect_brute_force_select(temp_array, 0, len_subarray,
                                                          cmp_opr, collation);

            temp_medians[number_of_medians] = temp_array[median_index];

            ++number_of_medians;
            sub_left += 5;
        }

        /* compute the medians of the medians... */
        result = temp_medians[quickselect_select(temp_medians, number_of_medians,
                                                 cmp_opr, collation)];
        pfree(temp_medians);
    }

    /* find index of median of medians... */
    for (i = left; i < right; ++i)
    {
        if (quickselect_compare(list[i], result, cmp_opr, collation) == 0)
        {
            return i;
        }
    }
    Assert(false);
    return -1;
}

/*
 * Iteratively partitions the 'list' in place, selecting at each point the
 * half we know the median to be in, until the partition we are in is has
 * just one item, which is the median.
 */
static size_t
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
			return left;
		}
        /* pivot_index = quickselect_get_pivot_random(left, right); */
		pivot_index = quickselect_get_pivot_median_of_medians(list, left, right,
                                                              cmp_opr, collation);
		pivot_index = quickselect_partition(list, left, right,
                                            cmp_opr, collation,
                                            pivot_index);
		if (k == pivot_index)
		{
			return k;
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

    return arr[quickselect_select(arr, arr_size, cmp_opr, collation)];
}
