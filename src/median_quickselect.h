#ifndef TIMESCALE_MEDIAN_QUICKSELECT
#define TIMESCALE_MEDIAN_QUICKSELECT

/*
 * This median function implements the 'quickselect' algorithm, aka Hoare's
 * "Algorithm 65: Find".
 *
 * https://en.wikipedia.org/wiki/Quickselect
 *
 */

#include <postgres.h>
#include <utils/numeric.h>

/*
 * Return the median of the specified array 'arr' of 'arr_size'.
 * Asserts on invalid input (e.g., a null 'arr', or a 0-sized array).
 */
extern Datum median_quickselect(Datum *arr, size_t arr_size,
                                FmgrInfo * cmp_opr, Oid collation);

#endif
