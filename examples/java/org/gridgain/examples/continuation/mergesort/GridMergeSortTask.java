package org.gridgain.examples.continuation.mergesort;

import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

/**
 * A task that performs distributed Merge Sort.
 *
 * @author @java.author
 * @version @java.version
 */
class GridMergeSortTask extends GridTaskSplitAdapter<int[], int[]> {
    /** Injected Grid instance. */
    @GridInstanceResource
    private Grid grid;

    /**
     * Receives the array to sort, splits it into 2 arrays, and returns 2
     * jobs that perform that task recursively, each for the corresponding part
     * of the array. Each recursive task will return a sorted array.
     *
     * Because this is a recursive algorithm and we cannot hold threads are every
     * recursion step, we use the <i>continuation</i> mechanism
     * ({@link GridJobContext} methods {@code holdcc()} and {@code callcc()})
     * to pause the parent tasks while the child tasks are running. Otherwise we may
     * run out of threads.
     *
     * @param gridSize Number of available grid nodes. Note that returned number of
     *      jobs can be less, equal or greater than this grid size.
     * @param initArr Array to sort.
     * @return 2 jobs that will run the sort recursively for each part of the array.
     */
    @Override protected Collection<GridJob> split(int gridSize, int[] initArr) {
        Collection<GridJob> jobs = new LinkedList<GridJob>();

        for (final int[] arr : splitArray(initArr)) {
            jobs.add(new GridJobAdapterEx() {
                // Auto-inject job context.
                @GridJobContextResource
                private GridJobContext jobCtx;

                // Task execution result future.
                private GridTaskFuture<int[]> fut;

                @Override public Object execute() throws GridException {
                    if (arr.length == 1)
                        return arr;

                    // Future is null before holdcc() is called and
                    // not null after callcc() is called.
                    if (fut == null) {
                        // Launch the recursive child task asynchronously.
                        fut = grid.execute(new GridMergeSortTask(), arr);

                        // Add a listener to the future, that will resume the
                        // parent task once the child one is completed.
                        fut.listenAsync(new CI1<GridFuture<int[]>>() {
                            @Override public void apply(GridFuture<int[]> fut) {
                                // CONTINUATION:
                                // =============
                                // Resume suspended job execution.
                                jobCtx.callcc();
                            }
                        });

                        // CONTINUATION:
                        // =============
                        // Suspend job execution to be continued later and
                        // release the executing thread.
                        return jobCtx.holdcc();
                    }
                    else {
                        assert fut.isDone();

                        // Return the result of a completed child task.
                        return fut.get();
                    }
                }
            });
        }

        return jobs;
    }

    /**
     * This method is called when both child jobs are completed, and is a
     * Reduce step of Merge Sort algorithm.
     *
     * On this step we do a merge of 2 sorted arrays, produced by child tasks,
     * into a 1 sorted array.
     *
     * @param results The child task execution results (sorted arrays).
     * @return A merge result: single sorted array.
     */
    @Override public int[] reduce(List<GridJobResult> results) {
        if (results.size() == 1) // This is in case we have a single-element array.
            return results.get(0).getData();

        assert results.size() == 2;

        int[] arr1 = results.get(0).getData();
        int[] arr2 = results.get(1).getData();

        return mergeArrays(arr1, arr2);
    }

    /**
     * Splits the array into two parts.
     *
     * If array size is odd, then the second part is one element
     * greater than the first one. Otherwise, the parts have
     * equal size.
     *
     * @param arr Array to split.
     * @return Split result: a collection of 2 arrays.
     */
    private static Iterable<int[]> splitArray(int[] arr) {
        int len1 = arr.length / 2;
        int len2 = len1 + arr.length % 2;

        int[] a1 = new int[len1];
        int[] a2 = new int[len2];

        System.arraycopy(arr, 0, a1, 0, len1);
        System.arraycopy(arr, len1, a2, 0, len2);

        X.println("Split array [len1=" + a1.length + ", len2=" + a2.length + ']');

        return Arrays.asList(a1, a2);
    }

    /**
     * Performs a merge of 2 arrays. This method runs the element-by-element
     * comparison of specified arrays and stacks the least elements into a
     * resulting array.
     *
     * @param arr1 First array.
     * @param arr2 Second array.
     * @return The merged array, in which any element from the first half is less or equal
     *      than any element from the second half.
     */
    private static int[] mergeArrays(int[] arr1, int[] arr2) {
        int[] ret = new int[arr1.length + arr2.length];

        int i1 = 0;
        int i2 = 0;

        // Merge 2 arrays into a resulting array
        for (int i = 0; i < ret.length; i++) {
            if (i1 >= arr1.length) {
                System.arraycopy(arr2, i2, ret, i, arr2.length - i2); // Copy the remainder of an array.

                break;
            }
            else if (i2 >= arr2.length) {
                System.arraycopy(arr1, i1, ret, i, arr1.length - i1); // Copy the remainder of an array.

                break;
            }
            else
                ret[i] = arr1[i1] <= arr2[i2] ? arr1[i1++] : arr2[i2++];
        }

        X.println("Merged arrays [resLen=" + ret.length + ", len1=" + arr1.length + ", len2=" + arr2.length + ']');

        return ret;
    }
}
