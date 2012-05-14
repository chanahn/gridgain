// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Reduced variant of {@link org.gridgain.grid.GridFuture} interface. Removed asynchronous
 * listen methods which require a valid grid kernal context.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 * @param <R> Type of the result for the future.
 */
public interface GridNioFuture<R> {
    /**
     * Synchronously waits for completion of the operation and
     * returns operation result.
     *
     * @return Operation result.
     * @throws GridInterruptedException Subclass of {@link GridException} thrown if the wait was interrupted.
     * @throws GridFutureCancelledException Subclass of {@link GridException} throws if operation was cancelled.
     * @throws GridException If operation failed.
     * @throws IOException If IOException occurred while performing operation.
     */
    public R get() throws IOException, GridException;

    /**
     * Synchronously waits for completion of the operation for
     * up to the timeout specified and returns operation result.
     * This method is equivalent to calling {@link #get(long, TimeUnit) get(long, TimeUnit.MILLISECONDS)}.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @return Operation result.
     * @throws GridInterruptedException Subclass of {@link GridException} thrown if the wait was interrupted.
     * @throws GridFutureTimeoutException Subclass of {@link GridException} thrown if the wait was timed out.
     * @throws GridFutureCancelledException Subclass of {@link GridException} throws if operation was cancelled.
     * @throws GridException If operation failed.
     * @throws IOException If IOException occurred while performing operation.
     */
    public R get(long timeout) throws IOException, GridException;

    /**
     * Synchronously waits for completion of the operation for
     * up to the timeout specified and returns operation result.
     *
     * @param timeout The maximum time to wait.
     * @param unit The time unit of the {@code timeout} argument.
     * @return Operation result.
     * @throws GridInterruptedException Subclass of {@link GridException} thrown if the wait was interrupted.
     * @throws GridFutureTimeoutException Subclass of {@link GridException} thrown if the wait was timed out.
     * @throws GridFutureCancelledException Subclass of {@link GridException} throws if operation was cancelled.
     * @throws GridException If operation failed.
     * @throws IOException If IOException occurred while performing operation.
     */
    public R get(long timeout, TimeUnit unit) throws IOException, GridException;

    /**
     * Cancels this future.
     *
     * @return {@code True} if future was canceled (i.e. was not finished prior to this call).
     * @throws GridException If cancellation failed.
     */
    public boolean cancel() throws GridException;

    /**
     * Checks if operation is done.
     *
     * @return {@code True} if operation is done, {@code false} otherwise.
     */
    public boolean isDone();

    /**
     * Returns {@code true} if this operation was cancelled before it completed normally.
     *
     * @return {@code True} if this operation was cancelled before it completed normally.
     */
    public boolean isCancelled();
}
