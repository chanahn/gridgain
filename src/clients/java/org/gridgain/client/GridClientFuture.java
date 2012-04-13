// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

/**
 * Future for asynchronous operations.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.2c.12042012
 */
public interface GridClientFuture<R> {
    /**
     * Synchronously waits for completion and returns result.
     *
     * @return Result.
     * @throws GridClientException In case of error.
     */
    public R get() throws GridClientException;

    /**
     * Checks if future is done.
     *
     * @return Whether future is done.
     */
    public boolean isDone();
}
