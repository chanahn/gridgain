// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

/**
 * Represents a future that already have a result and will never wait.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridClientFinishedFuture<R> implements GridClientFuture<R> {
    /** Future result. */
    private R res;

    /** Future exception. */
    private GridClientException error;

    /**
     * Creates succeeded future with given result.
     *
     * @param res Future result.
     */
    public GridClientFinishedFuture(R res) {
        this.res = res;
    }

    /**
     * Creates failed future with given error.
     *
     * @param error Future error.
     */
    public GridClientFinishedFuture(GridClientException error) {
        this.error = error;
    }

    /** {@inheritDoc} */
    @Override public R get() throws GridClientException {
        if (error != null)
            throw error;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return true;
    }
}
