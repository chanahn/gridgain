// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.gridgain.client.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Future adapter.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridClientFutureAdapter<R> implements GridClientFuture<R> {
    /** Done flag. */
    private final AtomicBoolean done = new AtomicBoolean(false);

    /** Latch. */
    private final CountDownLatch doneLatch = new CountDownLatch(1);

    /** Result. */
    private R res;

    /** Error. */
    private Throwable err;

    /** {@inheritDoc} */
    @Override public R get() throws GridClientException {
        try {
            doneLatch.await();
        }
        catch (InterruptedException e) {
            throw new GridClientException("Operation was interrupted.", e);
        }

        if (err != null)
            throw new GridClientException(err);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return done.get();
    }

    /**
     * Callback to notify that future is finished successfully.
     *
     * @param res Result (can be {@code null}).
     */
    public void onDone(R res) {
        if (done.compareAndSet(false, true)) {
            this.res = res;

            doneLatch.countDown();
        }
    }

    /**
     * Callback to notify that future is finished with error.
     *
     * @param err Error (can't be {@code null}).
     */
    public void onDone(Throwable err) {
        assert err != null;

        if (done.compareAndSet(false, true)) {
            this.err = err;

            doneLatch.countDown();
        }
    }
}
