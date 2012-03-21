// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Future that delegates to some other future.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridNioEmbeddedFuture<R> implements GridNioFuture<R> {
    /** Done flag. */
    private AtomicBoolean done = new AtomicBoolean();

    /** Cancelled flag. */
    private AtomicBoolean cancelled = new AtomicBoolean();

    /** */
    private CountDownLatch doneLatch = new CountDownLatch(1);

    /** Result. */
    @GridToStringInclude
    private GridNioFuture<R> delegate;

    /** Error. */
    private Throwable err;

    /** Future start time. */
    protected final long startTime = System.currentTimeMillis();

    /**
     * Await for done signal.
     *
     * @throws InterruptedException If interrupted.
     */
    private void latchAwait() throws InterruptedException {
        doneLatch.await();
    }

    /**
     * Signal all waiters for done condition.
     */
    private void latchCountDown() {
        doneLatch.countDown();
    }

    /**
     * Await for done signal for a given period of time (in milliseconds).
     *
     * @param time Time to wait for done signal.
     * @param unit Time unit.
     * @return {@code True} if signal was sent, {@code false} otherwise.
     * @throws InterruptedException If interrupted.
     */
    protected final boolean latchAwait(long time, TimeUnit unit) throws InterruptedException {
        return doneLatch.await(time, unit);
    }

    /**
     * @return Value of error.
     */
    protected Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) throws IOException, GridException {
        return get(timeout, MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public R get() throws IOException, GridException {
        try {
            if (doneLatch.getCount() != 0)
                latchAwait();

            if (done.get()) {
                Throwable err = this.err;

                if (err != null) {
                    if (err instanceof IOException)
                        throw (IOException)err;

                    throw U.cast(err);
                }

                return delegate.get();
            }

            throw new GridFutureCancelledException("Future was cancelled: " + this);
        }
        catch (InterruptedException e) {
            throw new GridInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws IOException, GridException {
        A.ensure(timeout >= 0, "timeout cannot be negative: " + timeout);
        A.notNull(unit, "unit");

        try {
            if (doneLatch.getCount() != 0)
                latchAwait(timeout, unit);

            if (done.get()) {
                Throwable err = this.err;

                if (err != null) {
                    if (err instanceof IOException)
                        throw (IOException)err;

                    throw U.cast(err);
                }

                return delegate.get(timeout, unit);
            }

            if (cancelled.get())
                throw new GridFutureCancelledException("Future was cancelled: " + this);

            throw new GridFutureTimeoutException("Timeout was reached before operation completed.");
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException("Got interrupted while waiting for future to complete.");
        }
    }

    /**
     * Default no-op implementation that always returns {@code false}.
     * Futures that do support cancellation should override this method
     * and call {@link #onCancelled()} callback explicitly if cancellation
     * indeed did happen.
     */
    @Override public boolean cancel() throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return done.get() || cancelled.get();
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(GridNioFuture, Throwable)} method.
     *
     * @param res Result.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(GridNioFuture<R> res) {
        return onDone(res, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(GridNioFuture, Throwable)} method.
     *
     * @param err Error.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(Throwable err) {
        return onDone(null, err);
    }

    /**
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param delegate Optional result.
     * @param err Optional error.
     * @return {@code True} if result was set by this call.
     */
    public boolean onDone(@Nullable GridNioFuture<R> delegate, @Nullable Throwable err) {
        assert delegate != null || err != null;
        
        if (done.compareAndSet(false, true)) {
            this.delegate = delegate;
            this.err = err;

            latchCountDown();

            return true;
        }

        return false;
    }

    /**
     * Callback to notify that future is cancelled.
     *
     * @return {@code True} if cancel flag was set by this call.
     */
    public boolean onCancelled() {
        boolean c = cancelled.get();

        if (c || done.get())
            return false;

        if (cancelled.compareAndSet(false, true)) {
            latchCountDown();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioEmbeddedFuture.class, this);
    }
}
