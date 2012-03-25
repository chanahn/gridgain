// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio.impl;

import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Implementation of session.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
class GridNioSessionImpl implements GridNioSession {
    /** Logger to use. */
    private GridLogger log;

    /** Closed atomic flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Metadata map. */
    private final ConcurrentMap<String, Object> meta = new ConcurrentHashMap<String, Object>();

    /** Pending write requests. */
    private final GridConcurrentLinkedDeque<GridNioFuture<?>> queue = new GridConcurrentLinkedDeque<GridNioFuture<?>>();

    /** Local connection address. */
    private final InetSocketAddress locAddr;

    /** Remote connection address */
    private final InetSocketAddress rmtAddr;

    /** Selection key associated with this session. */
    @GridToStringExclude
    private SelectionKey key;

    /** Worker index for server */
    private int selectorIdx = -1;

    /** Size counter. */
    private AtomicInteger queueSize = new AtomicInteger();

    /** Session create timestamp. */
    private long createTime;

    /** Session close timestamp. */
    private AtomicLong closeTime = new AtomicLong();

    /** Sent bytes counter. */
    private volatile long bytesSent;

    /** Received bytes counter. */
    private volatile long bytesRcvd;

    /** Last send activity timestamp. */
    private volatile long lastSndTime;

    /** Last read activity timestamp. */
    private volatile long lastRcvTime;

    /** Filter chain that will handle write and close requests. */
    private GridNioFilterChain filterChain;

    /**
     * Creates session instance.
     *
     * @param selectorIdx Selector index for this session.
     * @param filterChain Filter chain that will handle requests.
     * @param locAddr Local address.
     * @param rmtAddr Remote address.
     * @param log Logger to use.
     */
    GridNioSessionImpl(int selectorIdx, GridNioFilterChain filterChain, InetSocketAddress locAddr,
        InetSocketAddress rmtAddr, GridLogger log) {
        assert locAddr != null;
        assert rmtAddr != null;

        this.selectorIdx = selectorIdx;
        this.filterChain = filterChain;
        this.locAddr = locAddr;
        this.rmtAddr = rmtAddr;
        this.log = log.getLogger(getClass());

        createTime = System.currentTimeMillis();
        lastSndTime = createTime;
        lastRcvTime = createTime;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridNioFuture<Boolean> close() {
        try {
            return filterChain.onSessionClose(this);
        }
        catch (Exception e) {
            return new GridNioFinishedFuture<Boolean>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> send(Object msg) {
        try {
            return filterChain.onSessionWrite(this, msg);
        }
        catch (Exception e) {
            if (!(e instanceof IOException)) {
                U.warn(log, "Failed to send message (will close the session) [msg=" + msg + ", ses=" + this +
                    ", e=" + e + ']');

                close();
            }

            return new GridNioFinishedFuture<Object>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress localAddress() {
        return locAddr;
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress remoteAddress() {
        return rmtAddr;
    }

    /** {@inheritDoc} */
    @Override public long bytesSent() {
        return bytesSent;
    }

    /** {@inheritDoc} */
    @Override public long bytesReceived() {
        return bytesRcvd;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long closeTime() {
        return closeTime.get();
    }

    /** {@inheritDoc} */
    @Override public long lastReceiveTime() {
        return lastRcvTime;
    }

    /** {@inheritDoc} */
    @Override public long lastSendTime() {
        return lastSndTime;
    }

    /** {@inheritDoc} */
    @Override public <T> T meta(String key) {
        return (T)meta.get(key);
    }

    /** {@inheritDoc} */
    @Override public <T> T addMeta(String key, T val) {
        return (T)meta.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public <T> T putMetaIfAbsent(String key, T val) {
        return (T)meta.putIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public <T> T removeMeta(String key) {
        return (T)meta.remove(key);
    }

    /**
     * Atomically moves this session into a closed state.
     *
     * @return {@code True} if session was moved to a closed state,
     *      {@code false} if session was already closed.
     */
    boolean setClosed() {
        closeTime.compareAndSet(0, System.currentTimeMillis());

        return closed.compareAndSet(false, true);
    }

    /**
     * @return {@code True} if this session was closed.
     */
    boolean closed() {
        return closed.get();
    }

    /**
     * Sets selection key for this session.
     *
     * @param key Selection key.
     */
    void key(SelectionKey key) {
        assert this.key == null;

        this.key = key;
    }

    /**
     * @return Registered selection key for this session.
     */
    SelectionKey key() {
        return key;
    }

    /**
     * @return Selector index.
     */
    int selectorIndex() {
        return selectorIdx;
    }

    /**
     * Adds write future to the pending list and returns the size of the queue.
     * <p>
     * Note that separate counter for the queue size is needed because in case of concurrent
     * calls this method should return different values (when queue size is 0 and 2 concurrent calls
     * occur exactly one call will return 1)
     *
     * @param writeFut Write request to add.
     * @return Updated size of the queue.
     */
    int offerFuture(GridNioFuture<?> writeFut) {
        boolean res = queue.offer(writeFut);

        assert res : "Future was not added to queue";

        return queueSize.incrementAndGet();
    }

    /**
     * @return Message that is in the head of the queue, {@code null} if queue is empty.
     */
    @Nullable GridNioFuture<?> pollFuture() {
        GridNioFuture<?> last = queue.poll();

        if (last != null)
            queueSize.decrementAndGet();

        return last;
    }

    /**
     * Adds given amount of bytes to the sent bytes counter.
     * <p>
     * Note that this method is designed to be called in one thread only.
     *
     * @param cnt Number of bytes sent.
     */
    void bytesSent(int cnt) {
        bytesSent += cnt;
    }

    /**
     * Adds given amount ob bytes to the received bytes counter.
     * <p>
     * Note that this method is designed to be called in one thread only.
     *
     * @param cnt Number of bytes received.
     */
    void bytesReceived(int cnt) {
        bytesRcvd += cnt;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridNioSessionImpl.class, this);
    }
}
