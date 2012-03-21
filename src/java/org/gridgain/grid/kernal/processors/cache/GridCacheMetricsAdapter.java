// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Adapter for cache metrics.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridCacheMetricsAdapter implements GridCacheMetrics, Externalizable {
    /** Create time. */
    private long createTime = System.currentTimeMillis();

    /** Last read time. */
    private volatile long readTime = System.currentTimeMillis();

    /** Last update time. */
    private volatile long writeTime = System.currentTimeMillis();

    /** Number of reads. */
    private volatile int reads;

    /** Number of writes. */
    private volatile int writes;

    /** Number of hits. */
    private volatile int hits;

    /** Number of misses. */
    private volatile int misses;

    /** Number of transaction commits. */
    private volatile int txCommits;

    /** Number of transaction rollbacks. */
    private volatile int txRollbacks;

    /** Cache metrics. */
    @GridToStringExclude
    private transient GridCacheMetricsAdapter delegate;

    /**
     *
     */
    public GridCacheMetricsAdapter() {
        delegate = null;
    }

    /**
     * @param delegate Delegate cache metrics.
     */
    public GridCacheMetricsAdapter(GridCacheMetricsAdapter delegate) {
        assert delegate != null;

        this.delegate = delegate;
    }

    /**
     * @param createTime Create time.
     * @param readTime Read time.
     * @param writeTime Write time.
     * @param reads Reads.
     * @param writes Writes.
     * @param hits Hits.
     * @param misses Misses.
     * @param txCommits Transaction commits.
     * @param txRollbacks Transaction rollbacks.
     */
    public GridCacheMetricsAdapter(long createTime, long readTime, long writeTime, int reads, int writes, int hits,
        int misses, int txCommits, int txRollbacks) {
        this.createTime = createTime;
        this.readTime = readTime;
        this.writeTime = writeTime;
        this.reads = reads;
        this.writes = writes;
        this.hits = hits;
        this.misses = misses;
        this.txCommits = txCommits;
        this.txRollbacks = txRollbacks;
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(GridCacheMetricsAdapter delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long writeTime() {
        return writeTime;
    }

    /** {@inheritDoc} */
    @Override public long readTime() {
        return readTime;
    }

    /** {@inheritDoc} */
    @Override public int reads() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public int writes() {
        return writes;
    }

    /** {@inheritDoc} */
    @Override public int hits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public int misses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        readTime = System.currentTimeMillis();

        reads++;

        if (isHit)
            hits++;
        else
            misses++;

        if (delegate != null)
            delegate.onRead(isHit);
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        writeTime = System.currentTimeMillis();

        writes++;

        if (delegate != null)
            delegate.onWrite();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        txCommits++;

        if (delegate != null)
            delegate.onTxCommit();
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        txRollbacks++;

        if (delegate != null)
            delegate.onTxRollback();
    }

    /**
     * Create a copy of given metrics object.
     *
     * @param m Metrics to copy from.
     * @return Copy of given metrics.
     */
    public static GridCacheMetricsAdapter copyOf(GridCacheMetrics m) {
        assert m != null;

        return new GridCacheMetricsAdapter(
            m.createTime(),
            m.readTime(),
            m.writeTime(),
            m.reads(),
            m.writes(),
            m.hits(),
            m.misses(),
            m.txCommits(),
            m.txRollbacks()
        );
    }

    /**
     * @param m1 Metrics to merge.
     * @param m2 Metrics to merge.
     * @return Merged metrics.
     */
    public static GridCacheMetricsAdapter merge(GridCacheMetrics m1, GridCacheMetrics m2) {
        assert m1 != null;
        assert m2 != null;

        return new GridCacheMetricsAdapter(
            m1.createTime() < m2.createTime() ? m1.createTime() : m2.createTime(), // Prefer earliest.
            m1.readTime() < m2.readTime() ? m2.readTime() : m1.readTime(), // Prefer latest.
            m1.writeTime() < m2.writeTime() ? m2.writeTime() : m1.writeTime(), // Prefer latest.
            m1.reads() + m2.reads(),
            m1.writes() + m2.writes(),
            m1.hits() + m2.hits(),
            m1.misses() + m2.misses(),
            m1.txCommits() + m2.txCommits(),
            m1.txRollbacks() + m2.txRollbacks()
        );
    }

    /**
     * Clears metrics.
     *
     * NOTE: this method is for testing purposes only!
     */
    void clear() {
        createTime = System.currentTimeMillis();
        readTime = System.currentTimeMillis();
        writeTime = System.currentTimeMillis();
        reads = 0;
        writes = 0;
        hits = 0;
        misses = 0;
        txCommits = 0;
        txRollbacks = 0;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(createTime);
        out.writeLong(readTime);
        out.writeLong(writeTime);

        out.writeInt(reads);
        out.writeInt(writes);
        out.writeInt(hits);
        out.writeInt(misses);
        out.writeInt(txCommits);
        out.writeInt(txRollbacks);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        createTime = in.readLong();
        readTime = in.readLong();
        writeTime = in.readLong();

        reads = in.readInt();
        writes = in.readInt();
        hits = in.readInt();
        misses = in.readInt();
        txCommits = in.readInt();
        txRollbacks = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMetricsAdapter.class, this);
    }
}
