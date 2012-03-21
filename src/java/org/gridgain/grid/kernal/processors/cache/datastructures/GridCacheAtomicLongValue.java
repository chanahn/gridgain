// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.editions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;

/**
 * Atomic long value.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public final class GridCacheAtomicLongValue implements GridCacheInternalStorable<Long>, Externalizable, Cloneable {
    /** Value. */
    private long val;

    /** Persisted flag. */
    private boolean persisted;

    /**
     * Constructor.
     *
     * @param val Initial value.
     * @param persisted Persisted flag.
     */
    public GridCacheAtomicLongValue(long val, boolean persisted) {
        this.val = val;
        this.persisted = persisted;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheAtomicLongValue() {
        // No-op.
    }

    /**
     * @param val New value.
     */
    public void set(long val) {
        this.val = val;
    }

    /**
     * @return val Current value.
     */
    public long get() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(val);
        out.writeBoolean(persisted);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        val = in.readLong();
        persisted = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public boolean persistent() {
        return persisted;
    }

    /** {@inheritDoc} */
    @Override public Long cached2Store() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAtomicLongValue.class, this);
    }
}
