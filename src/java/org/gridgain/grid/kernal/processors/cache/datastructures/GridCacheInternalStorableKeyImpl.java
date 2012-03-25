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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;
import org.gridgain.grid.cache.datastructures.*;

import java.io.*;

/**
 * Key is used for caching and storing {@link GridCacheAtomicLong}, {@link GridCacheAtomicSequence} and
 * {@link GridCacheAtomicReference}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridCacheInternalStorableKeyImpl<E1, R> implements GridCacheInternalStorableKey<E1, R>, Externalizable,
    Cloneable {
    /** Name of data structure. */
    private String name;

    /** Closure for transformation stored value to value which should be cached. */
    private GridClosure<E1, R> closure;

    /**
     * Default constructor.
     *
     * @param name - Name of data structure.
     * @param closure Closure for transformation stored value to value which should be cached
     *      or {@code null} if value is not persistent.
     */
    public GridCacheInternalStorableKeyImpl(String name, @Nullable GridClosure<E1, R> closure) {
        assert !F.isEmpty(name);

        this.name = name;
        this.closure = closure;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheInternalStorableKeyImpl() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override @Nullable public R stored2cache(E1 val) {
        return closure == null ? null : closure.apply(val);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return this == obj || (obj instanceof GridCacheInternalStorableKeyImpl &&
            name().equals(((GridCacheInternalStorableKey)obj).name()));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, name);
    }

    @Override public void readExternal(ObjectInput in) throws IOException {
        name = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheInternalStorableKeyImpl.class, this);
    }
}
