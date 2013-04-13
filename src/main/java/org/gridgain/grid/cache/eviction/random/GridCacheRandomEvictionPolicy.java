// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.random;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import javax.management.*;
import java.util.concurrent.atomic.*;

/**
 * Cache eviction policy which will select random cache entry for eviction if cache
 * size exceeds the {@link #getMaxSize()} parameter. This implementation is
 * extremely light weight, lock-free, and does not create any data structures to maintain
 * any order for eviction.
 * <p>
 * Random eviction will provide the best performance over any key set in which every
 * key has the same probability of being accessed.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.14072011
 */
public class GridCacheRandomEvictionPolicy<K, V> implements GridCacheEvictionPolicy<K, V>,
    GridCacheRandomEvictionPolicyMBean {
    /** MBean server. */
    @GridMBeanServerResource
    @GridToStringExclude
    private MBeanServer jmx;

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Init flag. */
    private AtomicBoolean init = new AtomicBoolean(false);

    /** Maximum size. */
    private volatile int max = -1;

    /**
     * Constructs random eviction policy with all defaults.
     */
    public GridCacheRandomEvictionPolicy() {
        // No-op.
    }

    /**
     * Constructs random eviction policy with maximum size.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public GridCacheRandomEvictionPolicy(int max) {
        A.ensure(max > 0, "max > 1");

        this.max = max;
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public int getMaxSize() {
        return max;
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public void setMaxSize(int max) {
        A.ensure(max > 0, "max > 1");

        this.max = max;
    }

    /**
     * @param entry Entry to get info from.
     */
    private void registerMbean(GridCacheEntry<K, V> entry) {
        if (init.compareAndSet(false, true))
            CU.registerEvictionMBean(log, jmx, this, GridCacheRandomEvictionPolicyMBean.class, entry);
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<K, V> entry) {
        registerMbean(entry);

        GridCache<K, V> cache = entry.parent().cache();

        int size = cache.keySize();

        for (int i = max; i < size; i++) {
            GridCacheEntry<K, V> e = cache.randomEntry();

            if (e != null)
                e.evict();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheRandomEvictionPolicy.class, this);
    }
}
