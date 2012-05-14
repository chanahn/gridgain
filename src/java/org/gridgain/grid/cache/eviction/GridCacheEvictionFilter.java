// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;

/**
 * Eviction filter to specify which entries should not be evicted
 * (except explicit evict by calling {@link GridCacheEntry#evict(GridPredicate[])}).
 * If {@link #evictAllowed(GridCacheEntry)} method returns {@code false} then eviction
 * policy will not be notified and entry will never be evicted.
 * <p>
 * Eviction filter can be configured via {@link GridCacheConfiguration#getEvictionFilter()}
 * configuration property. Default value is {@code null} which means that all
 * cache entries will be tracked by eviction policy.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public interface GridCacheEvictionFilter<K, V> {
    /**
     * Checks if entry may be evicted from cache.
     *
     * @param entry Cache entry.
     * @return {@code True} if it is allowed to evict this entry.
     */
    public boolean evictAllowed(GridCacheEntry<K, V> entry);
}
