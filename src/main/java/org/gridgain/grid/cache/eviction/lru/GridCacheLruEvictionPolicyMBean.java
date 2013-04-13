// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.lru;

import org.gridgain.grid.util.mbean.*;

/**
 * MBean for {@code LRU} eviction policy.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.14072011
 */
@GridMBeanDescription("MBean for LRU cache eviction policy.")
public interface GridCacheLruEvictionPolicyMBean {
    /**
     * Gets maximum allowed cache size.
     *
     * @return Maximum allowed cache size.
     */
    @GridMBeanDescription("Maximum allowed cache size.")
    public int getMaxSize();

    /**
     * Sets maximum allowed cache size.
     *
     * @param max Maximum allowed cache size.
     */
    @GridMBeanDescription("Sets maximum allowed cache size.")
    public void setMaxSize(int max);

    /**
     * Gets current {@code HIRS} queue size.
     *
     * @return Current {@code HIRS} queue size.
     */
    @GridMBeanDescription("Current queue size.")
    public int getCurrentSize();

    /**
     * Gets number of voided nodes that remain on queue to be removed later for better concurrency.
     * This concept is similar to Garbage Collection Eden space, hence the name.
     *
     * @return Number of voided nodes that remain on queue.
     */
    @GridMBeanDescription("Current queue eden size.")
    public int getCurrentEdenSize();

    /**
     * Clear eden space of internal queue.
     */
    @GridMBeanDescription("Clears Eden space from internal queue.")
    public void gc();

    /**
     * Gets average time spent on GC'ing queue.
     *
     * @return Average time spent on GC'ing queue.
     */
    @GridMBeanDescription("Average time spent on GC'ing queue.")
    public long getAverageGcTime();

    /**
     * Gets number of LRU queue nodes GC'ed by this policy.
     *
     * @return Number of LRU queue nodes GC'ed by this policy.
     */
    @GridMBeanDescription("Number of queue nodes GC'ed.")
    public long getNodesGced();

    /**
     * Gets number of LRU queue nodes created by this policy.
     *
     * @return Number of LRU queue nodes created by this policy.
     */
    @GridMBeanDescription("Number of queue nodes created.")
    public long getNodesCreated();

    /**
     * Gets number of LRU queue GC calls executed by this policy.
     *
     * @return Number of LRU queue GC calls executed by this policy.
     */
    @GridMBeanDescription("Number of GC calls.")
    public long getGcCalls();

    /**
     * Gets formatted representation of internal queue.
     *
     * @return Formatted representation of internal queue.
     */
    @GridMBeanDescription("Formatted representation of internal queue.")
    public String queueFormatted();
}
