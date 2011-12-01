// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.cache.store.writefrombehind;

import org.gridgain.grid.util.mbean.*;

/**
 * Management bean that provides general administrative and configuration information
 * on particular write-from-behind cache.
 *
 * @see GridCacheWriteFromBehindStore
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.6.0c.30112011
 */
@GridMBeanDescription("MBean that provides administrative and configuration information on write-from-behind store.")
public interface GridCacheWriteFromBehindStoreMBean {
    /**
     * Gets the maximum size of the write cache. When the count of unique keys in write cache exceeds this value,
     * the write buffer is scheduled for write to the underlying store.
     *
     * @return Buffer size that triggers flush procedure.
     */
    @GridMBeanDescription("Size of internal buffer that triggers flush procedure.")
    public int getFlushSize();

    /**
     * Gets the number of flush threads that will perform store update operations.
     *
     * @return Count of worker threads.
     */
    @GridMBeanDescription("Count of flush threads.")
    public int getFlushThreadCount();

    /**
     * Gets the cache flush frequency. All pending operations on the underlying store will be performed
     * within time interval not less then this value.
     *
     * @return Flush frequency in milliseconds.
     */
    @GridMBeanDescription("Flush frequency interval in milliseconds.")
    public int getFlushFrequency();

    /**
     * Gets the maximum count of similar (put or remove) operations that can be grouped to a single batch.
     *
     * @return Maximum size of batch.
     */
    @GridMBeanDescription("Maximum size of batch for similar operations.")
    public int getBatchSize();

    /**
     * Gets count of write buffer overflow events since initialization. Each overflow event causes
     * the ongoing flush operation to be performed synchronously.
     *
     * @return Count of cache overflow events since start.
     */
    @GridMBeanDescription("Count of cache overflow events since write-from-behind cache has started.")
    public int getCacheOverflowCounter();

    /**
     * Gets count of cache entries that are in a store-retry state. An entry is assigned a store-retry state
     * when underlying store failed due some reason and cache has enough space to retain this entry till
     * the next try.
     *
     * @return Count of entries in store-retry state.
     */
    @GridMBeanDescription("Count of cache cache entries that are currently in retry state.")
    public int getRetryEntriesCount();

    /**
     * Gets count of entries that were processed by the write-from-behind store and have not been
     * flushed to the underlying store yet.
     *
     * @return Total count of entries in cache store internal buffer.
     */
    @GridMBeanDescription("Count of cache entries that are waiting to be flushed.")
    public int getWriteBufferSize();
}
