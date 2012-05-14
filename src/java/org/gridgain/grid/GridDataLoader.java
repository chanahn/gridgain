// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;

import java.util.concurrent.*;

/**
 * Data loader is responsible for loading external data into cache. It achieves it by
 * properly buffering updates and properly mapping keys to nodes responsible for the data
 * to make sure that there is the least amount of data movement possible and optimal
 * network and memory utilization.
 * <p>
 * Note that loader will load data concurrently by multiple internal threads, so the
 * data may get to remote nodes in different order from which it was added to
 * the loader.
 * <p>
 * Also note that {@code GridDataLoader} is not the only way to load data into cache.
 * Alternatively you can use {@link GridCache#loadCache(GridPredicate2, long, Object...)}
 * method to load data from underlying data store. You can also use standard
 * cache {@code put(...)} and {@code putAll(...)} operations as well, but they most
 * likely will not perform as well as this class for loading data. And finally,
 * data can be loaded from underlying data store on demand, whenever it is accessed -
 * for this no explicit data loading step is needed.
 * <p>
 * {@code GridDataLoader} supports the following configuration properties:
 * <ul>
 *  <li>
 *      {@code perNodeBufferSize} - when entries are added to data loader via
 *      {@link #addData(Object, Object)} method, they are not sent to data grid right
 *      away and are buffered internally for better performance and network utilization.
 *      This setting controls the size of internal per-node buffer before buffered data
 *      is sent to remote node. Default is defined by {@link #DFLT_PER_NODE_BUFFER_SIZE}
 *      value.
 *  </li>
 *  <li>
 *      {@code perNodeParallelLoadOperations} - sometimes data may be added
 *      to the data loader via {@link #addData(Object, Object)} method faster than it can
 *      be put in cache. In this case, new buffered load messages are sent to remote nodes
 *      before responses from previous ones are received. This could cause unlimited heap
 *      memory utilization growth on local and remote nodes. To control memory utilization,
 *      this setting limits maximum allowed number of parallel buffered load messages that
 *      are being processed on remote nodes. If this number is exceeded, then
 *      {@link #addData(Object, Object)} method will block to control memory utilization.
 *  </li>
 * </ul>
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public interface GridDataLoader<K, V> {
    /** Default max concurrent put operations count. */
    public static final int DFLT_MAX_PARALLEL_OPS = 64;

    /** Default per node buffer size. */
    public static final int DFLT_PER_NODE_BUFFER_SIZE = 100;

    /**
     * Name of cache to load data to.
     *
     * @return Cache name.
     */
    public String cacheName();

    /**
     * Gets size of per node key-value pairs buffer.
     *
     * @return Per node buffer size.
     */
    public int perNodeBufferSize();

    /**
     * Sets size of per node key-value pairs buffer.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_PER_NODE_BUFFER_SIZE}.
     *
     * @param bufSize Per node buffer size.
     */
    public void perNodeBufferSize(int bufSize);

    /**
     * Gets maximum number of parallel load operations for a single node.
     *
     * @return Maximum number of parallel load operations for a single node.
     */
    public int perNodeParallelLoadOperations();

    /**
     * Sets maximum number of parallel load operations for a single node.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_PARALLEL_OPS}.
     *
     * @param parallelOps Maximum number of parallel load operations for a single node.
     */
    public void perNodeParallelLoadOperations(int parallelOps);

    /**
     * Gets future for this loading process. This future completes whenever method
     * {@link #close(boolean)} completes. By attaching listeners to this future
     * it is possible to get asynchronous notifications for completion of this
     * loading process.
     *
     * @return Future for this loading process.
     */
    public GridFuture<?> future();

    /**
     * Optional deploy class for peer deployment. All classes loaded by a data loader
     * must be class-loadable from the same class-loader. GridGain will make the best
     * effort to detect the most suitable class-loader for data loading. However,
     * in complex cases, where compound or deeply nested class-loaders are used,
     * it is best to specify a deploy class which can be any class loaded by
     * the class-loader for given data.
     *
     * @param depCls Any class loaded by the class-loader for given data.
     */
    public void deployClass(Class<?> depCls);

    /**
     * Atomically loads value which is a function of a previous value stored in cache. This method
     * has the following effect: {@code "cache.putx(key, function(cache.get(key))"} where
     * function is the closure passed into this method.
     * <p>
     * Note that if several closures are added for the same key, all of them will be applied.
     * <p>
     * See {@link #addData(Object, Object)} for threading issues.
     *
     * @param key Key.
     * @param clo Closure to apply.
     * @throws GridException If failed to map key to node.
     * @throws GridInterruptedException If thread has been interrupted.
     * @throws IllegalStateException if grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     */
    public void addData(K key, GridClosure<V, V> clo) throws GridException, GridInterruptedException,
        IllegalStateException;

    /**
     * Loads data on remote node using passed in closure which serves as value factory. This method is
     * useful for cases when there is no real benefit in generating value on the sender side and
     * value can be generated on receiver side. This method has the following effect:
     * {@code "cache.putx(key, c.call())"}.
     *
     * @param key Key.
     * @param c Callable that returns result of type V.
     * @throws GridException If failed to map key to node.
     * @throws GridInterruptedException If thread has been interrupted.
     * @throws IllegalStateException if grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     */
    public void addData(K key, Callable<V> c) throws GridException, GridInterruptedException,
        IllegalStateException;

    /**
     * Adds data for loading on remote node. This method can be called for multiple
     * threads in parallel to speed up loading if needed.
     * <p>
     * Note that loader will load data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the loader.
     *
     * @param key Key.
     * @param val Value.
     * @throws GridException If failed to map key to node.
     * @throws GridInterruptedException If thread has been interrupted.
     * @throws IllegalStateException if grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     */
    public void addData(K key, V val) throws GridException, GridInterruptedException, IllegalStateException;

    /**
     * Loads any remaining data and closes this loader.
     *
     * @param cancel {@code True} to cancel ongoing loading operations.
     * @throws GridException If failed to map key to node.
     * @throws GridInterruptedException If thread has been interrupted.
     */
    public void close(boolean cancel) throws GridException, GridInterruptedException;
}
