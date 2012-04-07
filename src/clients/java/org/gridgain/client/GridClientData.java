// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import java.util.*;

/**
 * A data projection of grid client. Contains various methods for cache operations ant metrics retrieval.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public interface GridClientData {
    /**
     * Gets name of the remote cache.
     *
     * @return Name of the remote cache.
     */
    public String cacheName();

    /**
     * Gets client data which will only contact specified remote grid node. By default, remote node
     * is determined based on {@link GridClientDataAffinity} provided - this method allows
     * to override default behavior and use only specified server for all cache operations.
     * <p>
     * Use this method when there are other than {@code key-affinity} reasons why a certain
     * node should be contacted.
     *
     * @param node Node to be contacted.
     * @param nodes Optional additional nodes.
     * @return Client data which will only contact server with given node ID.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientData pinNodes(GridClientNode node, GridClientNode... nodes) throws GridClientException;

    /**
     * Gets pinned node or {@code null} if no node was pinned.
     *
     * @return Pinned node.
     */
    public Collection<GridClientNode> pinnedNodes();

    /**
     * Puts value to default cache.
     *
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually put to cache.
     * @throws GridClientException In case of error.
     */
    public <K, V> boolean put(K key, V val) throws GridClientException;

    /**
     * Asynchronously puts value to default cache.
     *
     * @param key key.
     * @param val Value.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> GridClientFuture<Boolean> putAsync(K key, V val) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Puts entries to default cache.
     *
     * @param entries Entries.
     * @throws GridClientException In case of error.
     */
    public <K, V> void putAll(Map<K, V> entries) throws GridClientException;

    /**
     * Asynchronously puts entries to default cache.
     *
     * @param entries Entries.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> GridClientFuture<?> putAllAsync(Map<K, V> entries) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Gets value from default cache.
     *
     * @param key Key.
     * @return Value.
     * @throws GridClientException In case of error.
     */
    public <K, V> V get(K key) throws GridClientException;

    /**
     * Asynchronously gets value from default cache.
     *
     * @param key key.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> GridClientFuture<V> getAsync(K key) throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Gets entries from default cache.
     *
     * @param keys Keys.
     * @throws GridClientException In case of error.
     * @return Entries.
     */
    public <K, V> Map<K, V> getAll(Collection<K> keys) throws GridClientException;

    /**
     * Asynchronously gets entries from default cache.
     *
     * @param keys Keys.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> GridClientFuture<Map<K, V>> getAllAsync(Collection<K> keys) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Removes value from default cache.
     *
     * @param key Key.
     * @return Whether value was actually removed.
     * @throws GridClientException In case of error.
     */
    public <K> boolean remove(K key) throws GridClientException;

    /**
     * Asynchronously removes value from default cache.
     *
     * @param key key.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K> GridClientFuture<Boolean> removeAsync(K key) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Removes entries from default cache.
     *
     * @param keys Keys.
     * @throws GridClientException In case of error.
     */
    public <K> void removeAll(Collection<K> keys) throws GridClientException;

    /**
     * Asynchronously removes entries from default cache.
     *
     * @param keys Keys.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K> GridClientFuture<?> removeAllAsync(Collection<K> keys) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Replaces value in default cache.
     *
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually replaced.
     * @throws GridClientException In case of error.
     */
    public <K, V> boolean replace(K key, V val) throws GridClientException;

    /**
     * Asynchronously replaces value in default cache.
     *
     * @param key key.
     * @param val Value.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> GridClientFuture<Boolean> replaceAsync(K key, V val) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Sets entry value to {@code val1} if current value is {@code val2}.
     * <p>
     * If {@code val1} is {@code null} and {@code val2} is equal to current value,
     * entry is removed from cache.
     * <p>
     * If {@code val2} is {@code null}, entry is created if it doesn't exist.
     * <p>
     * If both {@code val1} and {@code val2} are {@code null}, entry is removed.
     *
     * @param key Key.
     * @param val1 Value to set.
     * @param val2 Check value.
     * @return Whether value of entry was changed.
     * @throws GridClientException In case of error.
     */
    public <K, V> boolean cas(K key, V val1, V val2) throws GridClientException;

    /**
     * Asynchronously sets entry value to {@code val1} if current value is {@code val2}.
     * <p>
     * If {@code val1} is {@code null} and {@code val2} is equal to current value,
     * entry is removed from cache.
     * <p>
     * If {@code val2} is {@code null}, entry is created if it doesn't exist.
     * <p>
     * If both {@code val1} and {@code val2} are {@code null}, entry is removed.
     *
     * @param key Key.
     * @param val1 Value to set.
     * @param val2 Check value.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> GridClientFuture<Boolean> casAsync(K key, V val1, V val2)
        throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Gets affinity node ID for provided key. This method will return {@code null} if no
     * affinity was configured for the given cache or there are no nodes in topology with
     * cache enabled.
     *
     * @param key Key.
     * @return Node ID.
     * @throws GridClientException In case of error.
     */
    public <K> UUID affinity(K key) throws GridClientException;

    /**
     * Gets metrics for default cache.
     *
     * @return Cache metrics.
     * @throws GridClientException In case of error.
     */
    public GridClientDataMetrics metrics() throws GridClientException;

    /**
     * Asynchronously gets metrics for default cache.
     *
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientFuture<GridClientDataMetrics> metricsAsync() throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Gets metrics for entry.
     *
     * @param key Key.
     * @return Entry metrics.
     * @throws GridClientException In case of error.
     */
    public <K> GridClientDataMetrics metrics(K key) throws GridClientException;

    /**
     * Asynchronously gets metrics for entry.
     *
     * @param key Key.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K> GridClientFuture<GridClientDataMetrics> metricsAsync(K key)
        throws GridServerUnreachableException, GridClientClosedException;
}
