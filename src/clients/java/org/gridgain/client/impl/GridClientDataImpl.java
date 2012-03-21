// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.client.balancer.*;
import org.gridgain.client.util.*;

import java.util.*;

/**
 * Data projection that serves one cache instance and handles communication errors.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridClientDataImpl extends GridClientAbstractProjection<GridClientDataImpl> implements GridClientData {
    /** Cache name. */
    private String cacheName;

    /**
     * Creates a data projection.
     *
     * @param cacheName Cache name for projection.
     * @param client Client instance to resolve connection failures.
     * @param nodes Pinned nodes.
     * @param filter Node filter.
     * @param balancer Pinned node balancer.
     */
    public GridClientDataImpl(String cacheName, GridClientImpl client, Collection<GridClientNode> nodes,
        GridClientPredicate<GridClientNode> filter, GridClientLoadBalancer balancer) {
        super(client, nodes, filter, balancer);

        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public GridClientData pinNodes(GridClientNode node, GridClientNode... nodes) throws GridClientException {
        Collection<GridClientNode> n = new ArrayList<GridClientNode>(nodes.length + 1);

        n.add(node);
        n.addAll(Arrays.asList(nodes));

        return createProjection(n, null, null);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> pinnedNodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean put(K key, V val) throws GridClientException {
        return putAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> putAsync(final K key, final V val)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override
            public GridClientFuture<Boolean> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cachePut(cacheName, key, val);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void putAll(Map<K, V> entries) throws GridClientException {
        putAllAsync(entries).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<?> putAllAsync(final Map<K, V> entries)
        throws GridServerUnreachableException, GridClientClosedException {
        if (entries.isEmpty())
            return new GridClientFinishedFuture<Boolean>(false);

        K key = GridClientUtils.first(entries.keySet());

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cachePutAll(cacheName, entries);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> V get(K key) throws GridClientException {
        return this.<K, V>getAsync(key).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<V> getAsync(final K key) throws GridServerUnreachableException,
        GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<V>() {
            @Override
            public GridClientFuture<V> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheGet(cacheName, key);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Map<K, V> getAll(Collection<K> keys) throws GridClientException {
        return this.<K, V>getAllAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Map<K, V>> getAllAsync(final Collection<K> keys)
        throws GridServerUnreachableException, GridClientClosedException {
        if (keys.isEmpty())
            return new GridClientFinishedFuture<Map<K, V>>(Collections.<K, V>emptyMap());

        K key = GridClientUtils.first(keys);

        return withReconnectHandling(new ClientProjectionClosure<Map<K, V>>() {
            @Override
            public GridClientFuture<Map<K, V>> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheGetAll(cacheName, keys);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K> boolean remove(K key) throws GridClientException {
        return removeAsync(key).get();
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<Boolean> removeAsync(final K key) throws GridServerUnreachableException,
        GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override
            public GridClientFuture<Boolean> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheRemove(cacheName, key);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K> void removeAll(Collection<K> keys) throws GridClientException {
        removeAllAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<?> removeAllAsync(final Collection<K> keys)
        throws GridServerUnreachableException, GridClientClosedException {
        if (keys.isEmpty())
            return new GridClientFinishedFuture<Boolean>(false);

        K key = GridClientUtils.first(keys);

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheRemoveAll(cacheName, keys);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean replace(K key, V val) throws GridClientException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> replaceAsync(final K key, final V val)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override
            public GridClientFuture<Boolean> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheReplace(cacheName, key, val);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean cas(K key, V val1, V val2) throws GridClientException {
        return casAsync(key, val1, val2).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> casAsync(final K key, final V val1, final V val2)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override
            public GridClientFuture<Boolean> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheCompareAndSet(cacheName, key, val1, val2);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K> UUID affinity(K key) throws GridClientException {
        GridClientDataAffinity affinity = client.affinity(cacheName);

        if (affinity == null)
            return null;

        Collection<? extends GridClientNode> prj = projectionNodes();

        if (prj.isEmpty())
            throw new GridClientException("Failed to get affinity node (projection node set for cache is empty): " +
                cacheName());

        GridClientNode node = affinity.node(key, prj);

        assert node != null;

        return node.nodeId();
    }

    /** {@inheritDoc} */
    @Override public Map<String, ? extends Number> metrics() throws GridClientException {
        return metricsAsync().get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<Map<String, ? extends Number>> metricsAsync()
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<Map<String, ? extends Number>>() {
            @Override public GridClientFuture<Map<String, ? extends Number>> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheMetrics(cacheName, null);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <K> Map<String, ? extends Number> metrics(K key) throws GridClientException {
        return metricsAsync(key).get();
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<Map<String, ? extends Number>> metricsAsync(final K key)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<Map<String, ? extends Number>>() {
            @Override public GridClientFuture<Map<String, ? extends Number>> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheMetrics(cacheName, key);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override protected GridClientDataImpl createProjectionImpl(Collection<GridClientNode> nodes,
        GridClientPredicate<GridClientNode> filter, GridClientLoadBalancer balancer) {
        return new GridClientDataImpl(cacheName, client, nodes, filter, balancer);
    }
}
