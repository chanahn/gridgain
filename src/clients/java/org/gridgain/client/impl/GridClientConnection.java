// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

import javax.net.ssl.*;
import java.net.*;
import java.util.*;

/**
 * Facade for all possible network communications between client and server. Introduced to hide
 * protocol implementation (TCP, HTTP) from client code.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public abstract class GridClientConnection {
    /** Topology */
    protected GridClientTopology top;

    /** Client id. */
    protected final UUID clientId;

    /** Server address this connection connected to */
    private InetSocketAddress srvAddr;

    /** SSL context to use if ssl is enabled. */
    private SSLContext sslCtx;

    /** Client credentials. */
    private Object cred;

    /**
     * Creates a facade.
     *
     * @param clientId Client identifier.
     * @param srvAddr Server address this connection connected to.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param top Topology.
     * @param cred Client credentials.
     */
    protected GridClientConnection(UUID clientId, InetSocketAddress srvAddr, SSLContext sslCtx, GridClientTopology top,
        Object cred) {
        this.clientId = clientId;
        this.srvAddr = srvAddr;
        this.top = top;
        this.sslCtx = sslCtx;
        this.cred = cred;
    }

    /**
     * Closes connection facade.
     *
     * @param waitCompletion If {@code true} this method will wait until all pending requests are handled.
     */
    abstract void close(boolean waitCompletion);

    /**
     * Closes connection facade if no requests are in progress.
     *
     * @return {@code True} if no requests were in progress and client was closed, {@code false} otherwise.
     */
    abstract boolean closeIfIdle();

    /**
     * Gets timestamp of last network activity.
     *
     * @return Timestamp of last network activity returned by {@link System#currentTimeMillis()}
     */
    abstract long lastNetworkActivityTimestamp();

    /**
     * @return Server address this connection connected to.
     */
    public InetSocketAddress serverAddress() {
        return srvAddr;
    }

    /**
     * Encodes cache flags to bit map.
     *
     * @param flagSet Set of flags to be encoded.
     * @return Bit map.
     */
    public static int encodeCacheFlags(Collection<GridClientCacheFlag> flagSet) {
        int bits = 0;

        if (flagSet.contains(GridClientCacheFlag.SKIP_STORE))
            bits |= 1;

        if (flagSet.contains(GridClientCacheFlag.SKIP_SWAP))
            bits |= 1 << 1;

        if (flagSet.contains(GridClientCacheFlag.SYNC_COMMIT))
            bits |= 1 << 2;

        if (flagSet.contains(GridClientCacheFlag.SYNC_ROLLBACK))
            bits |= 1 << 3;

        if (flagSet.contains(GridClientCacheFlag.INVALIDATE))
            bits |= 1 << 4;

        return bits;
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param flags Cache flags to be enabled.
     * @return If value was actually put.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public <K, V> GridClientFuture<Boolean> cachePut(String cacheName, K key, V val, Set<GridClientCacheFlag> flags)
        throws GridClientConnectionResetException, GridClientClosedException {
        return cachePutAll(cacheName, Collections.singletonMap(key, val), flags);
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param flags Cache flags to be enabled.
     * @return Value.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public <K, V> GridClientFuture<V> cacheGet(String cacheName, final K key, Set<GridClientCacheFlag> flags)
        throws GridClientConnectionResetException, GridClientClosedException {
        final GridClientFuture<Map<K, V>> res = cacheGetAll(cacheName, Collections.singleton(key), flags);

        return new GridClientFutureAdapter<V>() {
            @Override public V get() throws GridClientException {
                return res.get().get(key);
            }

            @Override public boolean isDone() {
                return res.isDone();
            }
        };
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param flags Cache flags to be enabled.
     * @return Whether entry was actually removed.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K> GridClientFuture<Boolean> cacheRemove(String cacheName, K key,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param cacheName Cache name.
     * @param entries Entries.
     * @param flags Cache flags to be enabled.
     * @return {@code True} if map contained more then one entry or if put succeeded in case of one entry,
     *      {@code false} otherwise
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFuture<Boolean> cachePutAll(String cacheName, Map<K, V> entries,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param flags Cache flags to be enabled.
     * @return Entries.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFuture<Map<K, V>> cacheGetAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param flags Cache flags to be enabled.
     * @return Whether entries were actually removed
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K> GridClientFuture<Boolean> cacheRemoveAll(String cacheName, Collection<K> keys,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param flags Cache flags to be enabled.
     * @return Whether entry was added.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFuture<Boolean> cacheAdd(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param flags Cache flags to be enabled.
     * @return Whether value was actually replaced.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFuture<Boolean> cacheReplace(String cacheName, K key, V val,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;
    /**
     * <table>
     *     <tr><th>New value</th><th>Actual/old value</th><th>Behaviour</th></tr>
     *     <tr><td>null     </td><td>null   </td><td>Remove entry for key.</td></tr>
     *     <tr><td>newVal   </td><td>null   </td><td>Put newVal into cache if such key doesn't exist.</td></tr>
     *     <tr><td>null     </td><td>oldVal </td><td>Remove if actual value oldVal is equals to value in cache.</td></tr>
     *     <tr><td>newVal   </td><td>oldVal </td><td>Replace if actual value oldVal is equals to value in cache.</td></tr>
     * </table>
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param newVal Value 1.
     * @param oldVal Value 2.
     * @param flags Cache flags to be enabled.
     * @return Whether new value was actually set.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K, V> GridClientFuture<Boolean> cacheCompareAndSet(String cacheName, K key, V newVal, V oldVal,
        Set<GridClientCacheFlag> flags) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Metrics.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <K> GridClientFuture<GridClientDataMetrics> cacheMetrics(String cacheName, K key)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param taskName Task name.
     * @param arg Task argument.
     * @return Task execution result.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract <R> GridClientFuture<R> execute(String taskName, Object arg)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param id Node ID.
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Node.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<GridClientNode> node(UUID id, boolean includeAttrs, boolean includeMetrics)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param ipAddr IP address.
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Node.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<GridClientNode> node(String ipAddr, boolean includeAttrs,
        boolean includeMetrics) throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Nodes.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<List<GridClientNode>> topology(boolean includeAttrs, boolean includeMetrics)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * @param path Log file path.
     * @param fromLine Index of start line that should be retrieved.
     * @param toLine Index of end line that should be retrieved.
     * @return Log file contents.
     * @throws GridClientConnectionResetException In case of error.
     * @throws GridClientClosedException If client was manually closed before request was sent over network.
     */
    public abstract GridClientFuture<List<String>> log(String path, int fromLine, int toLine)
        throws GridClientConnectionResetException, GridClientClosedException;

    /**
     * Gets SSLContext of this client connection.
     *
     * @return {@link SSLContext} instance.
     */
    protected SSLContext sslContext() {
        return sslCtx;
    }

    /**
     * Returns credentials for this client connection.
     *
     * @return Credentials.
     */
    protected Object credentials() {
        return cred;
    }

    /**
     * Safely gets long value by given key.
     *
     * @param map Map to get value from.
     * @param key Metrics name.
     * @return Value or -1, if not found.
     */
    protected long safeLong(Map<String, Number> map, String key) {
        Number val = map.get(key);

        if (val == null)
            return -1;

        return val.longValue();
    }

    /**
     * Safely gets double value by given key.
     *
     * @param map Map to get value from.
     * @param key Metrics name.
     * @return Value or -1, if not found.
     */
    protected double safeDouble(Map<String, Number> map, String key) {
        Number val = map.get(key);

        if (val == null)
            return -1;

        return val.doubleValue();
    }

    /**
     * Converts metrics map to metrics object.
     *
     * @param metricsMap Map to convert.
     * @return Metrics object.
     */
    protected GridClientDataMetrics metricsMapToMetrics(Map<String, Number> metricsMap) {
        GridClientDataMetricsAdapter metrics = new GridClientDataMetricsAdapter();

        metrics.createTime(safeLong(metricsMap, "createTime"));
        metrics.readTime(safeLong(metricsMap, "readTime"));
        metrics.writeTime(safeLong(metricsMap, "writeTime"));
        metrics.reads((int)safeLong(metricsMap, "reads"));
        metrics.writes((int)safeLong(metricsMap, "writes"));
        metrics.hits((int)safeLong(metricsMap, "hits"));
        metrics.misses((int)safeLong(metricsMap, "misses"));

        return metrics;
    }
}
