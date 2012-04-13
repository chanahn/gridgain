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
import org.gridgain.client.marshaller.protobuf.*;
import org.gridgain.client.ssl.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Client implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.2c.12042012
 */
public class GridClientImpl implements GridClient {
    /** Null mask object */
    private static final Object NULL_MASK = new Object();

    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientImpl.class.getName());

    /** Client id. */
    private final UUID id;

    /** Client configuration. */
    private final GridClientConfigurationAdapter cfg;

    /** SSL context if ssl enabled. */
    private SSLContext sslCtx;

    /** Main compute projection. */
    private final GridClientComputeImpl compute;

    /** Data projections. */
    private ConcurrentMap<Object, GridClientDataImpl> dataMap = new ConcurrentHashMap<Object, GridClientDataImpl>();

    /** Active connections. */
    private ConcurrentMap<GridClientNode, GridClientConnection> conns = new ConcurrentHashMap<GridClientNode,
        GridClientConnection>();

    /** Topology. */
    private GridClientTopology top;

    /** Executor service in case of HTTP connection. */
    private ExecutorService executor;

    /** Connection idle checker thread. */
    private Thread idleCheckThread;

    /** Topology updater thread. */
    private Thread topUpdateThread;

    /** Connect lock. */
    private ReadWriteLock guard = new ReentrantReadWriteLock();

    /** Closed flag. */
    private volatile boolean closed;

    /**
     * Creates a new client based on a given configuration.
     *
     * @param id Client identifier.
     * @param cfg0 Client configuration.
     * @throws GridClientException If client configuration is incorrect.
     * @throws GridServerUnreachableException If none of the servers specified in configuration can
     *      be reached.
     */
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    public GridClientImpl(UUID id, GridClientConfiguration cfg0) throws GridClientException {
        this.id = id;

        cfg = new GridClientConfigurationAdapter(cfg0);

        Collection<InetSocketAddress> srvs = new ArrayList<InetSocketAddress>(cfg.getServers().size());

        for (String srvStr : cfg.getServers()) {
            try {
                String[] split = srvStr.split(":");

                InetSocketAddress addr = new InetSocketAddress(split[0], Integer.parseInt(split[1]));

                srvs.add(addr);
            }
            catch (RuntimeException ignored) {
                throw new GridClientException("Failed to create client (invalid server address specified): " + srvStr);
            }
        }

        top = new GridClientTopology(cfg.isEnableTopologyCache());

        for (GridClientDataConfiguration dataCfg : cfg.getDataConfigurations()) {
            GridClientDataAffinity aff = dataCfg.getAffinity();

            if (aff instanceof GridClientTopologyListener)
                addTopologyListener((GridClientTopologyListener)aff);
        }

        if (cfg.getBalancer() instanceof GridClientTopologyListener)
            top.addTopologyListener((GridClientTopologyListener)cfg.getBalancer());

        GridClientRoundRobinBalancer balancer = new GridClientRoundRobinBalancer();

        top.addTopologyListener(balancer);

        if (cfg.isSslEnabled()) {
            GridSslContextFactory factory = cfg.getSslContextFactory();

            if (factory == null)
                throw new GridClientException("Failed to create client: SSL is enabled but no SSL context factory " +
                    "provided.");

            try {
                sslCtx = factory.createSslContext();
            }
            catch (GridSslException e) {
                throw new GridClientException("Failed to create client (unable to create SSL context, " +
                    "check ssl context factory configuration): " + e.getMessage(), e);
            }
        }

        GridClientConnection conn = connect(srvs);

        // Request topology at start to determine which node we connected to.
        conn.topology(false, false).get();

        Map<String, GridClientCacheMode> overallCaches = new HashMap<String, GridClientCacheMode>();

        // Topology is now updated, so we can identify current connection.
        for (GridClientNodeImpl node : top.nodes()) {
            overallCaches.putAll(node.caches());

            if (node.availableAddresses(cfg.getProtocol()).contains(conn.serverAddress()))
                conns.put(node, conn);
        }

        if (conns.isEmpty()) {
            log.warning("Unable to identify remote node (connection would be closed and topology information will be " +
                "used for creating new connections): " + conn.serverAddress());

            conn.close(false);
        }

        for (Map.Entry<String, GridClientCacheMode> entry : overallCaches.entrySet()) {
            GridClientDataAffinity affinity = affinity(entry.getKey());

            if (affinity instanceof GridClientPartitionedAffinity && entry.getValue() !=
                GridClientCacheMode.PARTITIONED)
                log.warning(GridClientPartitionedAffinity.class.getSimpleName() + " is used for a cache configured " +
                    "for non-partitioned mode [cacheName=" + entry.getKey() + ", cacheMode=" + entry.getValue() + ']');
        }

        idleCheckThread = new IdleCheckerThread();

        idleCheckThread.start();

        topUpdateThread = new TopologyUpdaterThread(balancer);

        topUpdateThread.start();

        compute = new GridClientComputeImpl(this, null, null, cfg.getBalancer());
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /**
     * Closes client.
     * @param waitCompletion If {@code true} will wait for all pending requests to be proceeded.
     */
    public void stop(boolean waitCompletion) {
        guard.writeLock().lock();

        try {
            if (!closed) {
                closed = true;

                // Shutdown the topology refresh thread and connection idle thread.
                idleCheckThread.interrupt();

                topUpdateThread.interrupt();

                Iterator<Map.Entry<GridClientNode, GridClientConnection>> iter = conns.entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry<GridClientNode, GridClientConnection> e = iter.next();

                    e.getValue().close(waitCompletion);

                    iter.remove();
                }

                if (executor != null)
                    executor.shutdownNow();

                // Shutdown listener notification.
                top.shutdown();

                for (GridClientDataConfiguration dataCfg : cfg.getDataConfigurations()) {
                    GridClientDataAffinity aff = dataCfg.getAffinity();

                    if (aff instanceof GridClientTopologyListener)
                        removeTopologyListener((GridClientTopologyListener)aff);
                }
            }
        }
        finally {
            guard.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridClientData data() throws GridClientException {
        return data(null);
    }

    /** {@inheritDoc} */
    @Override public GridClientData data(final String cacheName) throws GridClientException {
        guard.readLock().lock();

        try {
            checkClosed();

            Object key = maskNull(cacheName);

            GridClientDataImpl data = dataMap.get(key);

            if (data == null) {
                GridClientDataConfiguration dataCfg = cfg.getDataConfiguration(cacheName);

                if (dataCfg == null && cacheName != null)
                    throw new GridClientException("Data configuration for given cache name was not provided: " +
                        cacheName);

                GridClientLoadBalancer balancer = dataCfg != null ? dataCfg.getPinnedBalancer() :
                    new GridClientRandomBalancer();

                GridClientPredicate<GridClientNode> cacheNodes = new GridClientPredicate<GridClientNode>() {
                    @Override public boolean apply(GridClientNode e) {
                        return e.caches().containsKey(cacheName);
                    }

                    @Override public String toString() {
                        return "GridClientHasCacheFilter [cacheName=" + cacheName + "]";
                    }
                };

                data = new GridClientDataImpl(cacheName, this, null, cacheNodes, balancer);

                GridClientDataImpl old = dataMap.putIfAbsent(key, data);

                if (old != null)
                    data = old;
            }

            return data;
        } finally {
            guard.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute compute() {
        return compute;
    }

    /** {@inheritDoc} */
    @Override public void addTopologyListener(GridClientTopologyListener lsnr) {
        top.addTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void removeTopologyListener(GridClientTopologyListener lsnr) {
        top.removeTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientTopologyListener> topologyListeners() {
        return top.topologyListeners();
    }

    /**
     * Gets active communication facade.
     *
     * @param node Remote node to which connection should be established.
     * @return Communication facade.
     * @throws GridServerUnreachableException If none of the servers can be reached after the exception.
     * @throws GridClientClosedException If client was closed manually.
     */
    GridClientConnection connection(GridClientNode node) throws GridServerUnreachableException,
        GridClientClosedException {
        guard.readLock().lock();

        try {
            checkClosed();

            GridClientConnection conn = conns.get(node);

            if (conn == null) {
                conn = connect(node.availableAddresses(cfg.getProtocol()));

                GridClientConnection old = conns.putIfAbsent(node, conn);

                if (old != null) {
                    conn.close(false);

                    conn = old;
                }
            }

            return conn;
        }
        finally {
            guard.readLock().unlock();
        }
    }

    /**
     * Handles communication connection failure. Tries to reconnect to any of the servers specified and
     * throws an exception if none of the servers can be reached.
     *
     * @param node Remote node for which this connection belongs to.
     * @param conn Facade that caused the exception.
     * @param e Exception.
     */
    void onFacadeFailed(GridClientNode node, GridClientConnection conn, GridClientConnectionResetException e) {
        if (log.isLoggable(Level.FINE))
            log.fine("Connection with remote node was terminated [node=" + node + ", srvAddr=" +
                conn.serverAddress() + ", errMsg=" + e.getMessage() + ']');

        guard.writeLock().lock();

        try {
            conn.close(false);

            conns.remove(node, conn);
        }
        finally {
            guard.writeLock().unlock();
        }
    }

    /**
     * @return Topology instance.
     */
    GridClientTopology topology() {
        return top;
    }

    /**
     * Gets data affinity for a given cache name.
     *
     * @param cacheName Name of cache for which affinity is obtained. Data configuration with this name
     *      must be configured at client startup.
     * @return Data affinity object.
     * @throws IllegalArgumentException If client data with given name was not configured.
     */
    GridClientDataAffinity affinity(String cacheName) {
        GridClientDataConfiguration dataCfg = cfg.getDataConfiguration(cacheName);

        return dataCfg == null ? null : dataCfg.getAffinity();
    }

    /**
     * Checks and throws an exception if this client was closed.
     *
     * @throws GridClientClosedException If client was closed.
     */
    private void checkClosed() throws GridClientClosedException {
        if (closed)
            throw new GridClientClosedException("Client was closed (no public methods of client can be used anymore).");
    }

    /**
     * Masks null cache name with unique object.
     *
     * @param cacheName Name to be masked.
     * @return Original name or some unique object if name is null.
     */
    private Object maskNull(String cacheName) {
        return cacheName == null ? NULL_MASK : cacheName;
    }

    /**
     * Creates a connected facade and returns it. Called either from constructor or inside
     * a write lock.
     *
     * @param srvs List of server addresses that this method will try to connect to.
     * @return Connected client facade.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    private GridClientConnection connect(Collection<InetSocketAddress> srvs) throws GridServerUnreachableException {
        if (srvs.isEmpty())
            throw new GridServerUnreachableException("Failed to establish connection to the grid node (address " +
                "list is empty).");

        IOException cause = null;

        for (InetSocketAddress srvr : srvs) {
            try {
                switch (cfg.getProtocol()) {
                    case TCP: {
                        return new GridTcpClientConnection(id, srvr, sslCtx, cfg.getConnectTimeout(),
                            cfg.isTcpNoDelay(), new GridClientProtobufMarshaller(), top, cfg.getCredentials());
                    }

                    case HTTP: {
                        executor = new ThreadPoolExecutor(10, 50, 30, SECONDS, new LinkedBlockingDeque<Runnable>());

                        return new GridHttpClientConnection(id, srvr, sslCtx, cfg.getConnectTimeout(), top, executor,
                            cfg.getCredentials());
                    }

                    default: {
                        throw new GridServerUnreachableException("Failed to create client (protocol is not " +
                            "supported): " + cfg.getProtocol());
                    }
                }
            }
            catch (IOException e) {
                if (log.isLoggable(Level.FINE))
                    log.fine("Unable to connect to grid node [srvAddr=" + srvr + ", msg=" + e.getMessage() + ']');

                cause = e;
            }
        }

        assert cause != null;

        throw new GridServerUnreachableException("Failed to connect to any of the servers in list: " + srvs, cause);
    }

    /**
     * Thread that checks opened client connections for idle and closes connections that idle for a long time.
     */
    @SuppressWarnings("BusyWait")
    private class IdleCheckerThread extends Thread {
        /**
         * Creates idle check thread instance.
         */
        private IdleCheckerThread() {
            super(id + "-idle-check");
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (!isInterrupted()) {
                    Thread.sleep(1000);

                    long now = System.currentTimeMillis();

                    for (Map.Entry<GridClientNode, GridClientConnection> e : conns.entrySet()) {
                        GridClientConnection conn = e.getValue();

                        long idleTime = now - conn.lastNetworkActivityTimestamp();

                        if (idleTime > cfg.getMaxConnectionIdleTime()) {
                            if (conn.closeIfIdle()) {
                                conns.remove(e.getKey(), conn);

                                if (log.isLoggable(Level.FINE))
                                    log.fine("Closed client connection since idle time exceeded maximum idle time " +
                                        "[node=" + e.getKey() + ", srvAddr" + conn.serverAddress() + ", idleTime=" +
                                        idleTime + ']');
                            }
                        }
                    }
                }
            }
            catch (InterruptedException ignored) {
                // Client is shutting down.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Thread that updates topology according to refresh interval specified in configuration.
     */
    @SuppressWarnings("BusyWait")
    private class TopologyUpdaterThread extends Thread {
        /** Balancer to use for topology updates. */
        private GridClientLoadBalancer topBalancer;

        /**
         * Creates topology refresh thread.
         *
         * @param topBalancer Balancer to use.
         */
        private TopologyUpdaterThread(GridClientLoadBalancer topBalancer) {
            super(id + "-topology-update");

            this.topBalancer = topBalancer;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridClientCompute topPrj = new GridClientComputeImpl(GridClientImpl.this, null, null, topBalancer);

                while (!isInterrupted() && !closed) {
                    Thread.sleep(cfg.getTopologyRefreshFrequency());

                    try {
                        topPrj.refreshTopology(false, false);
                    }
                    catch (GridClientException e) {
                        if (log.isLoggable(Level.FINE))
                            log.fine("Failed to update topology: " + e.getMessage());
                    }
                }
            }
            catch (InterruptedException ignored) {
                // Client is shutting down.
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientImpl [id=" + id + ", closed=" + closed + ']';
    }
}
