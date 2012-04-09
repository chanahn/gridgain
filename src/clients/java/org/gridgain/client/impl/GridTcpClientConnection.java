// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.client.message.*;
import org.gridgain.client.util.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

import static org.gridgain.client.message.GridClientCacheRequest.GridCacheOperation.*;

/**
 * This class performs request to grid over tcp protocol. Serialization is performed with marshaller
 * provided.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
class GridTcpClientConnection extends GridClientConnection {
    /** Logger */
    private static final Logger log = Logger.getLogger(GridTcpClientConnection.class.getName());

    /** Ping packet. */
    private static final byte[] PING_PACKET = new byte[] {(byte)0x90, 0x00, 0x00, 0x00, 0x00};

    /** Ping is sent every 5 seconds. */
    private static final int PING_SND_TIME = 5000;

    /** Connection is considered to be half-opened if server did not respond to ping in 7 seconds. */
    private static final int PING_RES_TIMEOUT = 7000;

    /** Socket read timeout. */
    private static final int SOCK_READ_TIMEOUT = 1000;

    /** Requests that are waiting for response. */
    private ConcurrentMap<Long, TcpClientFuture> pendingReqs =
        new ConcurrentHashMap<Long, TcpClientFuture>();

    /** Node by node id requests. Map for reducing server load. */
    private ConcurrentMap<UUID, TcpClientFuture> refreshNodeReqs = new ConcurrentHashMap<UUID, TcpClientFuture>();

    /** Lock for graceful shutdown. */
    private ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Condition to await for  */
    private Condition emptyCond = busyLock.writeLock().newCondition();

    /** Closed flag. */
    private volatile boolean closed;

    /** Closed by idle request flag. */
    private volatile boolean closedIdle;

    /** Request ID counter. */
    private AtomicLong reqIdCntr = new AtomicLong(1);

    /** Message marshaller */
    private GridClientMarshaller marshaller;

    /** Underlying socket connection. */
    private final Socket sock;

    /** Output stream. */
    private final OutputStream out;

    /** Input stream. */
    private final InputStream input;

    /** Timestamp of last packet send event. */
    private volatile long lastPacketSndTime;

    /** Timestamp of last packet receive event. */
    private volatile long lastPacketRcvTime;

    /** Session token. */
    private volatile byte[] sesTok;

    /** Reader thread. */
    private Thread rdr;

    /**
     * Creates a client facade, tries to connect to remote server, in case of success starts reader thread.
     *
     * @param clientId Client identifier.
     * @param srvAddr Server to connect to.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param connectTimeout Connect timeout.
     * @param tcpNoDelay TCP_NODELAY flag for outgoing socket connection.
     * @param marshaller Marshaller to use in communication.
     * @param top Topology instance.
     * @param cred Client credentials.
     * @throws IOException If connection could not be established.
     */
    @SuppressWarnings("CallToThreadStartDuringObjectConstruction")
    GridTcpClientConnection(UUID clientId, InetSocketAddress srvAddr, SSLContext sslCtx, int connectTimeout,
        boolean tcpNoDelay, GridClientMarshaller marshaller, GridClientTopology top, Object cred) throws IOException {
        super(clientId, srvAddr, sslCtx, top, cred);

        this.marshaller = marshaller;

        sock = sslCtx == null ? new Socket() : sslCtx.getSocketFactory().createSocket();

        sock.connect(srvAddr, connectTimeout);

        // These methods may throw an exception, we must handle resources gracefully.
        boolean success = false;

        try {
            sock.setSoTimeout(SOCK_READ_TIMEOUT);
            sock.setTcpNoDelay(tcpNoDelay);

            if (sslCtx != null)
                ((SSLSocket)sock).startHandshake();

            input = sock.getInputStream();
            out = sock.getOutputStream();

            rdr = new ReaderThread("client-conn-reader-" + serverAddress());

            // Avoid immediate attempt to close by idle.
            lastPacketSndTime = lastPacketRcvTime = System.currentTimeMillis();

            rdr.start();

            success = true;
        } finally {
            if (!success)
                close(false);
        }
    }

    /**
     * Closes this client. No methods of this class can be used after this method was called.
     * Any attempt to perform request on closed client will case {@link GridClientConnectionResetException}. All
     * pending requests are failed without waiting for response.
     *
     * @param waitCompletion If {@code true} this method will wait for all pending requests to be completed.
     */
    @Override void close(boolean waitCompletion) {
        busyLock.writeLock().lock();

        try {
            // If will wait, reject all incoming requests.
            closed = true;

            try {
                // Wait for all pending requests to be processed.
                if (waitCompletion && !pendingReqs.isEmpty())
                    emptyCond.await();
            }
            catch (InterruptedException ignored) {
                log.warning("Interrupted while waiting for all requests to be processed (all pending " +
                    "requests will be failed): " + serverAddress());

                Thread.currentThread().interrupt();
            }

            if (rdr != null)
                rdr.interrupt();

            shutdown();
        }
        finally {
            busyLock.writeLock().unlock();
        }

        // Join outside the write lock since reader tries to shutdown too.
        try {
            if (rdr != null)
                rdr.join();
        }
        catch (InterruptedException ignored) {
            if (log.isLoggable(Level.FINE))
                log.fine("Interrupted while waiting for reader thread shutdown: " + serverAddress());

            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes client only if there are no pending requests in map.
     *
     * @return {@code True} if client was closed.
     */
    @Override boolean closeIfIdle() {
        busyLock.writeLock().lock();

        try {
            // No futures can be added here.
            if (pendingReqs.isEmpty()) {
                closedIdle = true;

                shutdown();

                return true;
            }

            return false;
        }
        finally {
            busyLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override long lastNetworkActivityTimestamp() {
        return Math.max(lastPacketSndTime, lastPacketRcvTime);
    }

    /**
     * Closes all resources and fails all pending requests. Must be called within write lock.
     */
    private void shutdown() {
        assert ((ReentrantReadWriteLock)busyLock).writeLock().isHeldByCurrentThread();

        closed = true;

        // SSL socket does not support IO shutdown.
        if (!(sock instanceof SSLSocket)) {
            try {
                sock.shutdownInput();
                sock.shutdownOutput();
            }
            catch (IOException ignored) {
            }
        }

        GridClientUtils.closeQuiet(input);
        GridClientUtils.closeQuiet(out);
        GridClientUtils.closeQuiet(sock);

        for (Iterator<TcpClientFuture> iter = pendingReqs.values().iterator(); iter.hasNext(); ) {
            GridClientFutureAdapter fut = iter.next();

            fut.onDone(new GridClientException("Failed to perform request (connection was closed before response" +
                " is received): " + serverAddress()));

            iter.remove();
        }

        emptyCond.signalAll();
    }

    /**
     * Makes request to server via tcp protocol and returns a future that will be completed when
     * response is received.
     *
     * @param msg Message to request,
     * @return Response object.
     * @throws GridClientConnectionResetException If request failed.
     * @throws GridClientClosedException If client was closed.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg) throws GridClientConnectionResetException,
        GridClientClosedException {
        assert msg != null;

        TcpClientFuture<R> res = new TcpClientFuture<R>();

        return makeRequest(msg, res);
    }

    /**
     * Makes request to server via tcp protocol and returns a future that will be completed when response is received.
     *
     * @param msg Message to request,
     * @param fut Future that will handle response.
     * @return Response object.
     * @throws GridClientConnectionResetException If request failed.
     * @throws GridClientClosedException If client was closed.
     */
    private <R> GridClientFutureAdapter<R> makeRequest(GridClientMessage msg, TcpClientFuture<R> fut)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert msg != null;

        try {
            busyLock.readLock().lock();

            try {
                if (closed) {
                    if (closedIdle)
                        throw new GridConnectionIdleClosedException("Connection was closed by idle thread (will " +
                            "reconnect): " + serverAddress());

                    throw new GridClientClosedException("Failed to perform request (connection was closed before " +
                        "message is sent): " + serverAddress());
                }

                long reqId = reqIdCntr.getAndIncrement();

                msg.requestId(reqId);

                msg.clientId(clientId);

                msg.sessionToken(sesTok);

                fut.pendingMessage(msg);

                GridClientFutureAdapter old = pendingReqs.putIfAbsent(reqId, fut);

                assert old == null;

                sendPacket(msg);

                return fut;
            }
            finally {
                busyLock.readLock().unlock();
            }
        }
        catch (IOException e) {
            busyLock.writeLock().lock();

            try {
                // In case of IOException we should shutdown the whole client since connection is broken.
                shutdown();
            }
            finally {
                busyLock.writeLock().unlock();
            }

            throw new GridClientConnectionResetException("Failed to send message over network (will try to " +
                "reconnect): " + serverAddress(), e);
        }
    }

    /**
     * Handles incoming response message. If this connection is closed this method would signal empty event
     * if there is no more pending requests.
     *
     * @param msg Incoming response message.
     * @throws IOException If IO error occurred while sending retry messages.
     */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    private void handleResponse(GridClientResultBean msg) throws IOException {
        TcpClientFuture fut;

        if (msg.sessionToken() != null)
            sesTok = msg.sessionToken();

        busyLock.writeLock().lock();

        try {
            fut = pendingReqs.get(msg.requestId());

            if (fut == null)
                return;

            GridClientMessage src = fut.pendingMessage();

            switch (fut.retryState()) {
                case TcpClientFuture.STATE_INITIAL: {
                    if (msg.successStatus() == GridClientResultBean.STATUS_AUTH_FAILURE) {
                        fut.retryState(TcpClientFuture.STATE_AUTH_RETRY);

                        GridClientAuthenticationRequest req = buildAuthRequest();

                        req.requestId(msg.requestId());

                        sendPacket(req);

                        return;
                    }

                    break;
                }

                case TcpClientFuture.STATE_AUTH_RETRY: {
                    if (msg.successStatus() == GridClientResultBean.STATUS_SUCCESS) {
                        fut.retryState(TcpClientFuture.STATE_REQUEST_RETRY);

                        src.sessionToken(sesTok);

                        sendPacket(src);

                        return;
                    }

                    break;
                }
            }

            pendingReqs.remove(msg.requestId());

            if (pendingReqs.isEmpty() && closed)
                emptyCond.signalAll();
        }
        finally {
            busyLock.writeLock().unlock();
        }

        if (fut != null) {
            if (msg.successStatus() == GridClientResultBean.STATUS_AUTH_FAILURE)
                fut.onDone(new GridClientAuthenticationException("Client authentication failed [clientId=" + clientId +
                    ", srvAddr=" + serverAddress() + ", errMsg=" + msg.errorMessage() +']'));
            else if (msg.errorMessage() != null)
                fut.onDone(new GridClientException(msg.errorMessage()));
            else
                fut.onDone(msg.result());
        }
    }

    /**
     * Builds authentication request message with credentials taken from credentials object.
     *
     * @return AuthenticationRequest message.
     */
    private GridClientAuthenticationRequest buildAuthRequest() {
        GridClientAuthenticationRequest req = new GridClientAuthenticationRequest();

        req.clientId(clientId);

        req.credentials(credentials());

        return req;
    }

    /**
     * Tries to send packet over network.
     *
     * @param msg Message being sent.
     * @throws IOException If client was closed before message was sent over network.
     */
    private void sendPacket(GridClientMessage msg) throws IOException {
        byte[] data = marshaller.marshal(msg);

        byte[] res = new byte[5 + data.length];

        res[0] = (byte)0x90;

        GridClientByteUtils.intToBytes(data.length, res, 1);

        System.arraycopy(data, 0, res, 5, data.length);

        synchronized (sock) {
            out.write(res);
        }

        lastPacketSndTime = System.currentTimeMillis();
    }

    /**
     *
     * @param cacheName Cache name.
     * @param entries Entries.
     * @return {@code True} if map contained more then one entry or if put succeeded in case of one entry,
     *      {@code false} otherwise
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K, V> GridClientFuture<Boolean> cachePutAll(String cacheName, Map<K, V> entries)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert entries != null;

        GridClientCacheRequest<K, V> req = new GridClientCacheRequest<K, V>(PUT_ALL);

        req.cacheName(cacheName);
        req.values(entries);

        return makeRequest(req);
    }

    /**
     *
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Entries.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K, V> GridClientFuture<Map<K, V>> cacheGetAll(String cacheName, Collection<K> keys)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        GridClientCacheRequest<K, V> req = new GridClientCacheRequest<K, V>(GET_ALL);

        req.cacheName(cacheName);
        req.keys(new HashSet<K>(keys));

        return makeRequest(req);
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Whether entry was actually removed.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K> GridClientFuture<Boolean> cacheRemove(String cacheName, K key)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientCacheRequest<K, Object> req = new GridClientCacheRequest<K, Object>(RMV);

        req.cacheName(cacheName);
        req.key(key);

        return makeRequest(req);
    }

    /**
     *
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Whether entries were actually removed
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K> GridClientFuture<Boolean> cacheRemoveAll(String cacheName, Collection<K> keys)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        GridClientCacheRequest<K, Object> req = new GridClientCacheRequest<K, Object>(RMV_ALL);

        req.cacheName(cacheName);
        req.keys(new HashSet<K>(keys));

        return makeRequest(req);
    }

    /**
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether entry was added.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K, V> GridClientFuture<Boolean> cacheAdd(String cacheName, K key, V val)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest<K, V> add = new GridClientCacheRequest<K, V>(ADD);

        add.cacheName(cacheName);
        add.key(key);
        add.value(val);

        return makeRequest(add);
    }

    /**
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually replaced.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K, V> GridClientFuture<Boolean> cacheReplace(String cacheName, K key, V val)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;
        assert val != null;

        GridClientCacheRequest<K, V> replace = new GridClientCacheRequest<K, V>(REPLACE);

        replace.cacheName(cacheName);
        replace.key(key);
        replace.value(val);

        return makeRequest(replace);
    }

    /**
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @param val1 Value 1.
     * @param val2 Value 2.
     * @return Whether new value was actually set.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <K, V> GridClientFuture<Boolean> cacheCompareAndSet(String cacheName, K key, V val1, V val2)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;

        GridClientCacheRequest<K, V> msg = new GridClientCacheRequest<K, V>(CAS);

        msg.cacheName(cacheName);
        msg.key(key);
        msg.value(val1);
        msg.value2(val2);

        return makeRequest(msg);
    }

    /**
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @return Metrics.
     * @throws GridClientConnectionResetException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Override public <K> GridClientFuture<GridClientDataMetrics> cacheMetrics(String cacheName, K key)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientCacheRequest<K, Object> metrics = new GridClientCacheRequest<K, Object>(METRICS);

        metrics.cacheName(cacheName);
        metrics.key(key);

        TcpClientFuture fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                super.onDone(metricsMapToMetrics((Map<String, Number>)res));
            }
        };

        return makeRequest(metrics, fut);
    }

    /**
     *
     * @param taskName Task name.
     * @param arg Task argument.
     * @return Task execution result.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public <R> GridClientFuture<R> execute(String taskName, Object arg)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientTaskRequest msg = new GridClientTaskRequest();

        msg.taskName(taskName);
        msg.argument(arg);

        final GridClientFutureAdapter<GridClientTaskResultBean> fut = makeRequest(msg);

        return new GridClientFutureAdapter<R>() {
            @Override public R get() throws GridClientException {
                return fut.get().getResult();
            }

            @Override public boolean isDone() {
                return fut.isDone();
            }
        };
    }

    /**
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Node.
     * @throws GridClientConnectionResetException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Override public GridClientFuture<GridClientNode> node(final UUID id, boolean includeAttrs, boolean includeMetrics)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert id != null;

        TcpClientFuture fut = refreshNodeReqs.get(id);

        // Return request that is in progress.
        if (fut != null)
            return fut;

        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                //Clean up the node id requests map.
                refreshNodeReqs.remove(id);

                GridClientNodeImpl node = nodeBeanToNode((GridClientNodeBean)res);

                if (node != null)
                    top.updateNode(node);

                super.onDone(node);
            }
        };

        GridClientFutureAdapter old = refreshNodeReqs.putIfAbsent(id, fut);

        // If concurrent thread put request, do not send the message.
        if (old != null)
            return old;

        msg.nodeId(id.toString());
        msg.includeAttributes(includeAttrs);
        msg.includeMetrics(includeMetrics);

        return makeRequest(msg, fut);
    }

    /**
     *
     * @param ipAddr IP address.
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Node.
     * @throws GridClientConnectionResetException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Override public GridClientFuture<GridClientNode> node(String ipAddr, boolean includeAttrs, boolean includeMetrics)
        throws GridClientConnectionResetException, GridClientClosedException {

        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        TcpClientFuture fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                GridClientNodeImpl node = nodeBeanToNode((GridClientNodeBean)res);

                if (node != null)
                    top.updateNode(node);

                super.onDone(node);
            }
        };

        msg.nodeIp(ipAddr);
        msg.includeAttributes(includeAttrs);
        msg.includeMetrics(includeMetrics);

        return makeRequest(msg, fut);
    }

    /**
     *
     * @param includeAttrs Whether to include attributes.
     * @param includeMetrics Whether to include metrics.
     * @return Nodes.
     * @throws GridClientConnectionResetException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Override public GridClientFuture<List<GridClientNode>> topology(boolean includeAttrs, boolean includeMetrics)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientTopologyRequest msg = new GridClientTopologyRequest();

        TcpClientFuture fut = new TcpClientFuture() {
            @Override public void onDone(Object res) {
                Collection<GridClientNodeBean> beans = (Collection<GridClientNodeBean>)res;

                List<GridClientNodeImpl> nodes = new ArrayList<GridClientNodeImpl>(beans.size());

                for (GridClientNodeBean bean : beans)
                    nodes.add(nodeBeanToNode(bean));

                top.updateTopology(nodes);

                super.onDone(nodes);
            }
        };


        msg.includeAttributes(includeAttrs);
        msg.includeMetrics(includeMetrics);

        return makeRequest(msg, fut);
    }

    /**
     *
     * @param path Log file path.
     * @return Log file contents.
     * @throws GridClientConnectionResetException In case of error.
     */
    @Override public GridClientFuture<List<String>> log(String path, int fromLine, int toLine)
        throws GridClientConnectionResetException, GridClientClosedException {
        GridClientLogRequest msg = new GridClientLogRequest();

        msg.from(fromLine);
        msg.to(toLine);

        msg.path(path);

        return makeRequest(msg);
    }

    /**
     * Creates client node instance from message.
     *
     * @param nodeBean Node bean message.
     * @return Created node.
     */
    private GridClientNodeImpl nodeBeanToNode(GridClientNodeBean nodeBean) {
        if (nodeBean == null)
            return null;

        GridClientNodeImpl node = new GridClientNodeImpl();

        node.nodeId(UUID.fromString(nodeBean.getNodeId()));
        node.internalAddresses(nodeBean.getInternalAddresses());
        node.externalAddresses(nodeBean.getExternalAddresses());
        node.tcpPort(nodeBean.getTcpPort());
        node.httpPort(nodeBean.getJettyPort());

        if (nodeBean.getCaches() != null) {
            Map<String, GridClientCacheMode> caches = new HashMap<String, GridClientCacheMode>(nodeBean.getCaches()
                .size());

            for (Map.Entry<String, String> e : nodeBean.getCaches().entrySet()) {
                try {
                    caches.put(e.getKey(), GridClientCacheMode.valueOf(e.getValue()));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received from remote node (will ignore) [srv=" + serverAddress() +
                        ", cacheName=" + e.getKey() + ", cacheMode=" + e.getValue() + ']');
                }
            }

            if (nodeBean.getDefaultCacheMode() != null) {
                try {
                    caches.put(null, GridClientCacheMode.valueOf(nodeBean.getDefaultCacheMode()));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received for default cache from remote node (will ignore) [srv="
                        + serverAddress() + ", cacheMode=" + nodeBean.getDefaultCacheMode() + ']');
                }
            }

            node.caches(caches);
        }

        if (nodeBean.getAttributes() != null)
            node.attributes(nodeBean.getAttributes());

        GridClientNodeMetricsBean metricsBean = nodeBean.getMetrics();

        if (metricsBean != null) {
            GridClientNodeMetricsAdapter metrics = new GridClientNodeMetricsAdapter();

            metrics.setStartTime(metricsBean.getStartTime());
            metrics.setAverageActiveJobs(metricsBean.getAverageActiveJobs());
            metrics.setAverageCancelledJobs(metricsBean.getAverageCancelledJobs());
            metrics.setAverageCpuLoad(metricsBean.getAverageCpuLoad());
            metrics.setAverageJobExecuteTime(metricsBean.getAverageJobExecuteTime());
            metrics.setAverageJobWaitTime(metricsBean.getAverageJobWaitTime());
            metrics.setAverageRejectedJobs(metricsBean.getAverageRejectedJobs());
            metrics.setAverageWaitingJobs(metricsBean.getAverageWaitingJobs());
            metrics.setCurrentActiveJobs(metricsBean.getCurrentActiveJobs());
            metrics.setCurrentCancelledJobs(metricsBean.getCurrentCancelledJobs());
            metrics.setCurrentCpuLoad(metricsBean.getCurrentCpuLoad());
            metrics.setCurrentDaemonThreadCount(metricsBean.getCurrentDaemonThreadCount());
            metrics.setCurrentIdleTime(metricsBean.getCurrentIdleTime());
            metrics.setCurrentJobExecuteTime(metricsBean.getCurrentJobExecuteTime());
            metrics.setCurrentJobWaitTime(metricsBean.getCurrentJobWaitTime());
            metrics.setCurrentRejectedJobs(metricsBean.getCurrentRejectedJobs());
            metrics.setCurrentThreadCount(metricsBean.getCurrentThreadCount());
            metrics.setCurrentWaitingJobs(metricsBean.getCurrentWaitingJobs());
            metrics.setFileSystemFreeSpace(metricsBean.getFileSystemFreeSpace());
            metrics.setFileSystemTotalSpace(metricsBean.getFileSystemTotalSpace());
            metrics.setFileSystemUsableSpace(metricsBean.getFileSystemUsableSpace());
            metrics.setHeapMemoryCommitted(metricsBean.getHeapMemoryCommitted());
            metrics.setHeapMemoryInitialized(metricsBean.getHeapMemoryInitialized());
            metrics.setHeapMemoryMaximum(metricsBean.getHeapMemoryMaximum());
            metrics.setHeapMemoryUsed(metricsBean.getHeapMemoryUsed());
            metrics.setLastDataVersion(metricsBean.getLastDataVersion());
            metrics.setLastUpdateTime(metricsBean.getLastUpdateTime());
            metrics.setMaximumActiveJobs(metricsBean.getMaximumActiveJobs());
            metrics.setMaximumCancelledJobs(metricsBean.getMaximumCancelledJobs());
            metrics.setMaximumJobExecuteTime(metricsBean.getMaximumJobExecuteTime());
            metrics.setMaximumJobWaitTime(metricsBean.getMaximumJobWaitTime());
            metrics.setMaximumRejectedJobs(metricsBean.getMaximumRejectedJobs());
            metrics.setMaximumThreadCount(metricsBean.getMaximumThreadCount());
            metrics.setMaximumWaitingJobs(metricsBean.getMaximumWaitingJobs());
            metrics.setNodeStartTime(metricsBean.getNodeStartTime());
            metrics.setNonHeapMemoryCommitted(metricsBean.getNonHeapMemoryCommitted());
            metrics.setNonHeapMemoryInitialized(metricsBean.getNonHeapMemoryInitialized());
            metrics.setNonHeapMemoryMaximum(metricsBean.getNonHeapMemoryMaximum());
            metrics.setNonHeapMemoryUsed(metricsBean.getNonHeapMemoryUsed());
            metrics.setStartTime(metricsBean.getStartTime());
            metrics.setTotalCancelledJobs(metricsBean.getTotalCancelledJobs());
            metrics.setTotalCpus(metricsBean.getTotalCpus());
            metrics.setTotalExecutedJobs(metricsBean.getTotalExecutedJobs());
            metrics.setTotalIdleTime(metricsBean.getTotalIdleTime());
            metrics.setTotalRejectedJobs(metricsBean.getTotalRejectedJobs());
            metrics.setTotalStartedThreadCount(metricsBean.getTotalStartedThreadCount());
            metrics.setUpTime(metricsBean.getUpTime());

            node.metrics(metrics);
        }

        return node;
    }

    /**
     * Future extension that holds client tcp message and auth retry flag.
     */
    private static class TcpClientFuture<R> extends GridClientFutureAdapter<R> {
        /** Initial request. */
        private static final int STATE_INITIAL = 0;

        /** Authentication retry. */
        private static final int STATE_AUTH_RETRY = 1;

        /** Request retry after auth retry. */
        private static final int STATE_REQUEST_RETRY = 2;

        /** Pending message for this future. */
        private GridClientMessage pendingMsg;

        /** Flag indicating whether authentication retry was attempted for this request. */
        private int authRetry = STATE_INITIAL;

        /**
         * @return Originating request message.
         */
        public GridClientMessage pendingMessage() {
            return pendingMsg;
        }

        /**
         * @param pendingMsg Originating request message.
         */
        public void pendingMessage(GridClientMessage pendingMsg) {
            this.pendingMsg = pendingMsg;
        }

        /**
         * @return Whether or not authentication retry attempted.
         */
        public int retryState() {
            return authRetry;
        }

        /**
         * @param authRetry Whether or not authentication retry attempted.
         */
        public void retryState(int authRetry) {
            this.authRetry = authRetry;
        }
    }

    /**
     * Reader thread.
     */
    private class ReaderThread extends Thread {
        /** Ping receive time */
        private long lastPingRcvTime = System.currentTimeMillis();

        /** Ping send time. */
        private long lastPingSndTime = System.currentTimeMillis();

        /**
         * Creates a reader thread.
         *
         * @param name Thread name.
         */
        private ReaderThread(String name) {
            super(name);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                ByteArrayOutputStream buf = new ByteArrayOutputStream();

                int len = 0;

                boolean running = true;

                while (running) {
                    // Note that only this thread removes futures from map.
                    // So if we see closed condition, it is safe to check map size since no more futures
                    // will be added to the map.
                    if (closed) {
                        // Exit if either all requests processed or we do not wait for completion.
                        if (isInterrupted() || pendingReqs.isEmpty())
                            break;
                    }

                    // Header.
                    int symbol;

                    try {
                        symbol = input.read();
                    }
                    catch (SocketTimeoutException ignored) {
                        checkPing();

                        continue;
                    }

                    if (symbol == -1)
                        break;

                    // Check for correct header.
                    if ((byte)symbol != (byte)0x90) {
                        if (log.isLoggable(Level.FINE))
                            log.fine("Failed to parse incoming message (unexpected header received, will close) " +
                                "[srvAddr=" + serverAddress() + ", symbol=" + Integer.toHexString(symbol & 0xFF));

                        break;
                    }

                    // Packet.
                    while (true) {
                        try {
                            symbol = input.read();
                        }
                        catch (SocketTimeoutException ignored) {
                            checkPing();

                            continue;
                        }

                        if (symbol == -1) {
                            running = false;

                            break;
                        }

                        byte b = (byte)symbol;

                        buf.write(b);

                        if (len == 0) {
                            if (buf.size() == 4) {
                                len = GridClientByteUtils.bytesToInt(buf.toByteArray(), 0);

                                buf.reset();

                                if (len == 0) {
                                    // Ping received.
                                    lastPingRcvTime = System.currentTimeMillis();

                                    break;
                                }
                            }
                        }
                        else {
                            if (buf.size() == len) {
                                GridClientResultBean msg = marshaller.unmarshal(buf.toByteArray());

                                buf.reset();

                                len = 0;

                                lastPacketRcvTime = System.currentTimeMillis();

                                handleResponse(msg);

                                break;
                            }
                        }
                    }
                }
            }
            catch (IOException e) {
                if (!closed)
                    log.log(Level.WARNING, "Failed to read data from remote host (will close connection): " +
                        serverAddress(), e);
            }
            catch (Throwable e) {
                log.log(Level.SEVERE, "Unexpected throwable in connection reader thread (will close connection): " +
                    serverAddress(), e);
            }
            finally {
                busyLock.writeLock().lock();

                try {
                    shutdown();
                }
                finally {
                    busyLock.writeLock().unlock();
                }
            }
        }

        /**
         * Checks last ping send time and last ping receive time.
         *
         * @throws IOException If
         */
        private void checkPing() throws IOException {
            long now = System.currentTimeMillis();

            long lastRcvTime = Math.max(lastPacketRcvTime, lastPingRcvTime);

            if (now - lastPingSndTime > PING_SND_TIME) {
                synchronized (sock) {
                    out.write(PING_PACKET);
                }

                lastPingSndTime = now;
            }

            if (now - lastRcvTime > PING_RES_TIMEOUT)
                throw new IOException("Did not receive any packets within ping response interval (connection is " +
                    "considered to be half-opened) [lastPingSendTime=" + lastPingSndTime + ", lastReceiveTime=" +
                    lastRcvTime + ", addr=" + serverAddress() + ']');
        }
    }
}
