// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import net.sf.json.*;
import org.gridgain.client.*;
import org.gridgain.client.message.*;
import org.gridgain.client.util.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

/**
 * Java client implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridHttpClientConnection extends GridClientConnection {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridHttpClientConnection.class.getName());

    /** Thread pool. */
    private final ExecutorService pool;

    /** Busy lock for graceful close. */
    private ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Pending requests  */
    private GridConcurrentHashSet<FutureWorker> pendingRequests = new GridConcurrentHashSet<FutureWorker>();

    /** Session token. */
    private String sesTok;

    /** Closed flag. */
    private boolean closed;

    /**
     * Creates client.
     *
     * @param clientId Client identifier.
     * @param srvAddr Server address on which HTTP REST handler resides.
     * @param sslCtx SSL context to use if SSL is enabled, {@code null} otherwise.
     * @param connTimeout Connection timeout.
     * @param top Topology to use.
     * @param pool Thread pool executor.
     * @param cred Client credentials.
     * @throws IOException If input-output error occurs.
     */
    public GridHttpClientConnection(UUID clientId, InetSocketAddress srvAddr, SSLContext sslCtx, int connTimeout,
        GridClientTopology top, ExecutorService pool, Object cred) throws IOException {
        super(clientId, srvAddr, sslCtx, top, cred);

        Socket sock = new Socket();

        try {
            sock.connect(srvAddr, connTimeout);
        }
        finally {
            GridClientUtils.closeQuiet(sock);
        }

        this.pool = pool;
    }

    /** {@inheritDoc} */
    @Override void close(boolean waitCompletion) {
        busyLock.writeLock().lock();

        try {
            if (!closed)
                closed = true;
        }
        finally {
            busyLock.writeLock().unlock();
        }

        if (waitCompletion) {
            Iterator<FutureWorker> tasks = pendingRequests.iterator();

            try {
                while (tasks.hasNext()) {
                    FutureWorker worker = tasks.next();

                    worker.awaitCompletion();

                    tasks.remove();
                }
            }
            catch (InterruptedException ignored) {
                log.warning("Interrupted while waiting for all pending requests to complete (will cancel remaining " +
                    "requests): " + serverAddress());

                Thread.currentThread().interrupt();
            }
        }

        if (log.isLoggable(Level.FINE))
            log.fine("Cancelling " + pendingRequests.size() + " pending requests: " + serverAddress());

        Iterator<FutureWorker> tasks = pendingRequests.iterator();

        while (tasks.hasNext()) {
            FutureWorker worker = tasks.next();

            worker.cancel();

            tasks.remove();
        }
    }

    /** {@inheritDoc} */
    @Override boolean closeIfIdle() {
        // Should be never called.
        assert false : "closeIfIdle should be never called on GridHttpClientConnection.";

        return false;
    }

    /**
     * @return Current time so we never get closed by idle. Actually, we will never have idle connections
     * due to nature of http protocol.
     */
    @Override long lastNetworkActivityTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * Creates new future and passes it to the makeJettyRequest.
     *
     * @param params Request parameters.
     * @return Future.
     * @throws GridClientClosedException If client was manually closed.
     * @throws GridClientConnectionResetException If connection could not be established.
     */
    private <R> GridClientFuture<R> makeJettyRequest(Map<String, Object> params)
        throws GridClientClosedException, GridClientConnectionResetException {
        return makeJettyRequest(params, new GridClientFutureAdapter<R>());
    }

    /**
     * Makes request to Jetty server.
     *
     * @param params Parameters map.
     * @param fut Future to use.
     * @return Response.
     * @throws GridClientConnectionResetException In connection to the server can not be established.
     * @throws GridClientClosedException If connection was closed manually.
     */
    @SuppressWarnings("unchecked")
    private <R> GridClientFuture<R> makeJettyRequest(final Map<String, Object> params,
        final GridClientFutureAdapter fut) throws GridClientConnectionResetException, GridClientClosedException {
        assert params != null;
        assert params.containsKey("cmd");

        busyLock.readLock().lock();

        try {
            if (closed)
                throw new GridClientClosedException("Failed to perform request (connection was closed before request" +
                    " is sent): " + serverAddress());

            try {
                final InputStream input = openInputStream(buildRequestString(params));

                FutureWorker worker = new FutureWorker(fut) {
                    @Override protected void body() throws Exception {
                        try {
                            JSONObject json = readReply(input);

                            int successStatus = json.getInt("successStatus");

                            if (successStatus == GridClientResultBean.STATUS_AUTH_FAILURE) {
                                sesTok = null;

                                InputStream inputAuth = openInputStream(buildRequestString(params));

                                json = readReply(inputAuth);
                            }

                            if (json.getString("sessionToken") != null)
                                sesTok = json.getString("sessionToken");

                            successStatus = json.getInt("successStatus");

                            String errorMsg = (String)json.get("error");

                            if (successStatus == GridClientResultBean.STATUS_AUTH_FAILURE) {
                                sesTok = null;

                                fut.onDone(new GridClientAuthenticationException("Client authentication failed " +
                                    "[clientId=" + clientId + ", srvAddr=" + serverAddress() + ", errMsg=" + errorMsg +
                                    ']'));
                            }
                            else if (successStatus == GridClientResultBean.STATUS_FAILED) {
                                if (errorMsg == null || errorMsg.isEmpty())
                                    errorMsg = "Unknown server error.";

                                fut.onDone(new GridClientException(errorMsg));
                            }
                            else if (successStatus != GridClientResultBean.STATUS_SUCCESS) {
                                fut.onDone(new GridClientException("Unsupported server response status code" +
                                    ": " + successStatus));
                            }
                            else
                                fut.onDone(json.get("response"));
                        }
                        catch (Throwable e) {
                            fut.onDone(e);
                        }
                        finally {
                            pendingRequests.remove(this);
                        }
                    }

                    @Override protected void cancelBody() {
                        fut.onDone(new GridClientException("Failed to perform request (connection was closed before " +
                            "response is received): " + serverAddress()));

                        GridClientUtils.closeQuiet(input);
                    }
                };

                pendingRequests.add(worker);

                pool.execute(worker);

                return fut;
            }
            catch (IOException e) {
                throw new GridClientConnectionResetException("Failed to read response from remote server: " +
                    serverAddress(), e);
            }
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Builds request string with given parameters.
     *
     * @param params Request parameters.
     * @return Request string in URL format.
     */
    private String buildRequestString(Map<String, Object> params) {
        StringBuilder builder = new StringBuilder(sslContext() == null ? "http://" : "https://");

        builder.append(serverAddress().getHostName()).append(':').append(serverAddress().getPort()).append
            ("/gridgain?");

        params = new HashMap<String, Object>(params);

        if (sesTok != null)
            params.put("sessionToken", sesTok);
        else if (credentials() != null)
            params.put("cred", credentials());

        params.put("clientId", clientId.toString());

        for (Map.Entry<String, Object> entry : params.entrySet())
            if (!(entry.getValue() instanceof String))
                throw new IllegalArgumentException("Http connection supports only string arguments in requests" +
                    ", while received [key=" + entry.getKey() + ", value=" + entry.getValue() + "]");

        try {
            for (Map.Entry<String, Object> e : params.entrySet())
                builder.append(e.getKey()).append('=')
                    .append(URLEncoder.encode((String)e.getValue(), "UTF-8"))
                    .append('&');
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return builder.toString();
    }

    /**
     * Reads input stream contents, parses JSON object and closes input stream.
     *
     * @param input Input stream to read from.
     * @return JSON object parsed from input stream.
     * @throws IOException If input read failed.
     */
    private JSONObject readReply(InputStream input) throws IOException {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            StringBuilder buf = new StringBuilder();

            String line;

            while ((line = reader.readLine()) != null)
                buf.append(line);

            return JSONObject.fromObject(buf.toString());
        }
        finally {
            input.close();
        }
    }

    /**
     * Opens input stream from the specified URL.
     *
     * @param addr URL address.
     * @return Input stream.
     * @throws IOException If connection could not be established.
     */
    private InputStream openInputStream(String addr) throws IOException {
        URLConnection conn = new URL(addr).openConnection();

        if (sslContext() != null)
            ((HttpsURLConnection)conn).setSSLSocketFactory(sslContext().getSocketFactory());

        // Initiate connection.
        return conn.getInputStream();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> cachePutAll(String cacheName, Map<K, V> entries)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert entries != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "putall");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        int i = 1;

        for (Map.Entry<K, V> e : entries.entrySet()) {
            params.put("k" + i, e.getKey());
            params.put("v" + i, e.getValue());

            i++;
        }

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<V> cacheGet(String cacheName, K key)
        throws GridClientConnectionResetException, GridClientClosedException {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "get");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        params.put("key", key);

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Map<K, V>> cacheGetAll(final String cacheName, final Collection<K> keys)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert keys != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "getall");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        int i = 1;

        for (K key : keys) {
            params.put("k" + i, key);

            i++;
        }

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<Boolean> cacheRemove(final String cacheName, final K key)
        throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "rmv");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        params.put("key", key);

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<Boolean> cacheRemoveAll(final String cacheName, final Collection<K> keys)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert keys != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "rmvall");

        if (cacheName != null)
            params.put("cacheName", cacheName);

        int i = 1;

        for (K key : keys) {
            params.put("k" + i, key);

            i++;
        }

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> cacheAdd(String cacheName, K key, V val)
        throws GridClientConnectionResetException, GridClientClosedException {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "add");
        params.put("key", key);

        if (val != null)
            params.put("val", val);

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> cacheReplace(final String cacheName, final K key, final V val)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert key != null;
        assert val != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "rep");
        params.put("key", key);

        if (val != null)
            params.put("val", val);

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> cacheCompareAndSet(final String cacheName, final K key, final V val1,
        final V val2) throws GridClientConnectionResetException, GridClientClosedException {
        assert key != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "cas");
        params.put("key", key);

        if (val1 != null)
            params.put("val1", val1);

        if (val2 != null)
            params.put("val2", val2);

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<Map<String, ? extends Number>> cacheMetrics(final String cacheName,
        final K key) throws GridClientClosedException, GridClientConnectionResetException {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "cache");

        if (key != null)
            params.put("key", key);

        if (cacheName != null)
            params.put("cacheName", cacheName);

        return makeJettyRequest(params);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> GridClientFuture<R> execute(final String taskName, final Object taskArg)
        throws GridClientClosedException, GridClientConnectionResetException {
        assert taskName != null;

        Map<String, Object> paramsMap = new HashMap<String, Object>();

        paramsMap.put("cmd", "exe");
        paramsMap.put("name", taskName);

        if (taskArg != null)
            paramsMap.put("p1", taskArg);

        GridClientFutureAdapter fut = new GridClientFutureAdapter() {
            @Override public void onDone(Object res) {
                JSONObject json = (JSONObject)res;

                super.onDone(json.get("result"));
            }
        };

        return makeJettyRequest(paramsMap, fut);
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientNode> node(final UUID id, final boolean includeAttrs,
        final boolean includeMetrics) throws GridClientClosedException, GridClientConnectionResetException {
        assert id != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "node");
        params.put("id", id.toString());
        params.put("attr", String.valueOf(includeAttrs));
        params.put("mtr", String.valueOf(includeMetrics));

        GridClientFutureAdapter fut = new GridClientFutureAdapter() {
            @SuppressWarnings("unchecked")
            @Override public void onDone(Object res) {
                GridClientNodeImpl node = jsonBeanToNode(res);

                if (node != null)
                    top.updateNode(node);

                super.onDone(node);
            }
        };

        return makeJettyRequest(params, fut);
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientNode> node(final String ip, final boolean includeAttrs,
        final boolean includeMetrics) throws GridClientClosedException, GridClientConnectionResetException {
        assert ip != null;

        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "node");
        params.put("ip", ip);
        params.put("attr", String.valueOf(includeAttrs));
        params.put("mtr", String.valueOf(includeMetrics));

        GridClientFutureAdapter fut = new GridClientFutureAdapter() {
            @SuppressWarnings("unchecked")
            @Override public void onDone(Object res) {
                GridClientNodeImpl node = jsonBeanToNode(res);

                if (node != null)
                    top.updateNode(node);

                super.onDone(node);
            }
        };

        return makeJettyRequest(params, fut);
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<GridClientNode>> topology(final boolean includeAttrs,
        final boolean includeMetrics) throws GridClientClosedException, GridClientConnectionResetException {
        Map<String, Object> params = new HashMap<String, Object>();

        GridClientFutureAdapter fut = new GridClientFutureAdapter() {
            @SuppressWarnings("unchecked")
            @Override public void onDone(Object res) {
                assert res instanceof JSONArray : "Did not receive a JSON array [cls=" + res.getClass() + ", " +
                    "res=" + res + ']';

                JSONArray arr = (JSONArray)res;

                List<GridClientNodeImpl> nodeList = new ArrayList<GridClientNodeImpl>(arr.size());

                for (Object o : arr)
                    nodeList.add(jsonBeanToNode(o));

                top.updateTopology(nodeList);

                super.onDone(nodeList);
            }
        };

        params.put("cmd", "top");
        params.put("attr", String.valueOf(includeAttrs));
        params.put("mtr", String.valueOf(includeMetrics));

        return makeJettyRequest(params, fut);

    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<String>> log(final String path, final int fromLine, final int toLine)
        throws GridClientClosedException, GridClientConnectionResetException {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("cmd", "log");

        if (path != null)
            params.put("path", path);

        params.put("from", String.valueOf(fromLine));
        params.put("to", String.valueOf(toLine));

        GridClientFutureAdapter fut = new GridClientFutureAdapter() {
            @SuppressWarnings("unchecked")
            @Override public void onDone(Object res) {
                if (res == null || res instanceof JSONNull) {
                    super.onDone((Object) null);

                    return;
                }

                assert res instanceof JSONArray : "Did not receive a JSON array [cls=" + res.getClass() + ", " +
                    "res=" + res + ']';

                JSONArray arr = (JSONArray)res;

                List<String> list = new ArrayList<String>(arr.size());

                for (Object o : arr)
                    list.add((String)o);

                super.onDone(list);
            }
        };

        return makeJettyRequest(params, fut);
    }

    /**
     * Creates client node impl from json object representation.
     *
     * @param obj JSONObject (possibly JSONNull).
     * @return Converted client node.
     */
    private GridClientNodeImpl jsonBeanToNode(Object obj) {
        if (!(obj instanceof JSONObject))
            return null;

        JSONObject nodeBean = (JSONObject)obj;

        GridClientNodeImpl node = new GridClientNodeImpl();

        node.nodeId(UUID.fromString((String)nodeBean.get("nodeId")));
        node.internalAddresses((Collection<String>)nodeBean.get("internalAddresses"));
        node.externalAddresses((Collection<String>)nodeBean.get("internalAddresses"));
        node.tcpPort((Integer)nodeBean.get("tcpPort"));
        node.httpPort((Integer)nodeBean.get("jettyPort"));

        Map<String, GridClientCacheMode> caches = new HashMap<String, GridClientCacheMode>();

        if (nodeBean.get("caches") instanceof JSONObject) {
            Map<String, String> rawCaches = (Map<String, String>)nodeBean.get("caches");

            for (Map.Entry<String, String> e : rawCaches.entrySet())
                try {
                    caches.put(e.getKey(), GridClientCacheMode.valueOf(e.getValue()));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received from remote node (will ignore) [srv=" + serverAddress() +
                        ", cacheName=" + e.getKey() + ", cacheMode=" + e.getValue() + ']');
                }

            Object dfltCacheMode = nodeBean.get("defaultCacheMode");

            if (dfltCacheMode instanceof String && !((String)dfltCacheMode).isEmpty())
                try {
                    caches.put(null, GridClientCacheMode.valueOf((String)dfltCacheMode));
                }
                catch (IllegalArgumentException ignored) {
                    log.warning("Invalid cache mode received for default cache from remote node (will ignore) [srv="
                        + serverAddress() + ", cacheMode=" + dfltCacheMode + ']');
                }

            node.caches(caches);
        }

        Object attrs = nodeBean.get("attributes");

        if (attrs != null && !(attrs instanceof JSONNull))
            node.attributes((Map<String, Object>)attrs);

        Object metrics = nodeBean.get("metrics");

        if (metrics != null && !(metrics instanceof JSONNull))
            node.metrics((Map<String, Object>)metrics);

        return node;
    }

    /**
     * Worker for processing future responses.
     */
    private abstract static class FutureWorker implements Runnable {
        /** Pending future. */
        protected GridClientFutureAdapter fut;

        /** Completion latch. */
        private CountDownLatch latch = new CountDownLatch(1);

        /**
         * Creates worker.
         *
         * @param fut Future that is being processed.
         */
        protected FutureWorker(GridClientFutureAdapter fut) {
            this.fut = fut;
        }

        /** */
        @Override public void run() {
            try {
                body();
            }
            catch (Throwable e) {
                fut.onDone(e);
            }
            finally {
                latch.countDown();
            }
        }

        /**
         * Cancels worker and counts down the completion latch.
         */
        protected void cancel() {
            try {
                cancelBody();
            }
            finally {
                latch.countDown();
            }
        }

        /**
         * Wait for this future to complete or get cancelled.
         *
         * @throws InterruptedException If waiting thread was interrupted.
         */
        private void awaitCompletion() throws InterruptedException {
            latch.await();
        }

        /**
         * Future processing body.
         *
         * @throws Exception If any error occurs.
         */
        protected abstract void body() throws Exception;

        /**
         * Cancels this worker. This method will be invoked if executor was stopped and
         * this worker was not run.
         */
        protected abstract void cancelBody();
    }
}
