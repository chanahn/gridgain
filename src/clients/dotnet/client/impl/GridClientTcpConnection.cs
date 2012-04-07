// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Threading;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Net.Security;
    using System.Security;

    using GridGain.Client;
    using GridGain.Client.Impl.Marshaller;
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Util;
    using GridGain.Client.Ssl;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    using CacheOp = GridGain.Client.Impl.Message.GridClientCacheRequestOperation;

    /**
     * <summary>
     * This class performs request to grid over tcp protocol. Serialization is performed
     * with marshaller provided.</summary>
     */
    class GridClientTcpConnection : GridClientConnectionAdapter {
        /** <summary>Ping packet.</summary> */
        private static readonly byte[] PING_PACKET = new byte[] { (byte)0x90, 0x00, 0x00, 0x00, 0x00 };

        /** <summary>Ping is sent every 3 seconds.</summary> */
        private static readonly TimeSpan PING_SND_TIME = TimeSpan.FromSeconds(3);

        /** <summary>Connection is considered to be half-opened if server did not respond to ping in 7 seconds.</summary> */
        private static readonly TimeSpan PING_RES_TIMEOUT = TimeSpan.FromSeconds(7);

        /** <summary>Socket read timeout.</summary> */
        private static readonly TimeSpan SOCK_READ_TIMEOUT = TimeSpan.FromSeconds(2);

        /** <summary>Request ID counter (should be modified only with Interlocked class).</summary> */
        private long reqIdCntr = 1;

        /** <summary>Requests that are waiting for response. Guarded by intrinsic lock.</summary> */
        private readonly IDictionary<long, GridClientFuture> pendingReqs = new ConcurrentDictionary<long, GridClientFuture>();

        /** <summary>Node by node id requests. Map for reducing server load.</summary> */
        private ConcurrentDictionary<Guid, GridClientFuture<IGridClientNode>> refreshNodeReqs =
            new ConcurrentDictionary<Guid, GridClientFuture<IGridClientNode>>();

        /** <summary>Authenticated session token.</summary> */
        protected byte[] sesTok;

        /** <summary>Lock for graceful shutdown.</summary> */
        private ReaderWriterLock closeLock = new ReaderWriterLock();

        /** <summary>Closed flag.</summary> */
        private volatile bool closed = false;

        /** <summary>Closed by idle request flag.</summary> */
        private volatile bool closedIdle = false;

        /** <summary>Reader thread management flag to wait completion on connection closing.</summary> */
        private volatile bool waitCompletion = true;

        /** <summary>Message marshaller</summary> */
        private IGridClientMarshaller marshaller;

        /** <summary>Underlying tcp client.</summary> */
        private readonly TcpClient tcp;

        /** <summary>Output stream.</summary> */
        private readonly Stream stream;

        /** <summary>Timestamp of last packet send event.</summary> */
        private DateTime lastPacketSndTime;

        /** <summary>Timestamp of last packet receive event.</summary> */
        private DateTime lastPacketRcvTime;

        /** <summary>Ping send time. (reader thread only).</summary> */
        private DateTime lastPingSndTime;

        /** <summary>Ping receive time (reader thread only).</summary> */
        private DateTime lastPingRcvTime;

        /** <summary>Reader thread.</summary> */
        private readonly Thread rdr;

        /**
         * <summary>
         * Creates a client facade, tries to connect to remote server, in case
         * of success starts reader thread.</summary>
         *
         * <param name="clientId">Client identifier.</param>
         * <param name="srvAddr">Server to connect to.</param>
         * <param name="sslCtx">SSL context to use if SSL is enabled, <c>null</c> otherwise.</param>
         * <param name="connectTimeout">Connect timeout.</param>
         * <param name="marshaller">Marshaller to use in communication.</param>
         * <param name="credentials">Client credentials.</param>
         * <param name="top">Topology instance.</param>
         * <exception cref="IOException">If connection could not be established.</exception>
         */
        public GridClientTcpConnection(Guid clientId, IPEndPoint srvAddr, IGridClientSslContext sslCtx, int connectTimeout,
            IGridClientMarshaller marshaller, Object credentials, GridClientTopology top)
            : base(clientId, srvAddr, sslCtx, credentials, top) {
            this.marshaller = marshaller;

            if (connectTimeout <= 0)
                connectTimeout = (int) SOCK_READ_TIMEOUT.TotalMilliseconds;

            Dbg.Assert(connectTimeout > 0, "connectTimeout > 0");

            int retries = 3;
            Exception firstCause = null;

            do {
                if (retries-- <= 0)
                    throw new IOException("Cannot open socket for address: " + srvAddr, firstCause);

                if (tcp != null)
                    Dbg.WriteLine("Connection to address {0} failed, try re-connect", srvAddr);

                try {
                    // Create a TCP/IP client socket.
                    tcp = new TcpClient();

                    tcp.ReceiveTimeout = connectTimeout;
                    tcp.SendTimeout = connectTimeout;

                    // Open connection.
                    tcp.Connect(srvAddr);
                }
                catch (Exception x) {
                    if (firstCause != null)
                        firstCause = x;

                    if (tcp != null && tcp.Connected)
                        tcp.Close();

                    continue;
                }
            } while (!tcp.Connected);

            if (sslCtx == null)
                stream = tcp.GetStream();
            else {
                stream = sslCtx.CreateStream(tcp);

                lock (stream) {
                    ((SslStream)stream).AuthenticateAsClient(srvAddr.Address.ToString());

                    stream.Flush();
                }
            }

            // Avoid immediate attempt to close by idle.
            lastPacketSndTime = lastPacketRcvTime = lastPingSndTime = lastPingRcvTime = U.Now;

            rdr = new Thread(readPackets);

            rdr.Name = "grid-tcp-connection--client#" + clientId + "--addr#" + srvAddr;

            //Dbg.WriteLine("Start thread: " + rdr.Name);

            rdr.Start();

            // Authenticate client connection.
            if (credentials != null)
                authenticate(credentials).WaitDone();
        }

        /**
         * <summary>
         * Closes this client. No methods of this class can be used after this method was called.
         * Any attempt to perform request on closed client will case <see ctype="GridClientConnectionResetException"/>.
         * All pending requests are failed without waiting for response.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> this method will wait for all pending requests to be completed.</param>
         */
        override sealed public void Close(bool waitCompletion) {
            closeLock.AcquireWriterLock(Timeout.Infinite);

            try {
                // Skip, if already closed.
                if (closed)
                    return;

                // Mark connection as closed.
                this.closed = true;
                this.closedIdle = false;
                this.waitCompletion = waitCompletion;
            }
            finally {
                closeLock.ReleaseWriterLock();
            }

            shutdown();
        }

        /**
         * <summary>
         * Closes client only if there are no pending requests in map.</summary>
         *
         * <returns>Idle timeout.</returns>
         * <returns><c>True</c> if client was closed.</returns>
         */
        override public bool CloseIfIdle(TimeSpan timeout) {
            closeLock.AcquireWriterLock(Timeout.Infinite);

            try {
                // Skip, if already closed.
                if (closed)
                    return true;

                // Skip, if inactivity is less then timeout
                if (U.Now - lastNetworkActivityTimestamp() < timeout)
                    return false;

                // Skip, if there are several pending requests.
                lock (pendingReqs) {
                    if (pendingReqs.Count != 0)
                        return false;
                }

                // Mark connection as closed.
                closed = true;
                closedIdle = true;
                waitCompletion = true;
            }
            finally {
                closeLock.ReleaseWriterLock();
            }

            shutdown();

            return true;
        }

        /** <summary>Closes all resources and fails all pending requests.</summary> */
        private void shutdown() {
            Dbg.Assert(closed);

            Dbg.Assert(!Thread.CurrentThread.Equals(rdr));

            //Dbg.WriteLine("Join thread: " + rdr.Name);

            rdr.Interrupt();
            rdr.Join();

            //Dbg.WriteLine("Thread stopped: " + rdr.Name);

            lock (stream) {
                U.DoSilent<Exception>(() => stream.Close(), null);
                U.DoSilent<Exception>(() => tcp.Close(), null);
            }

            IList<GridClientFuture> reqs;

            lock (pendingReqs) {
                reqs = new List<GridClientFuture>(pendingReqs.Values);

                pendingReqs.Clear();
            }

            foreach (GridClientFuture fut in reqs)
                fut.Fail(() => {
                    throw new GridClientException("Failed to perform request" + 
                        " (connection was closed before response is received): " + ServerAddress);
                });
        }

        /**
         * <summary>
         * </summary>
         *
         * <returns>Last network activity for this connection.</returns>
         */
        private DateTime lastNetworkActivityTimestamp() {
            return lastPacketSndTime > lastPacketRcvTime ? lastPacketSndTime : lastPacketRcvTime;
        }

        /**
         * <summary>
         * Makes request to server via tcp protocol and returns a future that will be completed when
         * response is received.</summary>
         *
         * <param name="msg">Message to request,</param>
         * <returns>Response object.</returns>
         * <exception cref="GridClientConnectionResetException">If request failed.</exception>
         * <exception cref="GridClientClosedException">If client was closed.</exception>
         */
        private GridClientFuture<T> makeRequest<T>(GridClientRequest msg) {
            Dbg.Assert(msg != null);

            GridClientFuture<T> res = new GridClientFuture<T>();

            return makeRequest(msg, res);
        }

        /**
         * <summary>
         * Makes request to server via tcp protocol and returns a future
         * that will be completed when response is received.</summary>
         *
         * <param name="msg">Message to request,</param>
         * <param name="fut">Future that will handle response.</param>
         * <returns>Response object.</returns>
         * <exception cref="GridClientConnectionResetException">If request failed.</exception>
         * <exception cref="GridClientClosedException">If client was closed.</exception>
         */
        private GridClientFuture<T> makeRequest<T>(GridClientRequest msg, GridClientFuture<T> fut) {
            Dbg.Assert(msg != null);

            // Close connection if tcp connection lost.
            if (!tcp.Connected)
                Close(false);

            // Validate this connection is alive.
            closeLock.AcquireReaderLock(Timeout.Infinite);

            try {
                if (closed) {
                    if (closedIdle)
                        throw new GridClientConnectionIdleClosedException("Connection was closed by idle thread (will " +
                            "reconnect): " + ServerAddress);

                    throw new GridClientClosedException("Failed to perform request (connection was closed before " +
                        "message is sent): " + ServerAddress);
                }

                // Update request properties.
                long reqId = Interlocked.Increment(ref reqIdCntr);

                msg.RequestId = reqId;

                msg.ClientId = ClientId;

                msg.SessionToken = sesTok;

                // Add request to pending queue.
                lock (pendingReqs) {
                    pendingReqs.Add(reqId, fut);
                }
            }
            finally {
                closeLock.ReleaseReaderLock();
            }

            try {
                sendPacket(msg);
            }
            catch (IOException e) {
                // In case of IOException we should shutdown the whole client since connection is broken.
                Close(false);

                throw new GridClientConnectionResetException("Failed to send message over network (will try to " +
                    "reconnect): " + ServerAddress, e);
            }

            return fut;
        }

        /**
         * <summary>
         * Handles incoming response message.</summary>
         *
         * <param name="msg">Incoming response message.</param>
         */
        private void handleResponse(GridClientResponse msg) {
            Dbg.Assert(Thread.CurrentThread.Equals(rdr));

            GridClientFuture fut;

            lock (pendingReqs) {
                long reqId = msg.RequestId;

                if (pendingReqs.TryGetValue(reqId, out fut))
                    pendingReqs.Remove(reqId);
                else
                    return;
            }

            // Update authentication session token.
            if (msg.SessionToken != null)
                sesTok = msg.SessionToken;
            else
                sesTok = null;

            switch (msg.Status) {
                case GridClientResponseStatus.Success:
                    fut.Done(msg.Result);

                    break;

                case GridClientResponseStatus.Failed:
                    var error = msg.ErrorMessage;

                    if (error == null)
                        error = "Unknown server error";

                    fut.Fail(() => {
                        throw new GridClientException(error);
                    });

                    break;

                case GridClientResponseStatus.AuthFailure:
                    fut.Fail(() => {
                        throw new GridClientAuthenticationException("Authentication failed (session expired?)" +
                        " errMsg=" + msg.ErrorMessage + ", [srv=" + ServerAddress + "]");
                    });

                    U.Async(() => Close(false));

                    break;

                default:
                    fut.Fail(() => {
                        throw new GridClientException("Unknown server response status code: " + msg.Status);
                    });

                    break;
            }
        }

        /**
         * <summary>
         * Tries to send packet over network.</summary>
         *
         * <param name="msg">Message being sent.</param>
         * <exception cref="IOException">If client was closed before message was sent over network.</exception>
         */
        private void sendPacket(GridClientRequest msg) {
            byte[] data = marshaller.Marshal(msg);
            byte[] size = U.ToBytes(data.Length);
            byte[] head = new byte[1 + size.Length + data.Length];

            // Prepare packet.
            head[0] = (byte)0x90;
            Array.Copy(size, 0, head, 1, 4);
            Array.Copy(data, 0, head, 5, data.Length);

            // Enqueue packet to send.
            lock (stream) {
                stream.Write(head, 0, head.Length);
                stream.Flush();
            }

            lastPacketSndTime = U.Now;
        }

        /**
         * <summary>
         * Authenticate this client with passed credentials.</summary>
         *
         * <param name="creds">Authentication credentials.</param>
         * <exception cref="GridClientAuthenticationException">In authentication fails.</exception>
         */
        private IGridClientFuture authenticate(Object creds) {
            Dbg.Assert(creds != null);

            GridClientAuthenticationRequest req = new GridClientAuthenticationRequest();

            req.Credentials = creds;

            GridClientFuture<Boolean> res = new GridClientFuture<Boolean>();

            res.DoneConverter = o => {
                // Ignore passed result, but validate updated session token.
                if (sesTok == null)
                    throw new GridClientAuthenticationException("Authenticated session token in response is null.");

                return true;
            };

            return makeRequest<Boolean>(req, res);
        }

        /**
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="entries">Entries.</param>
         * <returns><c>True</c> if map contained more then one entry or if put succeeded in case of one entry,</returns>
         *      <c>false</c> otherwise
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<Boolean> CachePutAll<K, V>(String cacheName, IDictionary<K, V> entries) {
            Dbg.Assert(entries != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.PutAll);

            req.CacheName = cacheName;
            req.Values = entries.ToMap();

            return makeRequest<Boolean>(req);
        }

        /**
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="keys">Keys.</param>
         * <returns>Entries.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<IDictionary<K, V>> CacheGetAll<K, V>(String cacheName, IEnumerable<K> keys) {
            Dbg.Assert(keys != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.GetAll);

            req.CacheName = cacheName;
            req.Keys = new HashSet<K>(keys);

            GridClientFuture<IDictionary<K, V>> fut = new GridClientFuture<IDictionary<K, V>>();

            fut.DoneConverter = o => {
                if (o == null)
                    return null;

                var map = o as IDictionary;

                if (map == null)
                    throw new ArgumentException("Expects dictionary, but received: " + o);

                return map.ToMap<K, V>();
            };

            return makeRequest<IDictionary<K, V>>(req, fut);
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <returns>Whether entry was actually removed.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<Boolean> CacheRemove<K>(String cacheName, K key) {
            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Rmv);

            req.CacheName = cacheName;
            req.Key = key;

            return makeRequest<Boolean>(req);
        }

        /**
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="keys">Keys.</param>
         * <returns>Whether entries were actually removed</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture CacheRemoveAll<K>(String cacheName, IEnumerable<K> keys) {
            Dbg.Assert(keys != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.RmvAll);

            req.CacheName = cacheName;
            req.Keys = new HashSet<K>(keys);

            return makeRequest<Boolean>(req);
        }

        /**
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether entry was added.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<Boolean> CacheAdd<K, V>(String cacheName, K key, V val) {
            Dbg.Assert(key != null);
            Dbg.Assert(val != null);

            GridClientCacheRequest add = new GridClientCacheRequest(CacheOp.Add);

            add.CacheName = cacheName;
            add.Key = key;
            add.Value = val;

            return makeRequest<Boolean>(add);
        }

        /**
         * <summary>
         * Replace value in cache.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether value was actually replaced.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<Boolean> CacheReplace<K, V>(String cacheName, K key, V val) {
            Dbg.Assert(key != null);
            Dbg.Assert(val != null);

            GridClientCacheRequest replace = new GridClientCacheRequest(CacheOp.Replace);

            replace.CacheName = cacheName;
            replace.Key = key;
            replace.Value = val;

            return makeRequest<Boolean>(replace);
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val1">Value 1.</param>
         * <param name="val2">Value 2.</param>
         * <returns>Whether new value was actually set.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<Boolean> CacheCompareAndSet<K, V>(String cacheName, K key, V val1, V val2) {
            Dbg.Assert(key != null);

            GridClientCacheRequest msg = new GridClientCacheRequest(CacheOp.Cas);

            msg.CacheName = cacheName;
            msg.Key = key;
            msg.Value = val1;
            msg.Value2 = val2;

            return makeRequest<Boolean>(msg);
        }

        /**
         * <summary>
         * Requests metrics for specified key in the cache.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <returns>Key metrics in cache.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<IGridClientDataMetrics> CacheMetrics<K>(String cacheName, K key) {
            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Metrics);

            req.CacheName = cacheName;
            req.Key = key;

            GridClientFuture<IGridClientDataMetrics> fut = new GridClientFuture<IGridClientDataMetrics>();

            fut.DoneConverter = o => {
                if (o == null)
                    return null;

                var map = o as IDictionary;

                if (map == null)
                    throw new ArgumentException("Expects dictionary, but received: " + o);

                var m = new Dictionary<String, Object>();

                foreach (DictionaryEntry entry in map)
                    m[(String)entry.Key] = entry.Value;

                return parseCacheMetrics(m);
            };

            return makeRequest<IGridClientDataMetrics>(req, fut);
        }

        /**
         * <summary>
         * Requests task execution with specified name.</summary>
         *
         * <param name="taskName">Task name.</param>
         * <param name="arg">Task argument.</param>
         * <returns>Task execution result.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<T> Execute<T>(String taskName, Object arg) {
            GridClientTaskRequest msg = new GridClientTaskRequest();

            msg.TaskName = taskName;
            msg.Argument = arg;

            GridClientFuture<GridClientTaskResultBean> fut = makeRequest<GridClientTaskResultBean>(msg);

            return new GridClientFinishedFuture<T>(() => (T)fut.Result.Result);
        }

        /**
         * <summary>
         * Convert response data into grid node bean.</summary>
         *
         * <param name="o">Response bean to convert into grid node.</param>
         * <returns>Converted grid node.</returns>
         */
        private IGridClientNode futureNodeConverter(Object o) {
            var bean = o as GridClientNodeBean;

            if (bean == null)
                return null;

            IGridClientNode node = nodeBeanToNode(bean);

            // Update node in topology.
            node = Top.UpdateNode(node);

            return node;
        }

        /**
         * <summary>
         * Requests node by its ID.</summary>
         *
         * <param name="id">Node ID.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Node.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics) {
            Dbg.Assert(id != null);

            GridClientFuture<IGridClientNode> fut;

            // Return request that is in progress.
            if (refreshNodeReqs.TryGetValue(id, out fut))
                return fut;

            fut = new GridClientFuture<IGridClientNode>();

            fut.DoneCallback = () => {
                GridClientFuture<IGridClientNode> removed;

                //Clean up the node id requests map.
                Dbg.Assert(refreshNodeReqs.TryRemove(id, out removed));

                Dbg.Assert(fut.Equals(removed));
            };

            fut.DoneConverter = this.futureNodeConverter;

            GridClientFuture<IGridClientNode> actual = refreshNodeReqs.GetOrAdd(id, fut);

            // If another thread put actual request into cache.
            if (!actual.Equals(fut))
                // Ignore created future, use one from cache.
                return actual;

            GridClientTopologyRequest msg = new GridClientTopologyRequest();

            msg.NodeId = id.ToString();
            msg.IncludeAttributes = includeAttrs;
            msg.IncludeMetrics = includeMetrics;

            return makeRequest<IGridClientNode>(msg, fut);
        }

        /**
         * <summary>
         * Requests node by its IP address.</summary>
         *
         * <param name="ipAddr">IP address.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Node.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<IGridClientNode> Node(String ipAddr, bool includeAttrs, bool includeMetrics) {
            GridClientFuture<IGridClientNode> fut = new GridClientFuture<IGridClientNode>();

            fut.DoneConverter = this.futureNodeConverter;

            GridClientTopologyRequest msg = new GridClientTopologyRequest();

            msg.NodeIP = ipAddr;
            msg.IncludeAttributes = includeAttrs;
            msg.IncludeMetrics = includeMetrics;

            return makeRequest(msg, fut);
        }

        /**
         * <summary>
         * Requests actual grid topology.</summary>
         *
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Nodes.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics) {
            GridClientFuture<IList<IGridClientNode>> fut = new GridClientFuture<IList<IGridClientNode>>();

            fut.DoneConverter = o => {
                var it = o as IEnumerable;

                if (it == null)
                    return null;

                IList<IGridClientNode> nodes = new List<IGridClientNode>();

                foreach (Object bean in it)
                    nodes.Add(nodeBeanToNode((GridClientNodeBean)bean));

                Top.UpdateTopology(nodes);

                return nodes;
            };


            GridClientTopologyRequest msg = new GridClientTopologyRequest();

            msg.IncludeAttributes = includeAttrs;
            msg.IncludeMetrics = includeMetrics;

            return makeRequest(msg, fut);
        }

        /**
         * <summary>
         * Requests server log entries in specified scope.</summary>
         *
         * <param name="path">Log file path.</param>
         * <param name="fromLine">Index of start line that should be retrieved.</param>
         * <param name="toLine">Index of end line that should be retrieved.</param>
         * <returns>Log file contents.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         */
        override public IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine) {
            GridClientLogRequest msg = new GridClientLogRequest();

            msg.From = fromLine;
            msg.To = toLine;
            msg.Path = path;

            return makeRequest<IList<String>>(msg);
        }

        /**
         * <summary>
         * Creates client node instance from message.</summary>
         *
         * <param name="nodeBean">Node bean message.</param>
         * <returns>Created node.</returns>
         */
        private GridClientNodeImpl nodeBeanToNode(GridClientNodeBean nodeBean) {
            if (nodeBean == null)
                return null;

            Guid nodeId = Guid.Parse(nodeBean.NodeId);

            GridClientNodeImpl node = new GridClientNodeImpl(nodeId);

            node.InternalAddresses.AddAll<String>(nodeBean.InternalAddresses);
            node.ExternalAddresses.AddAll<String>(nodeBean.ExternalAddresses);
            node.TcpPort = nodeBean.TcpPort;
            node.HttpPort = nodeBean.JettyPort;

            if (nodeBean.Caches != null && nodeBean.Caches.Count > 0)
                node.Caches.AddAll<KeyValuePair<String, GridClientCacheMode>>(parseCacheModes(nodeBean.Caches));

            if (nodeBean.Attributes != null && nodeBean.Attributes.Count > 0)
                node.Attributes.AddAll<KeyValuePair<String, Object>>(nodeBean.Attributes);

            if (nodeBean.Metrics != null && nodeBean.Metrics.Count > 0)
                node.Metrics = parseNodeMetrics(nodeBean.Metrics);

            return node;
        }

        /** <summary>Reader thread.</summary> */
        private void readPackets() {
            try {
                bool running = true;

                while (running) {
                    // Note that only this thread removes futures from map.
                    // So if we see closed condition, it is safe to check map size since no more futures
                    // will be added to the map.
                    if (closed) {
                        // Exit if either all requests processed or we do not wait for completion.
                        if (!waitCompletion)
                            break;

                        lock (pendingReqs) {
                            if (pendingReqs.Count == 0)
                                break;
                        }
                    }

                    // Header.
                    int symbol;

                    try {
                        symbol = readByte();
                    }
                    catch (TimeoutException) {
                        checkPing();

                        continue;
                    }

                    // Connection closed.
                    if (symbol == -1) {
                        Dbg.WriteLine("Connection closed by remote host " +
                                "[srvAddr=" + ServerAddress + ", symbol=" + symbol + "]");

                        break;
                    }

                    // Check for correct header.
                    if ((byte)symbol != (byte)0x90) {
                        Dbg.WriteLine("Failed to parse incoming message (unexpected header received, will close) " +
                                "[srvAddr=" + ServerAddress + ", symbol=" + symbol + "]");

                        break;
                    }

                    // Packet.
                    MemoryStream buf = new MemoryStream();

                    int len = 0;

                    while (true) {
                        try {
                            symbol = readByte();
                        }
                        catch (TimeoutException) {
                            checkPing();

                            continue;
                        }

                        if (symbol == -1) {
                            running = false;

                            break;
                        }

                        byte b = (byte)symbol;

                        buf.WriteByte(b);

                        if (len == 0) {
                            if (buf.Length == 4) {
                                len = U.BytesToInt32(buf.ToArray(), 0);

                                // Reset buffer.
                                buf.SetLength(0);

                                if (len == 0) {
                                    // Ping received.
                                    lastPingRcvTime = U.Now;

                                    break;
                                }
                            }
                        }
                        else {
                            if (buf.Length == len) {
                                GridClientResponse msg = marshaller.Unmarshal<GridClientResponse>(buf.ToArray());

                                // Reset buffer.
                                buf.SetLength(0);

                                len = 0;

                                lastPacketRcvTime = U.Now;

                                handleResponse(msg);

                                break;
                            }
                        }
                    }
                }
            }
            catch (IOException e) {
                if (!closed)
                    Dbg.WriteLine("Failed to read data from remote host (will close connection)" +
                        " [addr=" + ServerAddress + ", e=" + e.Message + "]");
            }
            catch (Exception e) {
                Dbg.Print("Unexpected throwable in connection reader thread (will close connection)" +
                    " [addr={0}, e={1}]", ServerAddress, e);
            }
            finally {
                U.Async(() => Close(false));
            }
        }


        /**
         * <summary>
         * Reads a byte from the stream.</summary>
         *
         * <returns>The byte from the stream or -1 input stream ends.</returns>
         * <exception cref="TimeoutException">If read socket operation is timed out.</exception>
         */
        private int readByte() {
            byte[] bin = new byte[1];

            try {
                return stream.ReadByte();
            }
            catch (Exception e) {
                if (e.InnerException is SocketException)
                    e = e.InnerException;

                var sockEx = e as SocketException;

                if (sockEx != null && sockEx.ErrorCode == 10060)
                    throw new TimeoutException(e.Message, e);

                // All other exceptions are interpreted as stream ends.
                throw;
            }
        }

        /**
         * <summary>
         * Checks last ping send time and last ping receive time.</summary>
         *
         * <exception cref="IOException">If</exception>
         */
        private void checkPing() {
            DateTime now = U.Now;

            DateTime lastRcvTime = lastPacketRcvTime > lastPingRcvTime ? lastPacketRcvTime : lastPingRcvTime;

            if (now - lastPingSndTime > PING_SND_TIME) {
                lock (stream) {
                    stream.Write(PING_PACKET, 0, PING_PACKET.Length);
                    stream.Flush();
                }

                lastPingSndTime = now;
            }

            if (now - lastRcvTime > PING_RES_TIMEOUT)
                throw new IOException("Did not receive any packets within ping response interval (connection is " +
                    "considered to be half-opened) [lastPingSendTime=" + lastPingSndTime + ", lastReceiveTime=" +
                    lastRcvTime + ", addr=" + ServerAddress + ']');
        }
    }
}
