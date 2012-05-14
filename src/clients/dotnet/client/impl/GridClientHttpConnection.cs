// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Collections.Generic;
    using System.Web.Script.Serialization;

    using GridGain.Client;
    using GridGain.Client.Ssl;
    using GridGain.Client.Util;
    using GridGain.Client.Impl.Message;

    using sc = System.Collections;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;


    /** <summary>Java client implementation.</summary> */
    class GridClientHttpConnection : GridClientConnectionAdapter {
        /** <summary>Busy lock for graceful close.</summary> */
        private ReaderWriterLock busyLock = new ReaderWriterLock();

        /** <summary>Pending requests.</summary> */
        private IList<GridClientFuture> pendingReqs = new List<GridClientFuture>();

        /** <summary>Closed flag.</summary> */
        private bool closed = false;

        /** <summary>Session token.</summary> */
        private volatile String sessionToken;

        /** <summary>Request ID counter (should be modified only with Interlocked class).</summary> */
        private long reqIdCntr = 1;

        /**
         * <summary>
         * Creates HTTP client connection.</summary>

         * <param name="clientId">Client identifier.</param>
         * <param name="srvAddr">Server address on which HTTP REST handler resides.</param>
         * <param name="sslCtx">SSL context to use if SSL is enabled, <c>null</c> otherwise.</param>
         * <param name="credentials">Client credentials.</param>
         * <param name="top">Topology to use.</param>
         * <exception cref="System.IO.IOException">If input-output exception occurs.</exception>
         */
        public GridClientHttpConnection(Guid clientId, IPEndPoint srvAddr, IGridClientSslContext sslCtx,
            Object credentials, GridClientTopology top)
            : base(clientId, srvAddr, sslCtx, credentials, top) {
            // Validate server is available.
            var args = new Dictionary<String, Object>();

            args.Add("cmd", "noop");

            try {
                new WebClient().DownloadString(BuildRequestString(args, null));
            }
            catch (System.Net.WebException e) {
                if (e.InnerException is System.Security.Authentication.AuthenticationException)
                    Dbg.Print("To use invalid certificates for secured HTTP client protocol provide system-wide" +
                        " setting: System.Net.ServicePointManager.ServerCertificateValidationCallback");

                throw; // Re-throw base exception.
            }
        }

        /** <inheritdoc /> */
        override public void Close(bool waitCompletion) {
            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                if (closed)
                    return;

                closed = true;
            }
            finally {
                busyLock.ReleaseWriterLock();
            }

            IList<GridClientFuture> reqs;

            lock (pendingReqs) {
                reqs = new List<GridClientFuture>(pendingReqs);

                pendingReqs.Clear();
            }

            if (waitCompletion)
                foreach (GridClientFuture req in reqs)
                    try {
                        req.WaitDone();
                    }
                    catch (GridClientException e) {
                        Dbg.WriteLine("Failed to get waiting result: {0}", e);
                    }
            else
                foreach (GridClientFuture req in reqs)
                    req.Fail(() => {
                        throw new GridClientException("Failed to perform request (connection was closed" +
                            "before response is received): " + ServerAddress);
                    });
        }

        /**
         * <summary>
         * Closes client only if there are no pending requests in map.</summary>
         *
         * <returns>Idle timeout.</returns>
         * <returns><c>True</c> if client was closed.</returns>
         */
        override public bool CloseIfIdle(TimeSpan timeout) {
            return closed;
        }

        /**
         * <summary>
         * Creates new future and passes it to the MakeJettyRequest.</summary>
         *
         * <param name="args">Request parameters.</param>
         * <param name="converter">Response converter to pass into generated future.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientClosedException">If client was manually closed.</exception>
         */
        private IGridClientFuture<T> MakeJettyRequest<T>(IDictionary<String, Object> args, Func<Object, T> converter) {
            Dbg.Assert(args != null);
            Dbg.Assert(args.ContainsKey("cmd"));

            var fut = new GridClientFuture<T>();

            fut.DoneConverter = converter;

            busyLock.AcquireReaderLock(Timeout.Infinite);

            try {
                if (closed)
                    throw new GridClientClosedException("Failed to perform request (connection was closed" +
                        " before request is sent): " + ServerAddress);

                var addr = BuildRequestString(args, sessionToken);
                var client = new WebClient();

                lock (pendingReqs) {
                    pendingReqs.Add(fut);
                }

                U.Async(() => {
                    try {
                        OnResponse(args, fut, client.DownloadString(addr));
                    }
                    catch (Exception e) {
                        fut.Fail(() => {
                            throw new GridClientException(e.Message, e);
                        });
                    }
                });

                return fut;
            }
            finally {
                busyLock.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Builds request string with given parameters.</summary>
         *
         * <param name="args">Request parameters.</param>
         * <param name="sessionToken">Session token.</param>
         * <returns>Request string in URL format.</returns>
         */
        private Uri BuildRequestString(IDictionary<String, Object> args, String sessionToken) {
            StringBuilder builder = new StringBuilder(SslCtx == null ? "http://" : "https://")
                .Append(ServerAddress.ToString())
                .Append("/gridgain?");

            args = new Dictionary<String, Object>(args);

            args["clientId"] = ClientId.ToString();
            args["reqId"] = Interlocked.Increment(ref reqIdCntr) + "";

            if (!String.IsNullOrEmpty(sessionToken))
                args["sessionToken"] = sessionToken;
            else if (Credentials != null)
                args["cred"] = Credentials;

            foreach (KeyValuePair<String, Object> pair in args)
                if (!(pair.Value is String))
                    throw new ArgumentException("Http connection supports only string arguments in requests" +
                        ", while received [key=" + pair.Key + ", value=" + pair.Value + "]");

            foreach (KeyValuePair<String, Object> e in args)
                // todo: key & value should be URL-encoded.
                builder.Append(e.Key).Append('=').Append((String)e.Value).Append('&');

            return new Uri(builder.Remove(builder.Length - 1, 1).ToString());
        }

        /**
         * <summary>
         * Handle server response.</summary>
         *
         * <param name="args">Parameters map.</param>
         * <param name="fut">Future to use.</param>
         * <param name="str">Downloaded string response.</param>
         */
        private void OnResponse(IDictionary<String, Object> args, GridClientFuture fut, String str) {
            try {
                JavaScriptSerializer s = new JavaScriptSerializer();

                // Parse json response.
                var json = (IDictionary<String, Object>)s.Deserialize(str, typeof(Object));

                // Recover status.
                GridClientResponseStatus statusCode = GridClientResponse.FindByCode((int)json["successStatus"]);

                // Retry with credentials, if authentication failed.
                if (statusCode == GridClientResponseStatus.AuthFailure) {
                    // Reset session token.
                    sessionToken = null;

                    // Re-send request with credentials and without session token.
                    str = new WebClient().DownloadString(BuildRequestString(args, null));

                    // Parse json response.
                    json = (IDictionary<String, Object>)s.Deserialize(str, typeof(IDictionary<String, Object>));

                    // Recover status.
                    statusCode = GridClientResponse.FindByCode((int) json["successStatus"]);
                }

                Object o;
                String errorMsg = null;

                if (json.TryGetValue("error", out o))
                    errorMsg = (String)o;

                if (String.IsNullOrEmpty(errorMsg))
                    errorMsg = "Unknown server error";

                if (statusCode == GridClientResponseStatus.AuthFailure) {
                    // Close this connection.
                    Close(false);

                    throw new GridClientAuthenticationException("Client authentication failed " +
                        "[clientId=" + ClientId + ", srvAddr=" + ServerAddress + ", errMsg=" + errorMsg + ']');
                }

                if (statusCode == GridClientResponseStatus.Failed)
                    throw new GridClientException(errorMsg);

                if (statusCode != GridClientResponseStatus.Success)
                    throw new GridClientException("Unsupported response status code: " + statusCode);

                // Update session token only on success and auth-failed responses.
                if (json.TryGetValue("sessionToken", out o))
                    sessionToken = o == null ? null : o.ToString();

                fut.Done(json["response"]);
            }
            catch (Exception e) {
                fut.Fail(() => {
                    throw new GridClientException(e.Message, e);
                });
            }
            finally {
                lock (pendingReqs) {
                    pendingReqs.Remove(fut);
                }
            }
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CachePutAll<TKey, TVal>(String cacheName, IDictionary<TKey, TVal> entries) {
            Dbg.Assert(entries != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "putall");

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            int i = 1;

            foreach (KeyValuePair<TKey, TVal> e in entries) {
                args.Add("k" + i, e.Key);
                args.Add("v" + i, e.Value);

                i++;
            }

            return MakeJettyRequest<Boolean>(args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<TVal> CacheGet<TKey, TVal>(String cacheName, TKey key) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "get");

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            args.Add("key", key);

            return MakeJettyRequest<TVal>(args, o => (TVal)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IDictionary<K, V>> CacheGetAll<K, V>(String cacheName, IEnumerable<K> keys) {
            Dbg.Assert(keys != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "getall");

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            int i = 1;

            foreach (K key in keys)
                args.Add("k" + i++, key);

            return MakeJettyRequest<IDictionary<K, V>>(args, o => AsMap<K, V>(o));
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheRemove<K>(String cacheName, K key) {
            Dbg.Assert(key != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "rmv");

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            args.Add("key", key);

            return MakeJettyRequest<bool>(args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture CacheRemoveAll<K>(String cacheName, IEnumerable<K> keys) {
            Dbg.Assert(keys != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "rmvall");

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            int i = 1;

            foreach (K key in keys)
                args.Add("k" + i++, key);

            return MakeJettyRequest<Boolean>(args, o => false);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheAdd<K, V>(String cacheName, K key, V val) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "add");
            args.Add("key", key);

            if (val != null)
                args.Add("val", val);

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            return MakeJettyRequest<Boolean>(args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheReplace<K, V>(String cacheName, K key, V val) {
            Dbg.Assert(key != null);
            Dbg.Assert(val != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "rep");
            args.Add("key", key);

            if (val != null)
                args.Add("val", val);

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            return MakeJettyRequest<Boolean>(args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheCompareAndSet<K, V>(String cacheName, K key, V val1, V val2) {
            Dbg.Assert(key != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "cas");
            args.Add("key", key);

            if (val1 != null)
                args.Add("val1", val1);

            if (val2 != null)
                args.Add("val2", val2);

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            return MakeJettyRequest<Boolean>(args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IGridClientDataMetrics> CacheMetrics<K>(String cacheName, K key) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "cache");

            if (key != null)
                args.Add("key", key);

            if (cacheName != null)
                args.Add("cacheName", cacheName);

            return MakeJettyRequest<IGridClientDataMetrics>(args, o => parseCacheMetrics((IDictionary<String, Object>)o));
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<T> Execute<T>(String taskName, Object arg) {
            Dbg.Assert(taskName != null);

            IDictionary<String, Object> paramsMap = new Dictionary<String, Object>();

            paramsMap.Add("cmd", "exe");
            paramsMap.Add("name", taskName);

            if (arg != null)
                paramsMap.Add("p1", arg);

            return MakeJettyRequest<T>(paramsMap, o => {
                var json = (IDictionary<String, Object>)o;

                return (T)json["result"];
            });
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics) {
            Dbg.Assert(id != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("id", id.ToString());

            return Node(args, includeAttrs, includeMetrics);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IGridClientNode> Node(String ip, bool includeAttrs, bool includeMetrics) {
            Dbg.Assert(ip != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("ip", ip);

            return Node(args, includeAttrs, includeMetrics);
        }

        /**
         * <summary>
         * Requests for the node.</summary>
         *
         * <param name="args">Request parameters.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Node.</returns>
         */
        private IGridClientFuture<IGridClientNode> Node(IDictionary<String, Object> args, bool includeAttrs, bool includeMetrics) {
            Dbg.Assert(args != null);

            args.Add("cmd", "node");
            args.Add("attr", includeAttrs.ToString());
            args.Add("mtr", includeMetrics.ToString());

            return MakeJettyRequest<IGridClientNode>(args, o => {
                GridClientNodeImpl node = JsonBeanToNode((IDictionary<String, Object>)o);

                if (node != null)
                    Top.UpdateNode(node);

                return node;
            });
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "top");
            args.Add("attr", includeAttrs.ToString());
            args.Add("mtr", includeMetrics.ToString());

            return MakeJettyRequest(args, o => {
                var json = o as Object[];

                if (json == null)
                    throw new ArgumentException("Expected a JSON array [cls=" + o.GetType() + ", res=" + o + ']');

                IList<IGridClientNode> nodeList = new List<IGridClientNode>(json.Length);

                foreach (var item in json)
                    nodeList.Add(JsonBeanToNode((IDictionary<String, Object>)item));

                Top.UpdateTopology(nodeList);

                return nodeList;
            });
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "log");

            if (path != null)
                args.Add("path", path);

            args.Add("from", fromLine);
            args.Add("to", toLine);

            return MakeJettyRequest<IList<String>>(args, o => {
                return new List<String>(); // TODO: implement!!!
            });
        }

        /**
         * <summary>
         * Creates client node impl from json object representation.</summary>
         *
         * <param name="json">JSONObject (possibly JSONNull).</param>
         * <returns>Converted client node.</returns>
         */
        private GridClientNodeImpl JsonBeanToNode(IDictionary<String, Object> json) {
            Guid nodeId = Guid.Parse(json["nodeId"].ToString());

            GridClientNodeImpl node = new GridClientNodeImpl(nodeId);

            node.InternalAddresses.AddAll<String>(AsList<String>(json["internalAddresses"]));
            node.ExternalAddresses.AddAll<String>(AsList<String>(json["externalAddresses"]));
            node.TcpPort = (int)json["tcpPort"];
            node.HttpPort = (int)json["jettyPort"];

            IDictionary<String, GridClientCacheMode> caches = new GridClientNullDictionary<String, GridClientCacheMode>();

            if (json.ContainsKey("caches")) {
                IDictionary<String, String> rawCaches = AsMap<String, String>(json["caches"]);

                Object dfltCacheMode;

                if (json.TryGetValue("defaultCacheMode", out dfltCacheMode)) {
                    String mode = dfltCacheMode as String;

                    if (!String.IsNullOrEmpty(mode)) {
                        rawCaches = rawCaches.ToNullable();

                        rawCaches.Add(null, mode);
                    }
                }

                caches = parseCacheModes(rawCaches);
            }

            if (caches.Count > 0)
                node.Caches.AddAll<KeyValuePair<String, GridClientCacheMode>>(caches);

            Object o;
            if (json.TryGetValue("attributes", out o) && o != null) {
                var map = AsMap<String, Object>(o);

                if (map.Count > 0)
                    node.Attributes.AddAll<KeyValuePair<String, Object>>(map);
            }

            if (json.TryGetValue("metrics", out o) && o != null) {
                var map = AsMap<String, Object>(o);

                if (map.Count > 0)
                    node.Metrics = parseNodeMetrics(AsMap<String, Object>(o));
            }

            return node;
        }

        /**
         * <summary>
         * Convert json object to list.</summary>
         *
         * <param name="json">Json object to convert.</param>
         * <returns>Resulting list.</returns>
         */
        private static IList<T> AsList<T>(Object json) {
            if (json == null)
                return null;

            IList<T> list = new List<T>();

            foreach (var o in (sc.IEnumerable)json)
                list.Add((T)o);

            return list;
        }

        /**
         * <summary>
         * Convert json object to map.</summary>
         *
         * <param name="json">Json object to convert.</param>
         * <returns>Resulting map.</returns>
         */
        private static IDictionary<K, V> AsMap<K, V>(Object json) {
            return AsMap<K, V>(json, v => (V)v);
        }

        /**
         * <summary>
         * Convert json object to list.</summary>
         *
         * <param name="json">Json object to convert.</param>
         * <param name="cast">Casting callback for each value in the resulting map.</param>
         * <returns>Resulting map.</returns>
         */
        private static IDictionary<K, V> AsMap<K, V>(Object json, Func<Object, V> cast) {
            if (json == null)
                return null;

            IDictionary<K, V> map = new Dictionary<K, V>();

            foreach (var o in (IDictionary<String, Object>)json)
                try {
                    map.Add((K)(Object)o.Key, cast(o.Value));
                }
                catch (Exception e) {
                    Console.Out.WriteLine("e=" + e);

                    throw;
                }

            return map;
        }
    }
}
