// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Text;
    using System.Net;
    using GridGain.Client;
    using GridGain.Client.Util;
    using GridGain.Client.Hasher;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Client node implementation.</summary> */
    internal class GridClientNodeImpl : IGridClientNode, IGridClientConsistentHashObject {
        /** <summary>Reference to a list of addresses.</summary> */
        private readonly HashSet<IPEndPoint> restAddresses = new HashSet<IPEndPoint>();

        /**
         * <summary>
         * Constructs grid client node.</summary>
         *
         * <param name="nodeId">Node ID.</param>
         */
        public GridClientNodeImpl(Guid nodeId) {
            Id = nodeId;
            InternalAddresses = new List<String>();
            ExternalAddresses = new List<String>();
            Attributes = new Dictionary<String, Object>();
            Metrics = null;
            Caches = new GridClientNullDictionary<String, GridClientCacheMode>();
        }

        /** <summary>Node id.</summary> */
        public Guid Id {
            get;
            private set;
        }

        /** <summary>List of node internal addresses.</summary> */
        public IList<String> InternalAddresses {
            get;
            private set;
        }

        /** <summary>List of node external addresses.</summary> */
        public IList<String> ExternalAddresses {
            get;
            private set;
        }

        /** <summary>Tcp remote port value.</summary> */
        public int TcpPort {
            get;
            set;
        }

        /** <summary>Http(s) port value.</summary> */
        public int HttpPort {
            get;
            set;
        }

        /** <summary>Node attributes.</summary> */
        public IDictionary<String, Object> Attributes {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public T Attribute<T>(String name) {
            return Attribute<T>(name, default(T));
        }

        /** <inheritdoc /> */
        public T Attribute<T>(String name, T def) {
            Object result;

            return Attributes.TryGetValue(name, out result) && (result is T) ? (T)result : def;
        }


        /** <summary>Node metrics.</summary> */
        public IGridClientNodeMetrics Metrics {
            get;
            set;
        }

        /** Caches available on remote node.*/
        public IDictionary<String, GridClientCacheMode> Caches {
            get;
            private set;
        }

        /**
         * <summary>
         * Gets list of all addresses available for connection for tcp rest binary protocol.</summary>
         *
         * <param name="proto">Protocol type.</param>
         * <returns>List of socket addresses.</returns>
         */
        public IList<IPEndPoint> AvailableAddresses(GridClientProtocol proto) {
            lock (restAddresses) {
                if (restAddresses.Count == 0) {
                    int port = proto == GridClientProtocol.Tcp ? TcpPort : HttpPort;

                    if (port != 0) {
                        foreach (String addr in InternalAddresses)
                            restAddresses.Add(new IPEndPoint(Dns.GetHostAddresses(addr)[0], port));

                        foreach (String addr in ExternalAddresses)
                            restAddresses.Add(new IPEndPoint(Dns.GetHostAddresses(addr)[0], port));
                    }
                }
            }

            Dbg.Assert(restAddresses.Count > 0, "at least one rest address is defined");

            return new List<IPEndPoint>(restAddresses);
        }

        /** <inheritdoc /> */
        override public bool Equals(Object obj) {
            if (this == obj)
                return true;

            GridClientNodeImpl that = obj as GridClientNodeImpl;

            return that != null && Id.Equals(that.Id);
        }

        /** <inheritdoc /> */
        override public int GetHashCode() {
            return Id.GetHashCode();
        }

        /** <inheritdoc /> */
        override public String ToString() {
            StringBuilder sb = new StringBuilder("GridClientNodeImpl");

            sb.AppendFormat(" [NodeId={0}", Id);
            sb.AppendFormat(", InternalAddresses={0}", InternalAddresses);
            sb.AppendFormat(", ExternalAddresses={0}", ExternalAddresses);
            sb.AppendFormat(", TcpPort={0}", TcpPort);
            sb.AppendFormat(", HttpPort={0}", HttpPort);
            sb.Append(']');

            return sb.ToString();
        }
    }
}
