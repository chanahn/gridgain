// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Net;
    using System.Collections.Generic;

    /** <summary>Node descriptor.</summary> */
    public interface IGridClientNode {
        /** <summary>Node ID.</summary> */
        Guid Id {
            get;
        }

        /** <summary>List of node internal addresses.</summary> */
        IList<String> InternalAddresses {
            get;
        }

        /** <summary>List of node external addresses.</summary> */
        IList<String> ExternalAddresses {
            get;
        }

        /** <summary>Remote tcp port.</summary> */
        int TcpPort {
            get;
        }

        /** <summary>Remote http port.</summary> */
        int HttpPort {
            get;
        }

        /** <summary>Node metrics.</summary> */
        IGridClientNodeMetrics Metrics {
            get;
        }

        /**
         * <summary>
         * All configured caches and their types on remote node: map in which
         * key is a configured cache name and value is a mode of this cache.</summary>
         */
        IDictionary<String, GridClientCacheMode> Caches {
            get;
        }

        /** <summary>Node attributes.</summary> */
        IDictionary<String, Object> Attributes {
            get;
        }

        /**
         * <summary>
         * Gets node attribute.</summary>
         *
         * <param name="name">Attribute name.</param>
         * <returns>Attribute value.</returns>
         */
        T Attribute<T>(String name);

        /**
         * <summary>
         * Gets node attribute.</summary>
         *
         * <param name="name">Attribute name.</param>
         * <param name="def">Default attribute value, if attribute not set.</param>
         * <returns>Attribute value.</returns>
         */
        T Attribute<T>(String name, T def);

        /**
         * <summary>
         * Gets list of addresses on which REST binary protocol is bound.</summary>
         *
         * <param name="proto">Protocol for which addresses are obtained.</param>
         * <returns>List of addresses.</returns>
         */
        IList<IPEndPoint> AvailableAddresses(GridClientProtocol proto);
    }
}
