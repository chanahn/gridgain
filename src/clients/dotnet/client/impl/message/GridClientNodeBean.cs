// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using System.Text;
    using System.Collections.Generic;
    using GridGain.Client.Util;

    /** <summary>Node bean.</summary> */
    internal class GridClientNodeBean {
        /** <summary>Constructs client node bean.</summary> */
        public GridClientNodeBean() {
            InternalAddresses = new HashSet<String>();
            ExternalAddresses = new HashSet<String>();
            Metrics = new Dictionary<String, Object>();
            Attributes = new Dictionary<String, Object>();
            Caches = new GridClientNullDictionary<String, String>();
        }


        /** <summary>Node ID.</summary> */
        public String NodeId {
            get;
            set;
        }

        /** <summary>Internal addresses.</summary> */
        public ICollection<String> InternalAddresses {
            get;
            private set;
        }

        /** <summary>External addresses.</summary> */
        public ICollection<String> ExternalAddresses {
            get;
            private set;
        }

        /** <summary>Gets metrics.</summary> */
        public IDictionary<String, Object> Metrics {
            get;
            private set;
        }

        /** <summary>Attributes.</summary> */
        public IDictionary<String, Object> Attributes {
            get;
            private set;
        }

        /** <summary>REST binary protocol port.</summary> */
        public int TcpPort {
            get;
            set;
        }

        /** <summary>REST http protocol port.</summary> */
        public int JettyPort {
            get;
            set;
        }

        /**
         * <summary>
         * Configured node caches - the map where key is cache name
         * and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").</summary>
         */
        public IDictionary<String, String> Caches {
            get;
            private set;
        }
    }
}
