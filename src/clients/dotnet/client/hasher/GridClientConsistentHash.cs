// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using GridGain.Client;

    using U = GridGain.Client.Util.GridClientUtils;

    /**
     * <summary>
     * Controls key to node affinity using consistent hash algorithm. This class is thread-safe
     * and does not have to be externally synchronized.</summary>
     *
     * <remarks>
     * For a good explanation of what consistent hashing is, you can refer to
     * <a href="http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html">Tom White's Blog</a>.
     * </remarks>
     */
    internal class GridClientConsistentHash {
        /** <summary>Prime number.</summary> */
        public static readonly int Prime = 15485857;

        /** <summary>Null value.</summary> */
        protected static readonly String Null = "";

        /**
         * <summary>
         * Constructs client consistent hash. Deny public construction of this class.</summary>
         */
        protected GridClientConsistentHash() {
        }

        /**
         * <summary>
         * Converts an object into byte array for hashing.</summary>
         *
         * <param name="val">Value to get hash bytes for.</param>
         * <returns>Byte array for hashing.</returns>
         */
        public static byte[] ToHashBytes(Object val) {
            if (val == null)
                val = Null;

            if (val is bool)
                // Convert to Java format
                return BitConverter.GetBytes((int)((bool)val ? 1231 : 1237));

            if (val is byte)
                return new byte[] { (byte)val };

            // Bytes order (from lowest to highest) is specific only for consistent hash.
            // So we use standart bit converter instead of GridClientUtils byte converters.

            if (val is char)
                return BitConverter.GetBytes((char)val);

            if (val is short)
                return BitConverter.GetBytes((short)val);

            if (val is ushort)
                return BitConverter.GetBytes((ushort)val);

            if (val is int)
                return BitConverter.GetBytes((int)val);

            if (val is uint)
                return BitConverter.GetBytes((uint)val);

            if (val is long)
                return BitConverter.GetBytes((long)val);

            if (val is ulong)
                return BitConverter.GetBytes((ulong)val);

            if (val is float)
                return BitConverter.GetBytes((float)val);

            if (val is double)
                return BitConverter.GetBytes((double)val);

            var byteArr = val as byte[];

            if (byteArr != null)
                return byteArr;

            var str = val as String;

            if (str != null)
                return System.Text.Encoding.UTF8.GetBytes(str);

            if (val is Guid)
                return U.ToBytes((Guid)val);

            if (val is IGridClientConsistentHashObject)
                return BitConverter.GetBytes((int)val.GetHashCode());

            throw new InvalidOperationException("Unsupported object (does object implement GridCLientConsistentHashObject?): " + val);
        }
    }

    /**
     * <summary>
     * Controls key to node affinity using consistent hash algorithm. This class is thread-safe
     * and does not have to be externally synchronized.</summary>
     *
     * <remarks>
     * For a good explanation of what consistent hashing is, you can refer to
     * <a href="http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html">Tom White's Blog</a>.
     * </remarks>
     */
    internal class GridClientConsistentHash<TNode> : GridClientConsistentHash {
        /** <summary>Random generator.</summary> */
        private readonly Random Rnd = new Random();

        /** <summary>Affinity seed.</summary> */
        private readonly Object affSeed;

        /** <summary>Hasher function.</summary> */
        private readonly IGridClientHasher hasher;

        /** <summary>Map of hash assignments.</summary> */
        private readonly C5.TreeDictionary<int, TNode> circle = new C5.TreeDictionary<int, TNode>();

        /** <summary>Read/write lock.</summary> */
        private readonly ReaderWriterLock rw = new ReaderWriterLock();

        /** <summary>Distinct nodes in the hash.</summary> */
        private ISet<TNode> nodes = new HashSet<TNode>();

        /**
         * <summary>
         * Constructs consistent hash using empty affinity seed and <c>MD5</c> hasher function.</summary>
         */
        public GridClientConsistentHash()
            : this(null, null) {
        }

        /**
         * <summary>
         * Constructs consistent hash using given affinity seed and <c>MD5</c> hasher function.</summary>
         *
         * <param name="affSeed">Affinity seed (will be used as key prefix for hashing).</param>
         */
        public GridClientConsistentHash(Object affSeed)
            : this(affSeed, null) {
        }

        /**
         * <summary>
         * Constructs consistent hash using given hasher function.</summary>
         *
         * <param name="hasher">Hasher function to use for generation of uniformly distributed hashes.</param>
         *      If <c>null</c>, then <c>null</c> hashing is used.
         */
        public GridClientConsistentHash(IGridClientHasher hasher)
            : this(null, hasher) {
        }

        /**
         * <summary>
         * Constructs consistent hash using given affinity seed and hasher function.</summary>
         *
         * <param name="affSeed">Affinity seed (will be used as key prefix for hashing).</param>
         * <param name="hasher">Hasher function to use for generation of uniformly distributed hashes.</param>
         *      If <c>null</c>, then <c>null</c> hashing is used.
         */
        public GridClientConsistentHash(Object affSeed, IGridClientHasher hasher) {
            this.affSeed = affSeed == null ? Prime : affSeed;
            this.hasher = hasher == null ? new GridClientMD5Hasher() : hasher;
        }

        /**
         * <summary>
         * Adds nodes to consistent hash algorithm (if nodes are <c>null</c> or empty, then no-op).</summary>
         *
         * <param name="nodes">Nodes to add.</param>
         * <param name="replicas">Number of replicas for every node.</param>
         */
        public void AddNodes(ICollection<TNode> nodes, int replicas) {
            if (nodes == null || nodes.Count == 0)
                return;

            rw.AcquireWriterLock(Timeout.Infinite);

            try {
                foreach (TNode node in nodes) {
                    AddNode(node, replicas);
                }
            }
            finally {
                rw.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Find key for node in the circle.</summary>
         *
         * <param name="node">Node to get key in the circle for.</param>
         * <returns>Key in the circle for specified node or -1 if such node not found.</returns>
         */
        private int FindCircleKey(TNode node) {
            foreach (C5.KeyValuePair<int, TNode> pair in circle) {
                if (pair.Value == null && node == null)
                    return pair.Key;

                if (pair.Value != null && pair.Value.Equals(node))
                    return pair.Key;
            }

            return -1;
        }

        /**
         * <summary>
         * Adds a node to consistent hash algorithm.</summary>
         *
         * <param name="node">New node (if <c>null</c> then no-op).</param>
         * <param name="replicas">Number of replicas for the node.</param>
         * <returns><c>True</c> if node was added, <c>null</c> if it is <c>null</c> or</returns>
         *      is already contained in the hash.
         */
        public bool AddNode(TNode node, int replicas) {
            if (node == null)
                return false;

            long seed = affSeed.GetHashCode() * 31 + GetHash(node);

            rw.AcquireWriterLock(Timeout.Infinite);

            try {
                if (FindCircleKey(node) == -1) {
                    int hash = GetHash(seed);

                    bool added = false;

                    if (!circle.Contains(hash)) {
                        circle.Add(hash, node);

                        added = true;
                    }

                    for (int i = 1; i <= replicas; i++) {
                        seed = seed * affSeed.GetHashCode() + i;

                        hash = GetHash(seed);

                        if (!circle.Contains(hash)) {
                            circle.Add(hash, node);

                            added = true;
                        }
                    }

                    if (added)
                        nodes.Add(node);

                    return added;
                }

                return false;
            }
            finally {
                rw.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Removes a node and all of its replicas.</summary>
         *
         * <param name="node">Node to remove (if <c>null</c>, then no-op).</param>
         * <returns><c>True</c> if node was removed, <c>True</c> if node is <c>True</c> or</returns>
         *      not present in hash.
         */
        public bool RemoveNode(TNode node) {
            if (node == null)
                return false;

            rw.AcquireWriterLock(Timeout.Infinite);

            try {
                bool rmv = nodes.Remove(node);

                if (rmv)
                    foreach (C5.KeyValuePair<int, TNode> pair in circle.Snapshot())
                        if (node.Equals(pair.Value))
                            circle.Remove(pair.Key);

                return rmv;
            }
            finally {
                rw.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Gets number of distinct nodes, excluding replicas, in consistent hash.</summary>
         *
         * <returns>Number of distinct nodes, excluding replicas, in consistent hash.</returns>
         */
        public int Count {
            get {
                rw.AcquireReaderLock(Timeout.Infinite);

                try {
                    return nodes.Count;
                }
                finally {
                    rw.ReleaseReaderLock();
                }
            }
        }

        /**
         * <summary>
         * Gets size of all nodes (including replicas) in consistent hash.</summary>
         *
         * <returns>Size of all nodes (including replicas) in consistent hash.</returns>
         */
        public int Size {
            get {
                rw.AcquireReaderLock(Timeout.Infinite);

                try {
                    return circle.Count;
                }
                finally {
                    rw.ReleaseReaderLock();
                }
            }
        }

        /**
         * <summary>
         * Checks if consistent hash has nodes added to it.</summary>
         *
         * <returns><c>True</c> if consistent hash is empty, <c>null</c> otherwise.</returns>
         */
        public bool IsEmpty {
            get {
                return Size == 0;
            }
        }

        /**
         * <summary>
         * Picks a random node from consistent hash.</summary>
         *
         * <returns>Random node from consistent hash or <c>null</c> if there are no nodes.</returns>
         */
        public TNode Random {
            get {
                IList<TNode> nodes = Nodes;

                return nodes.Count == 0 ? default(TNode) : nodes[Rnd.Next(nodes.Count)];
            }
        }

        /**
         * <summary>
         * Gets set of all distinct nodes in the consistent hash (in no particular order).</summary>
         *
         * <returns>Set of all distinct nodes in the consistent hash.</returns>
         */
        public IList<TNode> Nodes {
            get {
                rw.AcquireReaderLock(Timeout.Infinite);

                try {
                    return new List<TNode>(nodes);
                }
                finally {
                    rw.ReleaseReaderLock();
                }
            }
        }

        /**
         * <summary>
         * Gets node for a key.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Node.</returns>
         */
        public TNode Node(Object key) {
            return Node(key, U.All<TNode>());
        }

        /**
         * <summary>
         * Gets node for a given key.</summary>
         *
         * <param name="key">Key to get node for.</param>
         * <param name="inc">Optional inclusion set. Only nodes contained in this set may be returned.</param>
         *      If <c>null</c>, then all nodes may be included.
         * <returns>Node for key, or <c>null</c> if node was not found.</returns>
         */
        public TNode Node(Object key, ICollection<TNode> inc) {
            return Node(key, inc, null);
        }

        /**
         * <summary>
         * Gets node for a given key.</summary>
         *
         * <param name="key">Key to get node for.</param>
         * <param name="inc">Optional inclusion set. Only nodes contained in this set may be returned.</param>
         *      If <c>null</c>, then all nodes may be included.
         * <param name="exc">Optional exclusion set. Only nodes not contained in this set may be returned.</param>
         *      If <c>null</c>, then all nodes may be returned.
         * <returns>Node for key, or <c>null</c> if node was not found.</returns>
         */
        public TNode Node(Object key, ICollection<TNode> inc, ICollection<TNode> exc) {
            return Node(key, U.Filter<TNode>(inc, exc));
        }

        /**
         * <summary>
         * Gets node for a given key.</summary>
         *
         * <param name="key">Key to get node for.</param>
         * <param name="filter">Optional predicate for node filtering.</param>
         * <returns>Node for key, or <c>null</c> if node was not found.</returns>
         */
        public TNode Node(Object key, Predicate<TNode> filter) {
            int hash = GetHash(key);

            if (filter == null)
                filter = U.All<TNode>();

            rw.AcquireReaderLock(Timeout.Infinite);

            try {
                // Get the first node by hash in the circle clock-wise.
                foreach (C5.KeyValuePair<int, TNode> pair in circle.RangeFrom(hash))
                    if (filter(pair.Value))
                        return pair.Value;

                foreach (C5.KeyValuePair<int, TNode> pair in circle.RangeTo(hash))
                    if (filter(pair.Value))
                        return pair.Value;

                return default(TNode); // Circle is empty.
            }
            finally {
                rw.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Gets hash code for a given object.</summary>
         *
         * <param name="val">Value to get hash code for.</param>
         * <returns>Hash code.</returns>
         */
        protected internal int GetHash(Object val) {
            return hasher.Hash(ToHashBytes(val));
        }
    }
}
