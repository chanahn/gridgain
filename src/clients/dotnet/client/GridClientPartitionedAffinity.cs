// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;

    using System.Collections.Generic;
    using System.Collections.Concurrent;

    using GridGain.Client.Util;
    using GridGain.Client.Hasher;

    using A = GridGain.Client.Util.GridClientArgumentCheck;

    /**
     * <summary>
     * Affinity function for partitioned cache. This function supports the following configuration:
     * <ul>
     * <li>
     *     <c>backups</c> - Use ths flag to control how many back up nodes will be
     *     assigned to every key. The default value is defined by <see ctype="DefaultBackupsCount"/>.
     * </li>
     * <li>
     *      <c>replicas</c> - Generally the more replicas a node gets, the more key assignments
     *      it will receive. You can configure different number of replicas for a node by
     *      setting user attribute with name <see ctype="#getReplicaCountAttributeName()"/> to some
     *      number. Default value is <c>512</c> defined by <see ctype="#DFLT_REPLICA_CNT"/> constant.
     * </li>
     * <li>
     *      <c>backupFilter</c> - Optional filter for back up nodes. If provided, then only
     *      nodes that pass this filter will be selected as backup nodes and only nodes that
     *      don't pass this filter will be selected as primary nodes. If not provided, then
     *      primary and backup nodes will be selected out of all nodes available for this cache.
     *      <para/>
     *      NOTE: In situations where there are no primary nodes at all, i.e. no nodes for which backup
     *      filter returns <c>false</c>, first backup node for the key will be considered primary.
     * </li>
     * </ul>
     * </summary>
     */
    public class GridClientPartitionedAffinity : IGridClientDataAffinity, IGridClientTopologyListener {
        /** <summary>Default number of partitions.</summary> */
        public const int DefaultPartitionsCount = 521;

        /** <summary>Default number of backups.</summary> */
        public const int DefaultBackupsCount = 1;

        /** <summary>Default replica count for partitioned caches.</summary> */
        public const int DefaultReplicasCount = 512;

        /**
         * <summary>
         * Name of node attribute to specify number of replicas for a node.
         * Default value is <c>gg:affinity:node:replicas</c>.</summary>
         */
        public const String DefaultReplicasCountAttributeName = "gg:affinity:node:replicas";

        /** <summary>Node hash.</summary> */
        private GridClientConsistentHash<Guid> nodeHash;

        /** <summary>Set of node IDs.</summary> */
        private HashSet<Guid> addedNodes = new HashSet<Guid>();

        /** <summary>Empty constructor with all defaults.</summary> */
        public GridClientPartitionedAffinity()
            : this(DefaultBackupsCount) {
        }

        /**
         * <summary>
         * Initializes affinity with specified number of backups.</summary>
         *
         * <param name="backups">Number of back up servers per key.</param>
         */
        public GridClientPartitionedAffinity(int backups)
            : this(backups, DefaultPartitionsCount, null, new GridClientMD5Hasher()) {
        }

        /**
         * <summary>
         * Initializes optional counts for replicas and backups.
         * <para/>
         * Note that <c>excludeNeighbors</c> parameter is ignored if <c>backupFilter</c> is set.</summary>
         *
         * <param name="backups">Backups count.</param>
         * <param name="parts">Total number of partitions.</param>
         * <param name="backupsFilter">Optional back up filter for nodes. If provided, then primary nodes</param>
         *      will be selected from all nodes outside of this filter, and backups will be selected
         *      from all nodes inside it.
         * <param name="hasher">Hasher that will be used to calculate key hashes.</param>
         */
        public GridClientPartitionedAffinity(int backups, int parts, Predicate<Guid> backupsFilter, IGridClientHasher hasher) {
            Partitions = parts;
            BackupsCount = backups;
            BackupsFilter = backupsFilter;

            nodeHash = new GridClientConsistentHash<Guid>(hasher);

            ReplicasCount = DefaultReplicasCount;
            ReplicasCountAttributeName = DefaultReplicasCountAttributeName;
        }

        /**
         * <summary>
         * Number of virtual replicas in consistent hash ring.
         * <para/>
         * To determine node replicas, node attribute with <see ctype="#getReplicaCountAttributeName()"/>
         * name will be checked first. If it is absent, then this value will be used.</summary>
         */
        public int ReplicasCount {
            get;
            set;
        }

        /** <summary>Count of key backups for redundancy.</summary> */
        public int BackupsCount {
            get;
            set;
        }

        /**
         * <summary>
         * Total number of key partitions. To ensure that all partitions are
         * equally distributed across all nodes, please make sure that this
         * number is significantly larger than a number of nodes. Also, partition
         * size should be relatively small. Try to avoid having partitions with more
         * than quarter million keys.
         * <para/>
         * Note that for fully replicated caches this method should always
         * return <c>1</c>.</summary>
         */
        public int Partitions {
            get;
            set;
        }

        /**
         * <summary>
         * Optional backup filter. If not <c>null</c>, then primary nodes will be
         * selected from all nodes outside of this filter, and backups will be selected
         * from all nodes inside it.
         * <para/>
         * Note that <c>excludeNeighbors</c> parameter is ignored if <c>backupFilter</c> is set.</summary>
         */
        public Predicate<Guid> BackupsFilter {
            get;
            set;
        }

        /**
         * <summary>
         * Gets optional attribute name for replica count. If not provided, the
         * default is <see ctype="#DefaultReplicaCountAttributeName"/>.</summary>
         */
        public String ReplicasCountAttributeName {
            get;
            set;
        }

        /** <inheritdoc /> */
        public TNode Node<TNode>(Object key, ICollection<TNode> nodes) where TNode : IGridClientNode {
            if (nodes == null || nodes.Count == 0)
                return default(TNode);

            int part = Partition(key);

            AddIfAbsent(nodes);

            // Store nodes in map for fast lookup.
            IDictionary<Guid, TNode> lookup = new Dictionary<Guid, TNode>(nodes.Count);
            foreach (TNode n in nodes)
                lookup.Add(n.Id, n);

            Guid nodeId;

            if (BackupsFilter == null) {
                nodeId = nodeHash.Node(part, lookup.Keys);
            }
            else {
                nodeId = nodeHash.Node(part, LookupFilter<TNode>(lookup, BackupsFilter, false));

                if (nodeId == null)
                    // Select from backup nodes.
                    nodeId = nodeHash.Node(part, LookupFilter<TNode>(lookup, BackupsFilter, true));
            }

            TNode node;

            lookup.TryGetValue(nodeId, out node);

            return node;
        }

        /** <inheritdoc /> */
        private int Partition(Object key) {
            return Math.Abs(key.GetHashCode() % Partitions);
        }

        /** <inheritdoc /> */
        public void OnNodeAdded(IGridClientNode node) {
            // No-op.
        }

        /** <inheritdoc /> */
        public void OnNodeRemoved(IGridClientNode node) {
            CheckRemoved(node);
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="n">Node.</param>
         * <returns>Replicas.</returns>
         */
        private int Replicas(IGridClientNode n) {
            int nodeReplicas = n.Attribute<int>(ReplicasCountAttributeName, -1);

            if (nodeReplicas <= 0)
                nodeReplicas = ReplicasCount;

            return nodeReplicas;
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="nodes">Nodes to add.</param>
         */
        private void AddIfAbsent<TNode>(IEnumerable<TNode> nodes) where TNode : IGridClientNode {
            foreach (IGridClientNode n in nodes)
                AddIfAbsent(n);
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="n">Node to add.</param>
         */
        private void AddIfAbsent(IGridClientNode n) {
            lock (addedNodes) {
                if (n != null && !addedNodes.Contains(n.Id))
                    Add(n.Id, Replicas(n));
            }
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="id">Node ID to add.</param>
         * <param name="replicas">Replicas.</param>
         */
        private void Add(Guid id, int replicas) {
            lock (addedNodes) {
                if (addedNodes.Add(id))
                    nodeHash.AddNode(id, replicas);
            }
        }

        /**
         * <summary>
         * </summary>
         *
         * Cleans up removed nodes.
         * <param name="node">Node that was removed from topology.</param>
         */
        private void CheckRemoved(IGridClientNode node) {
            lock (addedNodes) {
                if (addedNodes.Remove(node.Id))
                    nodeHash.RemoveNode(node.Id);
            }
        }

        /** <summary>Lookup filter.</summary> */
        private static Predicate<Guid> LookupFilter<TNode>(IDictionary<Guid, TNode> lookup, Predicate<Guid> backuped, bool inBackup) {
            return delegate(Guid id) {
                return lookup.ContainsKey(id) && backuped(id) == inBackup;
            };
        }
    }
}
