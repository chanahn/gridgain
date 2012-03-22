// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.client;

import org.gridgain.client.util.*;

import java.util.*;

/**
* Affinity function for partitioned cache. This function supports the following
* configuration:
* <ul>
* <li>
*      {@code backups} - Use ths flag to control how many back up nodes will be
*      assigned to every key. The default value is defined by {@link #DFLT_BACKUP_CNT}.
* </li>
* <li>
*      {@code replicas} - Generally the more replicas a node gets, the more key assignments
*      it will receive. You can configure different number of replicas for a node by
*      setting user attribute with name {@link #getReplicaCountAttributeName()} to some
*      number. Default value is {@code 512} defined by {@link #DFLT_REPLICA_CNT} constant.
* </li>
* <li>
*      {@code backupFilter} - Optional filter for back up nodes. If provided, then only
*      nodes that pass this filter will be selected as backup nodes and only nodes that
*      don't pass this filter will be selected as primary nodes. If not provided, then
*      primary and backup nodes will be selected out of all nodes available for this cache.
*      <p>
*      NOTE: In situations where there are no primary nodes at all, i.e. no nodes for which backup
*      filter returns {@code false}, first backup node for the key will be considered primary.
* </li>
* </ul>
*
* @author 2012 Copyright (C) GridGain Systems
* @version 4.0.0c.22032012
*/
public class GridClientPartitionedAffinity implements GridClientDataAffinity, GridClientTopologyListener {
    /** Default number of partitions. */
    public static final int DFLT_PARTITION_CNT = 521;

    /** Default number of backups. */
    public static final int DFLT_BACKUP_CNT = 1;

    /** Default replica count for partitioned caches. */
    public static final int DFLT_REPLICA_CNT = 512;

    /**
     * Name of node attribute to specify number of replicas for a node.
     * Default value is {@code gg:affinity:node:replicas}.
     */
    public static final String DFLT_REPLICA_CNT_ATTR_NAME = "gg:affinity:node:replicas";

    /** Node hash. */
    private GridClientConsistentHash<UUID> nodeHash;

    /** Total number of partitions. */
    private int parts = DFLT_PARTITION_CNT;

    /** */
    private int replicas = DFLT_REPLICA_CNT;

    /** */
    private int backups = DFLT_BACKUP_CNT;

    /** */
    private String attrName = DFLT_REPLICA_CNT_ATTR_NAME;

    /** Nodes IDs. */
    private Collection<UUID> addedNodes = new GridConcurrentHashSet<UUID>();

    /** Optional backup filter. */
    private GridClientPredicate<UUID> backupFilter;

    /** Optional primary filter. */
    private final GridClientPredicate<UUID> primaryFilter = new GridClientPredicate<UUID>() {
        @Override public boolean apply(UUID e) {
            GridClientPredicate<UUID> backup = backupFilter;

            return backup == null || !backupFilter.apply(e);
        }
    };

    /**
     * Empty constructor with all defaults.
     */
    public GridClientPartitionedAffinity() {
        this(DFLT_BACKUP_CNT);
    }

    /**
     * Initializes affinity with specified number of backups.
     *
     * @param backups Number of back up servers per key.
     */
    public GridClientPartitionedAffinity(int backups) {
        this(backups, DFLT_PARTITION_CNT, null, GridClientConsistentHash.MD5_HASHER);
    }

    /**
     * Initializes optional counts for replicas and backups.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @param backups Backups count.
     * @param parts Total number of partitions.
     * @param backupFilter Optional back up filter for nodes. If provided, then primary nodes
     *      will be selected from all nodes outside of this filter, and backups will be selected
     *      from all nodes inside it.
     * @param hasher Hasher that will be used to calculate key hashes.
     */
    public GridClientPartitionedAffinity(int backups, int parts, GridClientPredicate<UUID> backupFilter,
        GridClientConsistentHash.Hasher hasher) {
        this.backups = backups;
        this.parts = parts;
        this.backupFilter = backupFilter;

        nodeHash = new GridClientConsistentHash<UUID>(hasher);
    }

    /**
     * Gets default count of virtual replicas in consistent hash ring.
     * <p>
     * To determine node replicas, node attribute with {@link #getReplicaCountAttributeName()}
     * name will be checked first. If it is absent, then this value will be used.
     *
     * @return Count of virtual replicas in consistent hash ring.
     */
    public int getDefaultReplicas() {
        return replicas;
    }

    /**
     * Sets default count of virtual replicas in consistent hash ring.
     * <p>
     * To determine node replicas, node attribute with {@link #getReplicaCountAttributeName} name
     * will be checked first. If it is absent, then this value will be used.
     *
     * @param replicas Count of virtual replicas in consistent hash ring.s
     */
    public void setDefaultReplicas(int replicas) {
        this.replicas = replicas;
    }

    /**
     * Gets count of key backups for redundancy.
     *
     * @return Key backup count.
     */
    public int getKeyBackups() {
        return backups;
    }

    /**
     * Sets count of key backups for redundancy.
     *
     * @param backups Key backup count.
     */
    public void setKeyBackups(int backups) {
        this.backups = backups;
    }

    /**
     * Gets total number of key partitions. To ensure that all partitions are
     * equally distributed across all nodes, please make sure that this
     * number is significantly larger than a number of nodes. Also, partition
     * size should be relatively small. Try to avoid having partitions with more
     * than quarter million keys.
     * <p>
     * Note that for fully replicated caches this method should always
     * return {@code 1}.
     *
     * @return Total partition count.
     */
    public int getPartitions() {
        return parts;
    }

    /**
     * Sets total number of partitions.
     *
     * @param parts Total number of partitions.
     */
    public void setPartitions(int parts) {
        this.parts = parts;
    }

    /**
     * Gets optional backup filter. If not {@code null}, then primary nodes will be
     * selected from all nodes outside of this filter, and backups will be selected
     * from all nodes inside it.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @return Optional backup filter.
     */
    public GridClientPredicate<UUID> getBackupFilter() {
        return backupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then primary nodes will be selected
     * from all nodes outside of this filter, and backups will be selected from all
     * nodes inside it.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @param backupFilter Optional backup filter.
     */
    public void setBackupFilter(GridClientPredicate<UUID> backupFilter) {
        this.backupFilter = backupFilter;
    }

    /**
     * Gets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_CNT_ATTR_NAME}.
     *
     * @return User attribute name for replica count for a node.
     */
    public String getReplicaCountAttributeName() {
        return attrName;
    }

    /**
     * Sets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_CNT_ATTR_NAME}.
     *
     * @param attrName User attribute name for replica count for a node.
     */
    public void setReplicaCountAttributeName(String attrName) {
        this.attrName = attrName;
    }

    /** {@inheritDoc} */
    @Override public GridClientNode node(Object key, Collection<? extends GridClientNode> nodes) {
        int part = partition(key);
        
        if (nodes == null || nodes.isEmpty())
            return null;

        addIfAbsent(nodes);

        if (nodes.size() == 1) // Minor optimization.
            return GridClientUtils.first(nodes);

        Collection<UUID> nodeIds = new ArrayList<UUID>(nodes.size());

        for (GridClientNode node : nodes)
            nodeIds.add(node.nodeId());

        final Map<UUID, GridClientNode> lookup = new HashMap<UUID, GridClientNode>(nodes.size());

        // Store nodes in map for fast lookup.
        for (GridClientNode n : nodes)
            lookup.put(n.nodeId(), n);


        if (backupFilter != null) {
            UUID primaryId = nodeHash.node(part, primaryFilter, GridClientUtils.contains(nodeIds));

            if (primaryId != null)
                return lookup.get(primaryId);

            // Select from backup nodes.
            UUID backupId = nodeHash.node(part, backupFilter, GridClientUtils.contains(nodeIds));

            return lookup.get(backupId);
        }
        else {
            UUID nodeId = nodeHash.node(part, nodeIds);

            return lookup.get(nodeId);
        }
    }

    /** {@inheritDoc} */
    private int partition(Object key) {
        return Math.abs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public void onNodeAdded(GridClientNode node) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onNodeRemoved(GridClientNode node) {
        checkRemoved(node);
    }

    /**
     * @param n Node.
     * @return Replicas.
     */
    private int replicas(GridClientNode n) {
        Integer nodeReplicas = n.attribute(attrName);

        if (nodeReplicas == null)
            nodeReplicas = replicas;

        return nodeReplicas;
    }

    /**
     * @param nodes Nodes to add.
     */
    private void addIfAbsent(Iterable<? extends GridClientNode> nodes) {
        for (GridClientNode n : nodes)
            addIfAbsent(n);
    }

    /**
     * @param n Node to add.
     */
    private void addIfAbsent(GridClientNode n) {
        if (n != null && !addedNodes.contains(n.nodeId()))
            add(n.nodeId(), replicas(n));
    }

    /**
     * @param id Node ID to add.
     * @param replicas Replicas.
     */
    private void add(UUID id, int replicas) {
        nodeHash.addNode(id, replicas);

        addedNodes.add(id);
    }

    /**
     * Cleans up removed nodes.
     * @param node Node that was removed from topology.
     */
    private void checkRemoved(GridClientNode node) {
        addedNodes.remove(node.nodeId());
        
        nodeHash.removeNode(node.nodeId());
    }
}
