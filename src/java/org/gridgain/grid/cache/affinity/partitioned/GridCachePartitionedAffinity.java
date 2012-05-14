// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.partitioned;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.stopwatch.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Affinity function for partitioned cache. This function supports the following
 * configuration:
 * <ul>
 * <li>
 *      {@code backups} - Use ths flag to control how many back up nodes will be
 *      assigned to every key. The default value is defined by {@link #DFLT_BACKUP_COUNT}.
 * </li>
 * <li>
 *      {@code replicas} - Generally the more replicas a node gets, the more key assignments
 *      it will receive. You can configure different number of replicas for a node by
 *      setting user attribute with name {@link #getReplicaCountAttributeName()} to some
 *      number. Default value is {@code 512} defined by {@link #DFLT_REPLICA_COUNT} constant.
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
 * @version 4.0.3c.14052012
 */
public class GridCachePartitionedAffinity<K> implements GridCacheAffinity<K> {
    /** Flag to enable/disable consistency check (for internal use only). */
    private static final boolean AFFINITY_CONSISTENCY_CHECK = Boolean.getBoolean("GRIDGAIN_AFFINITY_CONSISTENCY_CHECK");

    /** Default number of partitions. */
    public static final int DFLT_PARTITION_COUNT = 521;

    /** Default number of backups. */
    public static final int DFLT_BACKUP_COUNT = 1;

    /** Default replica count for partitioned caches. */
    public static final int DFLT_REPLICA_COUNT = 512;

    /**
     * Name of node attribute to specify number of replicas for a node.
     * Default value is {@code gg:affinity:node:replicas}.
     */
    public static final String DFLT_REPLICA_COUNT_ATTR_NAME = "gg:affinity:node:replicas";

    /** Node hash. */
    private transient GridConsistentHash<UUID> nodeHash;

    /** Total number of partitions. */
    private int parts = DFLT_PARTITION_COUNT;

    /** */
    private int replicas = DFLT_REPLICA_COUNT;

    /** */
    private int backups = DFLT_BACKUP_COUNT;

    /** */
    private String attrName = DFLT_REPLICA_COUNT_ATTR_NAME;

    /** */
    private boolean exclNeighbors;

    /** Optional backup filter. */
    private GridPredicate<GridRichNode> backupFilter;

    /** Hasher function. */
    private GridConsistentHash.Hasher hasher = GridConsistentHash.MD5_HASHER;

    /** Initialization flag. */
    private AtomicBoolean init = new AtomicBoolean(false);

    /** Latch for initializing. */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private transient CountDownLatch initLatch = new CountDownLatch(1);

    /** */
    @GridInstanceResource
    private transient Grid grid;

    /** Nodes IDs. */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private transient Collection<UUID> addedNodes = new GridConcurrentHashSet<UUID>();

    /** Optional backup filter. */
    private final GridPredicate<UUID> backupIdFilter = new GridPredicate<UUID>() {
        @Override public boolean apply(UUID e) {
            if (backupFilter == null)
                return true;

            GridRichNode n = grid.node(e);

            return n != null && backupFilter.apply(n);
        }
    };

    /** Optional primary filter. */
    private final GridPredicate<UUID> primaryIdFilter = F0.not0(backupIdFilter);

    /** Map of neighbors. */
    @SuppressWarnings("TransientFieldNotInitialized")
    private transient ConcurrentMap<UUID, Collection<UUID>> neighbors =
        new GridConcurrentHashMap<UUID, Collection<UUID>>();

    /**
     * Empty constructor with all defaults.
     */
    public GridCachePartitionedAffinity() {
        // No-op.
    }

    /**
     * Initializes affinity with specified number of backups.
     *
     * @param backups Number of back up servers per key.
     */
    public GridCachePartitionedAffinity(int backups) {
        this.backups = backups;
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other
     * and specified number of backups.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     * @param backups Number of back up servers per key.
     */
    public GridCachePartitionedAffinity(boolean exclNeighbors, int backups) {
        this.exclNeighbors = exclNeighbors;
        this.backups = backups;
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other,
     * and specified number of backups and partitions.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     * @param backups Number of back up servers per key.
     * @param parts Total number of partitions.
     */
    public GridCachePartitionedAffinity(boolean exclNeighbors, int backups, int parts) {
        this.exclNeighbors = exclNeighbors;
        this.backups = backups;
        this.parts = parts;
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
     */
    public GridCachePartitionedAffinity(int backups, int parts, @Nullable GridPredicate<GridRichNode> backupFilter) {
        this.backups = backups;
        this.parts = parts;
        this.backupFilter = backupFilter;
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
    @Nullable public GridPredicate<GridRichNode> getBackupFilter() {
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
    public void setBackupFilter(@Nullable GridPredicate<GridRichNode> backupFilter) {
        this.backupFilter = backupFilter;
    }

    /**
     * Gets hasher function for consistent hash.
     *
     * @return Hasher function for consistent hash.
     */
    public GridConsistentHash.Hasher getHasher() {
        return hasher;
    }

    /**
     * Sets hasher function for consistent hash.
     *
     * @param hasher Hasher function for consistent hash.
     */
    public void setHasher(GridConsistentHash.Hasher hasher) {
        this.hasher = hasher;
    }

    /**
     * Gets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_COUNT_ATTR_NAME}.
     *
     * @return User attribute name for replica count for a node.
     */
    public String getReplicaCountAttributeName() {
        return attrName;
    }

    /**
     * Sets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_COUNT_ATTR_NAME}.
     *
     * @param attrName User attribute name for replica count for a node.
     */
    public void setReplicaCountAttributeName(String attrName) {
        this.attrName = attrName;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public void setExcludeNeighbors(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;
    }

    /**
     * Gets neighbors for a node.
     *
     * @param node Node.
     * @return Neighbors.
     */
    private Collection<UUID> neighbors(final GridRichNode node) {
        Collection<UUID> ns = neighbors.get(node.id());

        if (ns == null)
            ns = F.addIfAbsent(neighbors, node.id(), new ArrayList<UUID>(F.nodeIds(node.neighbors().nodes())));

        return ns;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRichNode> nodes(int part, Collection<GridRichNode> nodes) {
        if (nodes == null)
            return Collections.emptyList();

        int nodesSize = nodes.size();

        if (nodesSize == 0)
            return Collections.emptyList();

        GridStopwatch watch = W.stopwatch("AFFINITY_CHECK", false);

        try {
            initialize();

            addIfAbsent(nodes);

            if (nodesSize == 1) // Minor optimization.
                return nodes;

            final Map<UUID, GridRichNode> lookup = new GridLeanMap<UUID, GridRichNode>(nodesSize);

            // Store nodes in map for fast lookup.
            for (GridRichNode n : nodes)
                lookup.put(n.id(), n);

            Collection<UUID> nodeIds = lookup.keySet();

            Collection<UUID> selected;

            if (backupFilter != null) {
                UUID primaryId = nodeHash.node(part, primaryIdFilter, F.contains(nodeIds));

                Collection<UUID> backupIds = nodeHash.nodes(part, backups, backupIdFilter, F.contains(nodeIds));

                if (F.isEmpty(backupIds) && primaryId != null) {
                    GridRichNode n = lookup.get(primaryId);

                    assert n != null;

                    return Collections.singletonList(n);
                }

                selected = primaryId != null ? F.concat(false, primaryId, backupIds) : backupIds;
            }
            else {
                if (!exclNeighbors) {
                    selected = nodeHash.nodes(part, backups + 1, nodeIds);

                    if (selected.size() == 1) {
                        UUID id = F.first(selected);

                        assert id != null : "Node ID cannot be null in affinity node ID collection: " + selected;

                        GridRichNode n = lookup.get(id);

                        assert n != null;

                        return Collections.singletonList(n);
                    }
                }
                else {
                    selected = new ArrayList<UUID>(1 + backups);

                    final Collection<UUID> selected0 = selected;

                    List<UUID> ids = nodeHash.nodes(part, backups + 1, F.contains(nodeIds), new P1<UUID>() {
                        @Override public boolean apply(UUID id) {
                            GridRichNode n = lookup.get(id);

                            assert n != null;

                            Collection<UUID> neighbors = neighbors(n);

                            if (!F.containsAny(selected0, neighbors)) {
                                selected0.add(n.id());

                                return true;
                            }

                            return false;
                        }
                    });

                    if (AFFINITY_CONSISTENCY_CHECK)
                        assert F.eqOrdered(ids, selected);
                }
            }

            Collection<GridRichNode> ret = new ArrayList<GridRichNode>(1 + backups);

            for (UUID id : selected) {
                GridRichNode n = lookup.get(id);

                assert n != null;

                ret.add(n);
            }

            return ret;
        }
        finally {
            watch.stop();
        }
    }

    /** {@inheritDoc} */
    @Override public int partition(K key) {
        initialize();

        return Math.abs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        GridStopwatch watch = W.stopwatch("AFFINITY_PARTITIONS", false);

        try {
            initialize();

            return parts;
        }
        finally {
            watch.stop();
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        addedNodes = new GridConcurrentHashSet<UUID>();
        neighbors = new GridConcurrentHashMap<UUID, Collection<UUID>>();

        initLatch = new CountDownLatch(1);

        init.set(false);
    }

    /** {@inheritDoc}
     * @param nodeId*/
    @Override public void removeNode(UUID nodeId) {
        for (Iterator<UUID> it = addedNodes.iterator(); it.hasNext(); ) {
            UUID id = it.next();

            if (id.equals(nodeId)) {
                it.remove();

                nodeHash.removeNode(id);

                neighbors.clear();
            }
        }
    }

    /** {@inheritDoc} */
    private void initialize() {
        if (init.compareAndSet(false, true)) {
            nodeHash = new GridConsistentHash<UUID>(hasher);

            initLatch.countDown();
        }
        else {
            if (initLatch.getCount() > 0) {
                try {
                    initLatch.await();
                }
                catch (InterruptedException ignore) {
                    // No-op.
                }
            }
        }
    }

    /**
     * @param n Node.
     * @return Replicas.
     */
    private int replicas(GridNode n) {
        Integer nodeReplicas = n.attribute(attrName);

        if (nodeReplicas == null)
            nodeReplicas = replicas;

        return nodeReplicas;
    }

    /**
     * @param nodes Nodes to add.
     */
    private void addIfAbsent(Iterable<GridRichNode> nodes) {
        for (GridNode n : nodes)
            if (n != null && !addedNodes.contains(n.id()) && grid.node(n.id()) != null)
                add(n, replicas(n));
    }

    /**
     * @param n Node to add.
     * @param replicas Replicas.
     */
    private void add(GridNode n, int replicas) {
        neighbors.clear();

        UUID id = n.id();

        nodeHash.addNode(id, replicas);

        addedNodes.add(id);
    }
}
