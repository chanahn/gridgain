// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

/**
 * Client topology cache.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
class GridClientTopology {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientTopology.class.getName());

    /** Topology cache */
    private Map<UUID, GridClientNodeImpl> nodes = Collections.emptyMap();

    /** Flag indicating whether node attributes and metrics should be cached. */
    private boolean topCache;

    /** Lock for topology changing. */
    private ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Topology listeners. */
    private Collection<GridClientTopologyListener> topLsnrs = new ConcurrentLinkedQueue<GridClientTopologyListener>();

    /** Executor for listener notification. */
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Creates topology instance.
     *
     * @param topCache If {@code true}, then topology will cache node attributes and metrics.
     */
    GridClientTopology(boolean topCache) {
        this.topCache = topCache;
    }

    /**
     * Adds topology listener.
     *
     * @param lsnr Topology listener.
     */
    void addTopologyListener(GridClientTopologyListener lsnr) {
        topLsnrs.add(lsnr);
    }

    /**
     * Removes topology listener.
     *
     * @param lsnr Topology listener.
     */
    void removeTopologyListener(GridClientTopologyListener lsnr) {
        topLsnrs.remove(lsnr);
    }

    /**
     * Returns all added topology listeners.
     *
     * @return Unmodifiable view of topology listeners.
     */
    public Collection<GridClientTopologyListener> topologyListeners() {
        return Collections.unmodifiableCollection(topLsnrs);
    }

    /**
     * Updates topology if cache enabled. If cache is disabled, returns original node.
     *
     * @param node Converted rest server response.
     * @return Node in topology.
     */
    GridClientNode updateNode(GridClientNodeImpl node) {
        busyLock.writeLock().lock();

        try {
            boolean nodeAdded = !nodes.containsKey(node.nodeId());

            // We update the whole topology if node was not in topology or we cache metrics.
            if (nodeAdded || topCache) {
                Map<UUID, GridClientNodeImpl> updatedTop = new HashMap<UUID, GridClientNodeImpl>(nodes);

                updatedTop.put(node.nodeId(), clearAttributes(node));

                // Change the reference to new topology.
                // So everyone who captured old version will see a consistent snapshot.
                nodes = updatedTop;
            }

            if (nodeAdded)
                notifyEvents(Collections.singletonList(new TopologyEvent(true, node)));

            return node;
        }
        finally {
            busyLock.writeLock().unlock();
        }
    }

    /**
     * Updates (if cache is enabled) the whole topology. If cache is disabled, original collection is returned.
     *
     * @param nodeList Converted rest server response.
     * @return Topology nodes.
     */
    Collection<? extends GridClientNode> updateTopology(Collection<GridClientNodeImpl> nodeList) {
        Collection<TopologyEvent> evts = new LinkedList<TopologyEvent>();

        busyLock.writeLock().lock();

        try {
            Map<UUID, GridClientNodeImpl> updated = new HashMap<UUID, GridClientNodeImpl>();

            for (GridClientNodeImpl node : nodeList) {
                updated.put(node.nodeId(), clearAttributes(node));

                // Generate add events.
                if (!nodes.containsKey(node.nodeId()))
                    evts.add(new TopologyEvent(true, node));
            }

            for (Map.Entry<UUID, GridClientNodeImpl> e : nodes.entrySet()) {
                if (!updated.containsKey(e.getKey()))
                    evts.add(new TopologyEvent(false, e.getValue()));
            }

            nodes = updated;

            notifyEvents(evts);

            return nodeList;
        }
        finally {
            busyLock.writeLock().unlock();
        }
    }

    /**
     * Updates topology when node that is expected to be in topology fails.
     *
     * @param nodeId Node id for which node failed to be obtained.
     */
    void nodeFailed(UUID nodeId) {
        busyLock.writeLock().lock();

        try {
            boolean nodeDeleted = nodes.containsKey(nodeId);

            GridClientNode deleted = null;

            // We update the whole topology if node was not in topology or we cache metrics.
            if (nodeDeleted) {
                Map<UUID, GridClientNodeImpl> updatedTop = new HashMap<UUID, GridClientNodeImpl>(nodes);

                deleted = updatedTop.remove(nodeId);

                // Change the reference to new topology.
                // So everyone who captured old version will see a consistent snapshot.
                nodes = updatedTop;
            }

            if (nodeDeleted)
                notifyEvents(Collections.singletonList(new TopologyEvent(false, deleted)));
        }
        finally {
            busyLock.writeLock().unlock();
        }
    }

    /**
     * Gets node from last saved topology snapshot by it's id.
     *
     * @param id Node id.
     * @return Node or {@code null} if node was not found.
     */
    GridClientNode node(UUID id) {
        busyLock.readLock().lock();

        try {
            return nodes.get(id);
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Gets a collection of nodes from last saved topology snapshot by their ids.
     *
     * @param ids Collection of ids for which nodes should be retrieved..
     * @return Collection of nodes that are in topology.
     */
    Collection<GridClientNodeImpl> nodes(Iterable<UUID> ids) {
        Collection<GridClientNodeImpl> res = new LinkedList<GridClientNodeImpl >();

        busyLock.readLock().lock();

        try {
            for (UUID id : ids) {
                GridClientNodeImpl node = nodes.get(id);

                if (node != null)
                    res.add(node);
            }

            return res;
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Gets full topology snapshot.
     *
     * @return Collection of nodes that were in last captured topology snapshot.
     */
    Collection<GridClientNodeImpl> nodes() {
        busyLock.readLock().lock();

        try {
            return Collections.unmodifiableCollection(nodes.values());
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Shutdowns executor service that performs listener notification.
     */
    void shutdown() {
        executor.shutdown();
    }

    /**
     * Clears attributes and metrics map in case if node cache is disabled.
     *
     * @param node Node to be cleared.
     * @return The same node if cache is enabled or node contains no attributes and metrics,
     *      otherwise will return new node without attributes and metrics.
     */
    private GridClientNodeImpl clearAttributes(GridClientNodeImpl node) {
        if (topCache || (node.attributes().isEmpty() && node.metrics().isEmpty()))
            return node;
        else {
            // Fill all fields but attributes and metrics since we do not cache them.
            GridClientNodeImpl updated = new GridClientNodeImpl();

            updated.nodeId(node.nodeId());
            updated.internalAddresses(node.internalAddresses());
            updated.externalAddresses(node.externalAddresses());
            updated.tcpPort(node.tcpPort());
            updated.httpPort(node.httpPort());
            updated.caches(node.caches());

            return updated;
        }
    }

    /**
     * Runs listener notification is separate thread.
     *
     * @param evts Event list.
     */
    private void notifyEvents(final Iterable<TopologyEvent> evts) {
        try {
            executor.execute(new Runnable() {
                @Override public void run() {
                    for (TopologyEvent evt : evts) {
                        if (evt.added()) {
                            for (GridClientTopologyListener lsnr : topLsnrs)
                                lsnr.onNodeAdded(evt.node());
                        }
                        else {
                            for (GridClientTopologyListener lsnr : topLsnrs)
                                lsnr.onNodeRemoved(evt.node());
                        }
                    }
                }
            });
        }
        catch (RejectedExecutionException e) {
            log.warning("Unable to notify event listeners on topology change since client is shutting down: " +
                e.getMessage());
        }
    }

    /**
     * Event for node adding and removal.
     */
    private static class TopologyEvent {
        /** Added or removed flag */
        private boolean added;

        /** Node that triggered event. */
        private GridClientNode node;

        /**
         * Creates a new event.
         *
         * @param added If {@code true}, indicates that node was added to topology.
         *      If {@code false}, indicates that node was removed.
         * @param node Added or removed node.
         */
        private TopologyEvent(boolean added, GridClientNode node) {
            this.added = added;
            this.node = node;
        }

        /**
         * @return Flag indicating whether node was added or removed.
         */
        private boolean added() {
            return added;
        }

        /**
         * @return Node that triggered event.
         */
        private GridClientNode node() {
            return node;
        }
    }
}
