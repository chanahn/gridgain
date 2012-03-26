// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.client.balancer.*;
import org.gridgain.client.util.*;

import java.util.*;

/**
 * Class contains common connection-error handling logic.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
abstract class GridClientAbstractProjection<T extends GridClientAbstractProjection> {
    /** List of nodes included in this projection. If null, all nodes in topology are included. */
    protected Collection<GridClientNode> nodes;

    /** Node filter to be applied for this projection. */
    protected GridClientPredicate<GridClientNode> filter;

    /** Balancer to be used in this projection. */
    protected GridClientLoadBalancer balancer;

    /** Count of reconnect retries before exception is thrown. */
    private static final int RETRY_CNT = 3;

    /** Client instance. */
    protected GridClientImpl client;

    /**
     * Creates projection with specified client.
     *
     * @param client Client instance to use.
     * @param nodes Collections of nodes included in this projection.
     * @param filter Node filter to be applied.
     * @param balancer Balancer to use.
     */
    protected GridClientAbstractProjection(GridClientImpl client, Collection<GridClientNode> nodes,
        GridClientPredicate<GridClientNode> filter, GridClientLoadBalancer balancer) {
        assert client != null;

        this.client = client;
        this.nodes = nodes;
        this.filter = filter;
        this.balancer = balancer;
    }

    /**
     * This method executes request to a communication layer and handles connection error, if it occurs.
     * In case of communication exception client instance is notified and new instance of client is created.
     * If none of the grid servers can be reached, an exception is thrown.
     *
     * @param c Closure to be executed.
     * @param <R> Result future type.
     * @return Future returned by closure.
     * @throws GridServerUnreachableException If either none of the server can be reached or each attempt
     *      of communication within {@link #RETRY_CNT} attempts resulted in {@link GridClientConnectionResetException}.
     * @throws GridClientClosedException If client was closed manually.
     */
    protected <R> GridClientFuture<R> withReconnectHandling(ClientProjectionClosure<R> c)
        throws GridServerUnreachableException, GridClientClosedException {
        GridClientNode node = null;

        boolean changeNode = false;

        Throwable cause = null;

        for (int i = 0; i < RETRY_CNT; i++) {

            if (node == null || changeNode)
                node = balancedNode();

            GridClientConnection conn = null;

            try {
                conn = client.connection(node);

                return c.apply(conn);
            }
            catch (GridConnectionIdleClosedException e) {
                client.onFacadeFailed(node, conn, e);

                // It's ok, just reconnect to the same node.
                changeNode = false;

                cause = e;
            }
            catch (GridClientConnectionResetException e) {
                client.onFacadeFailed(node, conn, e);

                changeNode = true;

                cause = e;
            }
        }

        throw new GridServerUnreachableException("Failed to communicate with grid nodes " +
            "(maximum count of retries reached).", cause);
    }

    /**
     * This method executes request to a communication layer and handles connection error, if it occurs. Server
     * is picked up according to the projection affinity and key given. Connection will be made with the node
     * on which key is cached. In case of communication exception client instance is notified and new instance
     * of client is created. If none of servers can be reached, an exception is thrown.
     *
     * @param c Closure to be executed.
     * @param cacheName Cache name for which mapped node will be calculated.
     * @param affKey Affinity key.
     * @param <R> Type of result in future.
     * @return Operation future.
     * @throws GridServerUnreachableException If none of the nodes can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    protected <R> GridClientFuture<R> withReconnectHandling(ClientProjectionClosure<R> c, String cacheName,
        Object affKey) throws GridServerUnreachableException, GridClientClosedException {
        GridClientDataAffinity affinity = client.affinity(cacheName);

        // If pinned (fixed-nodes) or no affinity provided use balancer.
        if (nodes != null || affinity == null)
            return withReconnectHandling(c);
        else {
            Collection<? extends GridClientNode> prjNodes = projectionNodes();

            if (prjNodes.isEmpty())
                throw new GridServerUnreachableException("Failed to get affinity node (no nodes in topology were " +
                    "accepted by the filter): " + filter);

            GridClientNode node = affinity.node(affKey, prjNodes);

            for (int i = 0; i < RETRY_CNT; i++) {
                GridClientConnection conn = null;

                try {
                    conn = client.connection(node);

                    return c.apply(conn);
                }
                catch (GridConnectionIdleClosedException e) {
                    client.onFacadeFailed(node, conn, e);
                }
                catch (GridClientConnectionResetException e) {
                    client.onFacadeFailed(node, conn, e);

                    if (!checkNodeAlive(node.nodeId()))
                        throw new GridServerUnreachableException("Failed to communicate with mapped grid node for " +
                            "given affinity key (node left the grid) [nodeId=" + node.nodeId() + ", affKey=" + affKey +
                            ']');
                }
            }

            throw new GridServerUnreachableException("Failed to communicate with mapped grid node for given affinity " +
                "key (did node left the grid?) [nodeId=" + node.nodeId() + ", affKey=" + affKey + ']');
        }
    }

    /**
     * Tries to refresh node on every possible connection in topology.
     *
     * @param nodeId Node id to check.
     * @return {@code True} if response was received, {@code false} if either {@code null} response received or
     *      no nodes can be contacted at all.
     * @throws GridClientClosedException If client was closed manually.
     */
    protected boolean checkNodeAlive(UUID nodeId) throws GridClientClosedException {
        // Try to get node information on any of the connections possible.
        for (GridClientNodeImpl node : client.topology().nodes()) {
            try {
                // Do not try to connect to the same node.
                if (node.nodeId().equals(nodeId))
                    continue;

                GridClientConnection conn = client.connection(node);

                try {
                    GridClientNode target = conn.node(nodeId, false, false).get();

                    if (target == null)
                        client.topology().nodeFailed(nodeId);

                    return target != null;
                }
                catch (GridClientConnectionResetException e) {
                    client.onFacadeFailed(node, conn, e);
                }
                catch (GridClientClosedException e) {
                    throw e;
                }
                catch (GridClientException ignored) {
                    // Future failed, so connection exception have already been handled.
                }
            }
            catch (GridServerUnreachableException ignored) {
                // Try next node.
            }
        }

        return false;
    }

    /**
     * Gets most recently refreshed topology. If this compute instance is a projection,
     * then only nodes that satisfy projection criteria will be returned.
     *
     * @return Most recently refreshed topology.
     */
    protected Collection<? extends GridClientNode> projectionNodes() {
        Collection<? extends GridClientNode> prjNodes;

        if (nodes == null) {
            // Dynamic projection, ask topology for current snapshot.
            prjNodes = client.topology().nodes();

            if (filter != null)
                prjNodes = GridClientUtils.applyFilter(prjNodes, filter);
        }
        else
            prjNodes = nodes;

        return prjNodes;
    }

    /**
     * Return balanced node for current projection.
     *
     * @return Balanced node.
     * @throws GridServerUnreachableException If topology is empty.
     */
    private GridClientNode balancedNode() throws GridServerUnreachableException {
        Collection<? extends GridClientNode> prjNodes = projectionNodes();

        if (prjNodes.isEmpty()) {
            throw new GridServerUnreachableException("Failed to get balanced node (no nodes in topology were " +
                "accepted by the filter): " + filter);
        }

        if (prjNodes.size() == 1)
            return GridClientUtils.first(prjNodes);

        return balancer.balancedNode(prjNodes);
    }

    /**
     * Creates a sub-projection for current projection.
     *
     * @param nodes Collection of nodes that sub-projection will be restricted to. If {@code null},
     *      created projection is dynamic and will take nodes from topology.
     * @param filter Filter to be applied to nodes in projection.
     * @param balancer Balancer to use in projection.
     * @return Created projection.
     * @throws GridClientException If resulting projection is empty. Note that this exception may
     *      only be thrown on case of static projections, i.e. where collection of nodes is not null.
     */
    protected T createProjection(Collection<GridClientNode> nodes, GridClientPredicate<GridClientNode> filter,
        GridClientLoadBalancer balancer) throws GridClientException {
        if (nodes != null && nodes.isEmpty())
            throw new GridClientException("Failed to create projection: given nodes collection is empty.");

        if (filter != null && this.filter != null)
            filter = new GridClientAndPredicate<GridClientNode>(this.filter, filter);
        else if (filter == null)
            filter = this.filter;

        Collection<GridClientNode> subset = intersection(this.nodes, nodes);

        if (subset != null && subset.isEmpty())
            throw new GridClientException("Failed to create projection (given node set does not overlap with " +
                "existing node set) [prjNodes=" + this.nodes + ", nodes=" + nodes);

        if (filter != null && subset != null) {
            subset = GridClientUtils.applyFilter(subset, filter);

            if (subset != null && subset.isEmpty())
                throw new GridClientException("Failed to create projection (none of the nodes in projection node " +
                    "set passed the filter) [prjNodes=" + subset + ", filter=" + filter + ']');
        }

        if (balancer == null)
            balancer = this.balancer;

        return createProjectionImpl(nodes, filter, balancer);
    }

    /**
     * Calculates intersection of two collections. Returned collection always a new collection.
     *
     * @param first First collection to intersect.
     * @param second Second collection to intersect.
     * @return Intersection or {@code null} if both collections are {@code null}.
     */
    private Collection<GridClientNode> intersection(Collection<GridClientNode> first,
        Collection<GridClientNode> second) {
        if (first == null && second == null)
            return null;

        if (first != null && second != null) {
            Collection<GridClientNode> res = new LinkedList<GridClientNode>(first);

            res.retainAll(second);

            return res;
        }
        else
            return new ArrayList<GridClientNode>(first != null ? first : second);
    }

    /**
     * Subclasses must implement this method and return concrete implementation of projection needed.
     *
     * @param nodes Nodes that are included in projection.
     * @param filter Filter to be applied.
     * @param balancer Balancer to be used.
     * @return Created projection.
     */
    protected abstract T createProjectionImpl(Collection<GridClientNode> nodes,
        GridClientPredicate<GridClientNode> filter, GridClientLoadBalancer balancer);

    /**
     * Closure to execute reconnect-handling code.
     */
    protected static interface ClientProjectionClosure<R> {
        /**
         * All closures that implement this interface may safely call all methods of communication connection.
         * If any exceptions in connection occur, they will be automatically handled by projection.
         *
         * @param conn Communication connection that should be accessed.
         * @return Future - result of operation.
         * @throws GridClientConnectionResetException If connection was unexpectedly reset. Connection will be
         *      either re-established or different server will be accessed (if available).
         * @throws GridClientClosedException If client was manually closed by user..
         */
        public GridClientFuture<R> apply(GridClientConnection conn) throws GridClientConnectionResetException,
            GridClientClosedException;
    }
}
