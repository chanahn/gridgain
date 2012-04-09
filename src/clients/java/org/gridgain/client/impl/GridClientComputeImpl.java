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
 * Compute projection implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
@SuppressWarnings("NullableProblems")
public class GridClientComputeImpl extends GridClientAbstractProjection<GridClientComputeImpl>
    implements GridClientCompute {
    /**
     * Creates a new compute projection.
     *
     * @param client Started client.
     * @param nodes Nodes to be included in this projection.
     * @param nodeFilter Node filter to be used for this projection.
     * @param balancer Balancer to be used in this projection.
     */
    public GridClientComputeImpl(GridClientImpl client, Collection<GridClientNode> nodes,
        GridClientPredicate<GridClientNode> nodeFilter, GridClientLoadBalancer balancer) {
        super(client, nodes, nodeFilter, balancer);
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute projection(GridClientNode node) throws GridClientException {
        return createProjection(Collections.singletonList(node), null, null);
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute projection(GridClientPredicate<GridClientNode> filter)
        throws GridClientException {
        return createProjection(null, filter, null);
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute projection(Collection<GridClientNode> nodes) throws GridClientException {
        return createProjection(nodes, null, null);
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute projection(GridClientPredicate<GridClientNode> filter,
        GridClientLoadBalancer balancer) throws GridClientException {
        return createProjection(null, filter, balancer);
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute projection(Collection<GridClientNode> nodes, GridClientLoadBalancer balancer)
        throws GridClientException {
        return createProjection(nodes, null, balancer);
    }

    /** {@inheritDoc} */
    @Override public GridClientLoadBalancer balancer() {
        return balancer;
    }

    /** {@inheritDoc} */
    @Override public <R> R execute(String taskName, Object taskArg) throws GridClientException {
        return this.<R>executeAsync(taskName, taskArg).get();
    }

    /** {@inheritDoc} */
    @Override public <R> GridClientFuture<R> executeAsync(final String taskName, final Object taskArg)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<R>() {
            @Override public GridClientFuture<R> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.execute(taskName, taskArg);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityExecute(String taskName, String cacheName, Object affKey, Object taskArg)
        throws GridClientException {
        return this.<R>affinityExecuteAsync(taskName, cacheName, affKey, taskArg).get();
    }

    /** {@inheritDoc} */
    @Override public <R> GridClientFuture<R> affinityExecuteAsync(final String taskName, String cacheName, Object affKey,
        final Object taskArg) throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<R>() {
            @Override public GridClientFuture<R> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.execute(taskName, taskArg);
            }
        }, cacheName, affKey);
    }

    /** {@inheritDoc} */
    @Override public GridClientNode node(UUID id) {
        return client.topology().node(id);
    }

    /**
     * Gets most recently refreshed topology. If this compute instance is a projection,
     * then only nodes that satisfy projection criteria will be returned.
     *
     * @return Most recently refreshed topology.
     */
    @Override public Collection<? extends GridClientNode> nodes() {
        return projectionNodes();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends GridClientNode> nodes(Collection<UUID> ids) {
        return client.topology().nodes(ids);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(GridClientPredicate<GridClientNode> filter) {
        return GridClientUtils.applyFilter(projectionNodes(), filter);
    }

    /** {@inheritDoc} */
    @Override public GridClientNode refreshNode(UUID id, boolean includeAttrs, boolean includeMetrics)
        throws GridClientException {
        return refreshNodeAsync(id, includeAttrs, includeMetrics).get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientNode> refreshNodeAsync(final UUID id, final boolean includeAttrs,
        final boolean includeMetrics) throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<GridClientNode>() {
            @Override public GridClientFuture<GridClientNode> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.node(id, includeAttrs, includeMetrics);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridClientNode refreshNode(String ip, boolean includeAttrs, boolean includeMetrics)
        throws GridClientException {
        return refreshNodeAsync(ip, includeAttrs, includeMetrics).get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientNode> refreshNodeAsync(final String ip, final boolean includeAttrs,
        final boolean includeMetrics) throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<GridClientNode>() {
            @Override public GridClientFuture<GridClientNode> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.node(ip, includeAttrs, includeMetrics);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public List<GridClientNode> refreshTopology(boolean includeAttrs, boolean includeMetrics)
        throws GridClientException {
        return refreshTopologyAsync(includeAttrs, includeMetrics).get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<GridClientNode>> refreshTopologyAsync(final boolean includeAttrs,
        final boolean includeMetrics) throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<List<GridClientNode>>() {
            @Override public GridClientFuture<List<GridClientNode>> apply(GridClientConnection conn)
                throws GridClientConnectionResetException,
                GridClientClosedException {
                return conn.topology(includeAttrs, false);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public List<String> log(int lineFrom, int lineTo) throws GridClientException {
        return logAsync(lineFrom, lineTo).get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<String>> logAsync(final int lineFrom, final int lineTo)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<List<String>>() {
            @Override public GridClientFuture<List<String>> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.log(null, lineFrom, lineTo);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public List<String> log(String path, int lineFrom, int lineTo) throws GridClientException {
        return logAsync(path, lineFrom, lineTo).get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<List<String>> logAsync(final String path, final int lineFrom, final int lineTo)
        throws GridServerUnreachableException, GridClientClosedException {
        return withReconnectHandling(new ClientProjectionClosure<List<String>>() {
            @Override public GridClientFuture<List<String>> apply(GridClientConnection conn)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.log(path, lineFrom, lineTo);
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected GridClientComputeImpl createProjectionImpl(Collection<GridClientNode> nodes,
        GridClientPredicate<GridClientNode> filter, GridClientLoadBalancer balancer) {
        return new GridClientComputeImpl(client, nodes, filter, balancer);
    }
}
