// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.client.balancer.*;

import java.util.*;

/**
 * A compute projection of grid client. Contains various methods for task execution, full and partial (per node)
 * topology refresh and log downloading.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public interface GridClientCompute {
    /**
     * Creates a projection that will communicate only with specified remote node.
     * <p>
     * If current projection is dynamic projection, then this method will check is passed node is in topology.
     * If any filters were specified in current topology, this method will check if passed node is accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed node is in that subset. If any of the checks fails an exception will be thrown.
     *
     * @param node Single node to which this projection will be restricted.
     * @return Resulting static projection that is bound to a given node.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(GridClientNode node) throws GridClientException;

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(GridClientPredicate<GridClientNode> filter) throws GridClientException;

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected with balancer of this projection.
     * <p>
     * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
     * If any filters were specified in current topology, this method will check if passed nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted.
     * @return Resulting static projection that is bound to a given nodes.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(Collection<GridClientNode> nodes) throws GridClientException;

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter. The
     * balancer passed will override default balancer specified in configuration.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @param balancer Balancer that will select balanced node in resulting projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(GridClientPredicate<GridClientNode> filter, GridClientLoadBalancer balancer)
        throws GridClientException;

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected with passed balancer..
     * <p>
     * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
     * If any filters were specified in current topology, this method will check if passed nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted.
     * @param balancer Balancer that will select nodes in resulting projection.
     * @return Resulting static projection that is bound to a given nodes.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(Collection<GridClientNode> nodes, GridClientLoadBalancer balancer)
        throws GridClientException;

    /**
     * Gets balancer used by projection.
     *
     * @return Instance of {@link GridClientLoadBalancer}.
     */
    public GridClientLoadBalancer balancer();

    /**
     * Executes task.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Task execution result.
     * @throws GridClientException In case of error.
     */
    public <R> R execute(String taskName, Object taskArg) throws GridClientException;

    /**
     * Asynchronously executes task.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <R> GridClientFuture<R> executeAsync(String taskName, Object taskArg)
        throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Executes task using cache affinity key for routing. This way the task will start executing
     * exactly on the node where this affinity key is cached.
     *
     * @param taskName Task name or task class name.
     * @param cacheName Name of the cache on which affinity should be calculated.
     * @param affKey Affinity key.
     * @param taskArg Optional task argument.
     * @return Task execution result.
     * @throws GridClientException In case of error.
     */
    public <R> R affinityExecute(String taskName, String cacheName, Object affKey, Object taskArg)
        throws GridClientException;

    /**
     * Asynchronously executes task using cache affinity key for routing. This way
     * the task will start executing exactly on the node where this affinity key is cached.
     *
     * @param taskName Task name or task class name.
     * @param cacheName Name of the cache on which affinity should be calculated.
     * @param affKey Affinity key.
     * @param taskArg Optional task argument.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <R> GridClientFuture<R> affinityExecuteAsync(String taskName, String cacheName, Object affKey,
        Object taskArg) throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Gets most recently refreshed topology. If this compute instance is a projection,
     * then only nodes that satisfy projection criteria will be returned.
     *
     * @return Most recently refreshed topology.
     */
    public Collection<? extends GridClientNode> nodes();

    /**
     * Gets node with given id from most recently refreshed topology.
     *
     * @param id Node ID.
     * @return Node for given ID or {@code null} if node with given id was not found.
     */
     public GridClientNode node(UUID id);

    /**
     * Gets nodes for the given IDs based on most recently refreshed topology.
     *
     * @param ids Node IDs.
     * @return Collection of nodes for provided IDs.
     */
    public Collection<? extends GridClientNode> nodes(Collection<UUID> ids);

    /**
     * Gets nodes that passes the filter. If this compute instance is a projection, then only
     * nodes that passes projection criteria will be passed to the filter.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     */
    public Collection<? extends GridClientNode> nodes(GridClientPredicate<GridClientNode> filter);

    /**
     * Gets node by its ID.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptor or {@code null} if node doesn't exist.
     * @throws GridClientException In case of error.
     */
    public GridClientNode refreshNode(UUID id, boolean includeAttrs, boolean includeMetrics) throws GridClientException;

    /**
     * Asynchronously gets node by its ID.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientFuture<GridClientNode> refreshNodeAsync(UUID id, boolean includeAttrs, boolean includeMetrics)
        throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Gets node by IP address.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptor or {@code null} if node doesn't exist.
     * @throws GridClientException In case of error.
     */
    public GridClientNode refreshNode(String ip, boolean includeAttrs, boolean includeMetrics)
        throws GridClientException;

    /**
     * Asynchronously gets node by IP address.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientFuture<GridClientNode> refreshNodeAsync(String ip, boolean includeAttrs, boolean includeMetrics)
        throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Gets current topology.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptors.
     * @throws GridClientException In case of error.
     */
    public List<GridClientNode> refreshTopology(boolean includeAttrs, boolean includeMetrics)
        throws GridClientException;

    /**
     * Asynchronously gets current topology.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientFuture<List<GridClientNode>> refreshTopologyAsync(boolean includeAttrs, boolean includeMetrics)
        throws GridServerUnreachableException, GridClientClosedException;

    /**
     * Gets contents of default log file ({@code GRIDGAIN_HOME/work/log/gridgain.log}).
     *
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Log contents.
     * @throws GridClientException In case of error.
     */
    public List<String> log(int lineFrom, int lineTo) throws GridClientException;

    /**
     * Asynchronously gets contents of default log file
     * ({@code GRIDGAIN_HOME/work/log/gridgain.log}).
     *
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientFuture<List<String>> logAsync(int lineFrom, int lineTo) throws GridServerUnreachableException,
        GridClientClosedException;

    /**
     * Gets contents of custom log file.
     *
     * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Log contents.
     * @throws GridClientException In case of error.
     */
    public List<String> log(String path, int lineFrom, int lineTo) throws GridClientException;

    /**
     * Asynchronously gets contents of custom log file.
     *
     * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Future.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientFuture<List<String>> logAsync(String path, int lineFrom, int lineTo)
        throws GridServerUnreachableException, GridClientClosedException;
}
