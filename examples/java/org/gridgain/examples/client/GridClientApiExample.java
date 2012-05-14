// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client;

import org.gridgain.client.*;
import org.gridgain.client.balancer.*;
import org.gridgain.grid.*;
import org.gridgain.grid.typedef.*;

import java.net.*;
import java.util.*;

/**
 * This example demonstrates use of Java remote client API. To execute
 * this example you should start an instance of {@link GridClientExampleNodeStartup}
 * class which will start up a GridGain node with proper configuration.
 * <p>
 * After node has been started this example creates a client and performs several cache
 * puts and executes a test task.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridClientApiExample {
    /** Grid node address to connect to. */
    private static final String SERVER_ADDRESS = "127.0.0.1";

    /** Count of keys to be stored in this example. */
    public static final int KEYS_CNT = 10;

    /**
     * Starts up an empty node with specified cache configuration, then runs client cache example and client
     * compute example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws Exception {
        clientCacheExample();

        clientComputeExample();
    }

    /**
     * This method will create a client with default configuration. Note that this method expects that
     * first node will bind rest binary protocol on default port. It also expects that partitioned cache is
     * configured in grid.
     *
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createClient() throws GridClientException {
        GridClientConfigurationAdapter cfg = new GridClientConfigurationAdapter();

        GridClientDataConfigurationAdapter cacheCfg = new GridClientDataConfigurationAdapter();

        // Set remote cache name.
        cacheCfg.setName("partitioned");

        // Set client partitioned affinity for this cache.
        cacheCfg.setAffinity(new GridClientPartitionedAffinity());

        cfg.setDataConfigurations(Collections.singletonList(cacheCfg));

        // Point client to a local node. Note that this server is only used
        // for initial connection. After having established initial connection
        // client will make decisions which grid node to use based on collocation
        // with key affinity or load balancing.
        cfg.setServers(Collections.singletonList(SERVER_ADDRESS + ':' + GridConfigurationAdapter.DFLT_TCP_PORT));

        return GridClientFactory.start(cfg);
    }

    /**
     * Shows simple cache usage via GridGain client.
     *
     * @throws GridClientException If client encountered exception.
     */
    private static void clientCacheExample() throws GridClientException {
        GridClient client = createClient();

        try {
            // Show grid topology.
            X.println(">>> Client created, current grid topology: " + client.compute().nodes());

            // Random node ID.
            final UUID randNodeId = client.compute().nodes().iterator().next().nodeId();

            // Get client projection of grid partitioned cache.
            GridClientData rmtCache = client.data("partitioned");

            Collection<String> keys = new ArrayList<String>(KEYS_CNT);

            // Put some values to the cache.
            for (int i = 0; i < KEYS_CNT; i++) {
                String key = String.valueOf(i);

                // Put request will go exactly to the primary node for this key.
                rmtCache.put(key, "val-" + i);

                UUID nodeId = rmtCache.affinity(key);

                X.println(">>> Storing key " + key + " on node " + nodeId);

                keys.add(key);
            }

            // Pin a remote node for communication. All further communication
            // on returned projection will happen through this pinned node.
            GridClientData prj = rmtCache.pinNodes(client.compute().node(randNodeId));

            // Request batch from our local node in pinned mode.
            Map<String, Object> vals = prj.getAll(keys);

            for (Map.Entry<String, Object> entry : vals.entrySet())
                X.println(">>> Loaded cache entry [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

            // After nodes are pinned the list of pinned nodes may be retrieved.
            X.println(">>> Pinned nodes: " + prj.pinnedNodes());

            // Keys may be stored asynchronously.
            GridClientFuture<Boolean> futPut = rmtCache.putAsync(String.valueOf(0), "new value for 0");

            X.println(">>> Result of asynchronous put: " + (futPut.get() ? "success" : "failure"));

            Map<UUID, Map<String, String>> keyVals = new HashMap<UUID, Map<String, String>>();

            // Batch puts are also supported.
            // Here we group key-value pairs by their affinity node ID to ensure
            // the least amount of network trips possible.
            for (int i = 0; i < KEYS_CNT; i++) {
                String key = String.valueOf(i);

                UUID nodeId = rmtCache.affinity(key);

                Map<String, String> m = keyVals.get(nodeId);

                if (m == null)
                    keyVals.put(nodeId, m = new HashMap<String, String>());

                m.put(key, "val-" + i);
            }

            for (Map<String, String> kvMap : keyVals.values())
                // Affinity-aware bulk put operation - it will connect to the
                // affinity node for provided keys.
                rmtCache.putAll(kvMap);

            // Asynchronous batch put is available as well.
            Collection<GridClientFuture<?>> futs = new LinkedList<GridClientFuture<?>>();

            for (Map<String, String> kvMap : keyVals.values()) {
                GridClientFuture<?> futPutAll = rmtCache.putAllAsync(kvMap);

                futs.add(futPutAll);
            }

            // Wait for all futures to complete.
            for (GridClientFuture<?> fut : futs)
                fut.get();

            // Of course there's getting value by key functionality.
            String key = String.valueOf(0);

            X.println(">>> Value for key " + key + " is " + rmtCache.get(key));

            // Asynchronous gets, too.
            GridClientFuture<String> futVal = rmtCache.getAsync(key);

            X.println(">>> Asynchronous value for key " + key + " is " + futVal.get());

            // Multiple values can be fetched at once. Here we batch our get
            // requests by affinity nodes to ensure least amount of network trips.
            for (Map.Entry<UUID, Map<String, String>> nodeEntry : keyVals.entrySet()) {
                UUID nodeId = nodeEntry.getKey();
                Collection<String> keyCol = nodeEntry.getValue().keySet();

                // Since all keys in our getAll(...) call are mapped to the same primary node,
                // grid cache client will pick this node for the request, so we only have one
                // network trip here.
                X.println(">>> Values from node [nodeId=" + nodeId + ", values=" + rmtCache.getAll(keyCol) + ']');
            }

            // Multiple values may be retrieved asynchronously, too.
            // Here we retrieve all keys at ones. Since this request
            // will be sent to some grid node, this node may not be
            // the primary node for all keys and additional network
            // trips will have to be made within grid.
            GridClientFuture futVals = rmtCache.getAllAsync(keys);

            X.println(">>> Asynchronous values for keys are " + futVals.get());

            // Contents of cache may be removed one by one synchronously.
            // Again, this operation is affinity aware and only the primary
            // node for the key is contacted.
            boolean res = rmtCache.remove(String.valueOf(0));

            X.println(">>> Result of removal: " + (res ? "success" : "failure"));

            // ... and asynchronously.
            GridClientFuture<Boolean> futRes = rmtCache.removeAsync(String.valueOf(1));

            X.println(">>> Result of asynchronous removal is: " + (futRes.get() ? "success" : "failure"));

            // Multiple entries may be removed at once synchronously...
            rmtCache.removeAll(Arrays.asList(String.valueOf(2), String.valueOf(3)));

            // ... and asynchronously.
            GridClientFuture<?> futResAll = rmtCache.removeAllAsync(Arrays.asList(String.valueOf(3), String.valueOf(4)));

            futResAll.get();

            // Values may also be replaced.
            res = rmtCache.replace(String.valueOf(0), "new value for 0");

            X.println(">>> Result for replace for nonexistent key is " + (res ? "success" : "failure"));

            // Asynchronous replace is supported, too.
            futRes = rmtCache.replaceAsync(String.valueOf(0), "newest value for 0");

            X.println(">>> Result for asynchronous replace for nonexistent key is " +
                (futRes.get() ? "success" : "failure"));

            // Compare and set are implemented, too.
            res = rmtCache.cas(String.valueOf(0), "new value for 0", null);

            X.println(">>> Result for put using cas for key that didn't have value yet is " +
                (res ? "success" : "failure"));

            // CAS can be asynchronous.
            futRes = rmtCache.casAsync(String.valueOf(0), "newest value for 0", "new value for 0");

            X.println(">>> Result for put using asynchronous cas is " + (futRes.get() ? "success" : "failure"));

            // It's possible to obtain cache metrics using data client API.
            X.println(">>> Cache metrics : " + rmtCache.metrics());

            // Cache metrics may be retrieved for individual keys.
            X.println(">>> Cache metrics for a key : " + rmtCache.metrics(String.valueOf(0)));

            // Global and per key metrics retrieval can be asynchronous, too.
            GridClientFuture futMetrics = rmtCache.metricsAsync();

            X.println(">>> Cache asynchronous metrics : " + futMetrics.get());

            futMetrics = rmtCache.metricsAsync(String.valueOf(0));

            X.println(">>> Cache asynchronous metrics for a key : " + futMetrics.get());
        }
        finally {
            GridClientFactory.stopAll();
        }
    }

    /**
     * Selects a particular node to run example task on, executes it and prints out the task result.
     *
     * @throws GridClientException If client encountered exception.
     */
    private static void clientComputeExample() throws GridClientException {
        GridClient client = createClient();

        try {
            // Show grid topology.
            X.println(">>> Client created, current grid topology: " + client.compute().nodes());

            // Random node ID.
            final UUID randNodeId = client.compute().nodes().iterator().next().nodeId();

            // Note that in this example we get a fixed projection for task call because we cannot guarantee that
            // other nodes contain ClientExampleTask in classpath.
            GridClientCompute prj = client.compute().projection(new GridClientPredicate<GridClientNode>() {
                @Override public boolean apply(GridClientNode node) {
                    return node.nodeId().equals(randNodeId);
                }
            });

            // Execute test task that will count total count of cache entries in grid.
            Integer entryCnt = prj.execute(GridClientExampleTask.class.getName(), null);

            X.println(">>> Predicate projection : there are totally " + entryCnt + " test entries on the grid");

            // Same as above, using different projection API.
            GridClientNode clntNode = prj.node(randNodeId);

            prj = prj.projection(clntNode);

            entryCnt = prj.execute(GridClientExampleTask.class.getName(), null);

            X.println(">>> GridClientNode projection : there are totally " + entryCnt + " test entries on the grid");

            // Use of collections is also possible.
            prj = prj.projection(Collections.singleton(clntNode));

            entryCnt = prj.execute(GridClientExampleTask.class.getName(), null);

            X.println(">>> Collection projection : there are totally " + entryCnt + " test entries on the grid");

            // Balancing - may be random or round-robin. Users can create
            // custom load balancers as well.
            GridClientLoadBalancer balancer = new GridClientRandomBalancer();

            // Balancer may be added to predicate or collection examples.
            prj = client.compute().projection(new GridClientPredicate<GridClientNode>() {
                @Override public boolean apply(GridClientNode node) {
                    return node.nodeId().equals(randNodeId);
                }
            }, balancer);

            entryCnt = prj.execute(GridClientExampleTask.class.getName(), null);

            X.println(">>> Predicate projection with balancer : there are totally " + entryCnt +
                " test entries on the grid");

            // Now let's try round-robin load balancer.
            balancer = new GridClientRoundRobinBalancer();

            prj = prj.projection(Collections.singleton(clntNode), balancer);

            entryCnt = prj.execute(GridClientExampleTask.class.getName(), null);

            X.println(">>> GridClientNode projection : there are totally " + entryCnt + " test entries on the grid");

            // Execution may be asynchronous.
            GridClientFuture<Integer> fut = prj.executeAsync(GridClientExampleTask.class.getName(), null);

            X.println(">>> Execute async : there are totally " + fut.get() + " test entries on the grid");

            // Execution may use affinity.
            GridClientData rmtCache = client.data("partitioned");

            String key = String.valueOf(0);

            rmtCache.put(key, "new value for 0");

            entryCnt = prj.affinityExecute(GridClientExampleTask.class.getName(), "partitioned", key, null);

            X.println(">>> Affinity execute : there are totally " + entryCnt + " test entries on the grid");

            // Affinity execution may be asynchronous, too.
            fut = prj.affinityExecuteAsync(GridClientExampleTask.class.getName(), "partitioned", key, null);

            X.println(">>> Affinity execute async : there are totally " + fut.get() + " test entries on the grid");

            // GridClientCompute can be queried for nodes participating in it.
            Collection c = prj.nodes(Collections.singleton(randNodeId));

            X.println(">>> Nodes with UUID " + randNodeId + " : " + c);

            // Nodes may also be filtered with predicate. Here
            // we create projection which only contains local node.
            c = prj.nodes(new GridClientPredicate<GridClientNode>() {
                @Override public boolean apply(GridClientNode node) {
                    return node.nodeId().equals(randNodeId);
                }
            });

            X.println(">>> Nodes filtered with predicate : " + c);

            // Information about nodes may be refreshed explicitly.
            clntNode = prj.refreshNode(randNodeId, true, true);

            X.println(">>> Refreshed node : " + clntNode);

            // As usual, there's also an asynchronous version.
            GridClientFuture<GridClientNode> futClntNode = prj.refreshNodeAsync(randNodeId, false, false);

            X.println(">>> Refreshed node asynchronously : " + futClntNode.get());

            // Nodes may also be refreshed by IP address.
            String clntAddr = "127.0.0.1";

            for (InetSocketAddress addr : clntNode.availableAddresses(GridClientProtocol.TCP))
                if (addr != null)
                    clntAddr = addr.getAddress().getHostAddress();

            // Force node metrics refresh (by default it happens periodically in the background).
            clntNode = prj.refreshNode(clntAddr, true, true);

            X.println(">>> Refreshed node by IP : " + clntNode.toString());

            // Asynchronous version.
            futClntNode = prj.refreshNodeAsync(clntAddr, false, false);

            X.println(">>> Refreshed node by IP asynchronously : " + futClntNode.get());

            // Topology as a whole may be refreshed, too.
            Collection<GridClientNode> top = prj.refreshTopology(true, true);

            X.println(">>> Refreshed topology : " + top);

            // Asynchronous version.
            GridClientFuture<List<GridClientNode>> topFut = prj.refreshTopologyAsync(false, false);

            X.println(">>> Refreshed topology asynchronously : " + topFut.get());

            try {
                // Client can be used to query logs.
                Collection<String> log = prj.log(0, 1);

                X.println(">>> First log lines : " + log);

                // Log entries may be fetched asynchronously.
                GridClientFuture<List<String>> futLog = prj.logAsync(1, 2);

                X.println(">>> First log lines fetched asynchronously : " + futLog.get());

                // Log file name can also be specified explicitly.
                log = prj.log("work/log/gridgain.log", 0, 1);

                X.println(">>> First log lines from log file work/log/gridgain.log : " + log);

                // Asynchronous version supported as well.
                futLog = prj.logAsync("work/log/gridgain.log", 1, 2);

                X.println(">>> First log lines fetched asynchronously : " + futLog.get());
            }
            catch (GridClientException e) {
                X.println("Log file was not found: " + e);
            }
        }
        finally {
            GridClientFactory.stopAll(true);
        }
    }
}
