// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client.router;

import org.gridgain.client.*;
import org.gridgain.client.router.*;
import org.gridgain.examples.client.api.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

/**
 * This example demonstrates use of Java client, connected to Grid through router.
 * To execute this example you should start an instance of
 * {@link GridClientExampleNodeStartup} class which will start up a GridGain node.
 * And an instance of {@link GridExampleRouterStartup} which will start up
 * a GridGain router.
 * <p>
 * Alternatively you can run node and router instances from command line.
 * To do so you need to execute commands
 * {@code GRIDGAIN_HOME/bin/ggstart.sh examples/config/spring-cache-client.xml}
 * and {@code GRIDGAIN_HOME/bin/ggrouter.sh config/default-router.xml}
 * For more details on how to configure standalone router instances please refer to
 * configuration file {@code GRIDGAIN_HOME/config/default-router.xml}.
 * <p>
 * This example creates client, configured to work with router and performs
 * few cache operations to show router work.
 * <p>
 * Note that different nodes and routers cannot share the same port for rest services.
 * If you want to start more than one node and/or router on the same physical machine
 * you must provide different configurations for each node and router.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridRouterExample {
    /** Grid node address to connect to. */
    private static final String ROUTER_ADDRESS = "127.0.0.1";

    /**
     * Runs router example using both TCP and HTTP protocols.
     *
     * @param args Command line arguments, none required.
     * @throws GridClientException If failed.
     */
    public static void main(String[] args) throws GridClientException {
        GridClient tcpClient = createTcpClient();

        X.println(">>> TCP client created, current grid topology: " + tcpClient.compute().nodes());

        try {
            runExample(tcpClient);
        }
        finally {
            GridClientFactory.stop(tcpClient.id());
        }

        GridClient httpClient = createHttpClient();

        X.println(">>> HTTP client created, current grid topology: " + httpClient.compute().nodes());

        try {
            runExample(httpClient);
        }
        finally {
            GridClientFactory.stop(httpClient.id());
        }
    }

    /**
     * Performs few cache operations on the given client.
     *
     * @param client Client to run example.
     * @throws GridClientException If failed.
     */
    private static void runExample(GridClient client) throws GridClientException {
        GridClientData rmtCache = client.data("partitioned");

        X.println(">>> Storing key 'key' in cache.");

        rmtCache.put("key", "value");

        X.println(">>> Value of key 'key' is: " + rmtCache.get("key"));

        X.println(">>> Removing key 'key' from cache.");

        rmtCache.remove("key");

        X.println(">>> Value of key 'key' is: " + rmtCache.get("key"));
    }

    /**
     * This method will create a client configured to work with locally started router
     * on TCP REST protocol.
     *
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createTcpClient() throws GridClientException {
        GridClientConfigurationAdapter cfg = new GridClientConfigurationAdapter();

        cfg.setDataConfigurations(Collections.singletonList(cacheConfiguration()));

        // Point client to a local TCP router.
        cfg.setRouters(Collections.singletonList(ROUTER_ADDRESS + ':' + GridTcpRouterConfiguration.DFLT_TCP_PORT));

        return GridClientFactory.start(cfg);
    }

    /**
     * This method will create a client configured to work with locally started router
     * on HTTP REST protocol.
     *
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createHttpClient() throws GridClientException {
        GridClientConfigurationAdapter cfg = new GridClientConfigurationAdapter();

        cfg.setDataConfigurations(Collections.singletonList(cacheConfiguration()));

        cfg.setProtocol(GridClientProtocol.HTTP);

        // Point client to a local HTTP router.
        // Using port number from config/router-jetty.xml.
        cfg.setRouters(Collections.singletonList(ROUTER_ADDRESS + ":8081"));

        return GridClientFactory.start(cfg);
    }

    /**
     * Creates client cache configuration for partitioned cache.
     *
     * @return Client cache configuration.
     */
    private static GridClientDataConfigurationAdapter cacheConfiguration() {
        GridClientDataConfigurationAdapter cacheCfg = new GridClientDataConfigurationAdapter();

        // Set remote cache name.
        cacheCfg.setName("partitioned");

        // Set client partitioned affinity for this cache.
        cacheCfg.setAffinity(new GridClientPartitionedAffinity());
        return cacheCfg;
    }
}
