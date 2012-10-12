// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.security;

import org.gridgain.client.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.typedef.*;

import javax.net.ssl.*;
import java.util.*;

/**
 * This example demonstrates use of Java client authentication feature.
 * You should start an instance of {@link GridAuthenticationNodeStartup} class which
 * will start up a GridGain node with proper configuration.
 * <p>
 * After node has been started this example creates a client and performs topology request.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridAuthenticationAndSecureSessionClientExample {
    /** Change this property to start example in SSL mode. */
    private static final boolean USE_SSL = GridAuthenticationNodeStartup.USE_SSL;

    static {
        // Disable 'localhost' verification - for testing with self-signed certificates.
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String hostname, SSLSession sslSes) {
                return "localhost".equals(hostname);
            }
        });
    }

    /**
     * Starts up an empty node with specified configuration, then runs client with security credentials supplied.
     * Depending on {@link #USE_SSL} flag value either passcode authentication spi will be used or SSL will be enabled.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try {
            GridClient client = createClient("s3cret");

            X.println(">>> Client successfully authenticated.");

            // Client is authenticated. You can add you code here, we will just show grid topology.
            X.println(">>> Current grid topology: " + client.compute().refreshTopology(true, true));

            // Command succeeded, session between client and grid node has been established.
            if (USE_SSL)
                X.println(">>> Secure session between client and grid has been established.");
            else
                X.println(">>> Session between client and grid has been established.");

            //...
            //...
            //...
        }
        catch (GridClientAuthenticationException e) {
            X.println(">>> Failed to create client (was the passcode correct?): " + e.getMessage());
        }
        catch (GridClientException e) {
            X.println(">>> Failed to create client (did you start grid nodes?): " + e.getMessage());
        }
        finally {
            GridClientFactory.stopAll(true);
        }
    }

    /**
     * This method will create a client with default configuration. Note that this method expects that
     * first node will bind rest binary protocol on default port. It also expects that partitioned cache is
     * configured in grid.
     *
     * @param passcode Passcode
     * @return Client instance.
     * @throws GridClientException If client could not be created.
     */
    private static GridClient createClient(String passcode) throws GridClientException {
        String ggHome = X.getSystemOrEnv("GRIDGAIN_HOME");

        if (ggHome == null)
            throw new GridClientException("GRIDGAIN_HOME must be set to GridGain installation root.");

        GridClientConfigurationAdapter cc = new GridClientConfigurationAdapter();

        GridClientDataConfigurationAdapter partitioned = new GridClientDataConfigurationAdapter();

        // Set remote cache name.
        partitioned.setName("partitioned");

        // Set client partitioned affinity for this cache.
        partitioned.setAffinity(new GridClientPartitionedAffinity());

        cc.setDataConfigurations(Collections.singletonList(partitioned));

        // Point client to a local node with TCP protocol (default):
        cc.setServers(Collections.singletonList("localhost:11211"));

        // or with HTTP protocol:
        //cc.setProtocol(GridClientProtocol.HTTP);
        //cc.setServers(Collections.singletonList("localhost:8080"));

        // Set passcode credentials.
        cc.setCredentials(passcode);

        // If we use ssl, set appropriate key- and trust-store.
        if (USE_SSL) {
            GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

            factory.setKeyStoreFilePath(ggHome + "/examples/keystore/client.jks");
            factory.setKeyStorePassword("123456".toCharArray());

            factory.setTrustStoreFilePath(ggHome + "/examples/keystore/trust.jks");
            factory.setTrustStorePassword("123456".toCharArray());

            cc.setSslContextFactory(factory);
        }

        return GridClientFactory.start(cc);
    }
}
