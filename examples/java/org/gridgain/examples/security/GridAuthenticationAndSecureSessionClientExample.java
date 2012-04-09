// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.security;

import org.gridgain.client.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

/**
 * Shows client authentication feature.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public class GridAuthenticationAndSecureSessionClientExample {
    /** Change this property to start example in SSL mode. */
    private static final boolean useSsl = false;

    /**
     * Starts up an empty node with specified configuration, then runs client with security credentials supplied.
     * Depending on {@link #useSsl} flag value either passcode authentication spi will be used or SSL will be enabled.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        String passcode = args.length > 0 ? args[0] : "s3cret";

        try {
            // Start up a grid node.
            G.start(useSsl ? "examples/config/spring-cache-ssl.xml" :
                    "examples/config/spring-cache-authentication-passcode.xml");

            GridClient client = createClient(passcode);

            X.println(">>> Client successfully authenticated.");

            // Client is authenticated. You can add you code here, we will just show grid topology.
            X.println(">>> Current grid topology: " + client.compute().refreshTopology(true, true));

            // Command succeeded, session between client and grid node has been established.
            if(useSsl)
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
            X.println(">>> Failed to create client (did you specify correct keystore?): " + e.getMessage());
        }
        catch (GridException e) {
            if (e.hasCause(ClassNotFoundException.class))
                X.println("Failed to create grid " +
                    "('security' is enterprise feature, are you using community edition?): " + e.getMessage());
            else
                X.println("Failed to create grid: " + e.getMessage());
        }
        finally {
            G.stopAll(false);

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

        if(ggHome == null)
            throw new GridClientException("GRIDGAIN_HOME must be set to GridGain installation root.");

        GridClientConfigurationAdapter cc = new GridClientConfigurationAdapter();

        GridClientDataConfigurationAdapter partitioned = new GridClientDataConfigurationAdapter();

        // Set remote cache name.
        partitioned.setName("partitioned");

        // Set client partitioned affinity for this cache.
        partitioned.setAffinity(new GridClientPartitionedAffinity());

        cc.setDataConfigurations(Collections.singletonList(partitioned));

        // Point client to a local node.
        cc.setServers(Collections.singletonList("localhost:" + GridConfigurationAdapter.DFLT_TCP_PORT));

        // Set passcode credentials.
        cc.setCredentials(passcode);

        // If we use ssl, set appropriate key- and trust-store.
        if (useSsl) {
            cc.setSslEnabled(true);

            GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

            factory.setKeyStoreFilePath(ggHome + "/examples/keystore/client.store");
            factory.setKeyStorePassword("123456".toCharArray());

            factory.setTrustStoreFilePath(ggHome + "/examples/keystore/trust.store");
            factory.setTrustStorePassword("123456".toCharArray());

            cc.setSslContextFactory(factory);
        }

        return GridClientFactory.start(cc);
    }
}
