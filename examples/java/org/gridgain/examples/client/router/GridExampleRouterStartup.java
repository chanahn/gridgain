// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client.router;

import org.gridgain.client.router.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;

import javax.swing.*;
import java.net.*;

/**
 * This example shows how to configure and run TCP and HTTP routers from java API.
 * <p>
 * Refer to {@link GridRouterFactory} documentation for more details on
 * how to manage routers' lifecycle. Also see {@link GridTcpRouterConfiguration}
 * and {@link GridHttpRouterConfiguration} for more configuration options.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridExampleRouterStartup {
    /** Change to {@code false} to disable {@code TCP_NODELAY}. */
    private static final boolean TCP_NODELAY = true;

    /**
     * Change to {@code true} to enable SSL.
     * Note that you need to appropriately update node and client configurations.
     */
    private static final boolean SSL_ENABLED = false;

    /**
     * Starts up a router with default configuration.
     *
     * @param args Command line arguments, none required.
     * @throws UnknownHostException If router can't connect to Grid.
     * @throws GridException If router failed to start.
     * @throws GridSslException If router failed to create a SSL context.
     */
    public static void main(String[] args) throws UnknownHostException, GridException, GridSslException {
        try {
            GridRouterFactory.startTcpRouter(tcpRouterConfiguration());

            GridRouterFactory.startHttpRouter(httpRouterConfiguration());

            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[]{
                    new JLabel("GridGain router started."),
                    new JLabel("Press OK to stop GridGain router.")
                },
                "GridGain router",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
        finally {
            GridRouterFactory.stopAllRouters();
        }
    }

    /**
     * Creates a default TCP router configuration.
     *
     * @return TCP router configuration
     */
    private static GridTcpRouterConfiguration tcpRouterConfiguration() {
        GridTcpRouterConfigurationAdapter cfg = new GridTcpRouterConfigurationAdapter();

        cfg.setNoDelay(TCP_NODELAY);

        if (SSL_ENABLED) {
            GridSslBasicContextFactory sslFactory = new GridSslBasicContextFactory();

            sslFactory.setKeyStoreFilePath("examples/keystore/client.jks");
            sslFactory.setKeyStorePassword("123456".toCharArray());

            cfg.setSslContextFactory(sslFactory);
        }

        // Uncomment the following line to set router's credentials.
        // Note that you need to appropriately update node configuration.
        // See GridAuthenticationAndSecureSessionClientExample for more details
        // on how to configure client authentication.
        //cfg.setCredentials("s3cret");

        return cfg;
    }

    /**
     * Creates a default HTTP router configuration.
     *
     * @return HTTP router configuration
     */
    private static GridHttpRouterConfiguration httpRouterConfiguration() {
        GridHttpRouterConfigurationAdapter cfg = new GridHttpRouterConfigurationAdapter();

        // Uncomment the following line to provide custom Jetty configuration.
        //cfg.setJettyConfigurationPath("config/my-router-jetty.xml");

        if (SSL_ENABLED) {
            GridSslBasicContextFactory sslFactory = new GridSslBasicContextFactory();

            sslFactory.setKeyStoreFilePath("examples/keystore/client.jks");
            sslFactory.setKeyStorePassword("123456".toCharArray());

            cfg.setClientSslContextFactory(sslFactory);
        }

        // Uncomment the following line to set router's credentials.
        // Note that you need to appropriately update node configuration.
        // See GridAuthenticationAndSecureSessionClientExample for more details
        // on how to configure client authentication.
        //cfg.setCredentials("s3cret");

        return cfg;
    }
}
