// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client;

import org.gridgain.grid.typedef.*;
import org.gridgain.grid.util.*;

/**
 * Starts up grid node (server) with pre-defined ports and tasks to test client-server interactions.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 * <p>
 * After this example has been started you can use pre-defined endpoints and task names in your
 * client-server interactions to work with the nodes over secured protocols (binary over SSL or https).
 * <p>
 * Available end-points:
 * <ul>
 *     <li>127.0.0.1:10443 - TCP SSL-protected endpoint.</li>
 *     <li>127.0.0.1:11443 - HTTP SSL-protected endpoint.</li>
 * </ul>
 * <p>
 * Required credentials for remote client authentication: "s3cret".
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridServerSslNodeStartup {
    /**
     * Starts up two nodes with specified cache configuration on pre-defined endpoints.
     *
     * @param args Command line arguments, none required.
     * @throws Exception In case of any exception.
     */
    public static void main(String[] args) throws Exception {
        if (!GridUtils.isEnterprise()) {
            X.println(">>> This example is available in enterprise edition only.\n" +
                ">>> \n" +
                ">>> To start this example with community edition you should disable\n" +
                ">>> authentication and secure session SPIs in the configuration files.");

            return;
        }

        try {
            G.start("examples/config/spring-server-ssl-node.xml");

            X.println(">>> Press 'Ctrl+C' to stop the process...");

            Thread.sleep(Long.MAX_VALUE);
        }
        finally {
            G.stopAll(true);
        }
    }
}
