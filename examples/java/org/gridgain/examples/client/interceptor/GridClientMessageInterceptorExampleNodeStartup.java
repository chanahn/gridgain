// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.client.interceptor;

import org.gridgain.grid.typedef.*;

import javax.swing.*;

/**
 * Starts up grid node (server) for use with {@link GridClientMessageInterceptorExample}.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridClientMessageInterceptorExampleNodeStartup {
    /**
     * Starts up a node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws Exception In case of any exception.
     */
    public static void main(String[] args) throws Exception {
        G.start(args.length > 0 ? args[0] : "examples/config/spring-cache-client-interceptor.xml");

        try {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[]{
                    new JLabel("GridGain started."),
                    new JLabel("Press OK to stop GridGain.")
                },
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
        finally {
            G.stopAll(true);
        }
    }
}
