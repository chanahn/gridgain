// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.security;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.typedef.*;

import javax.swing.*;
import java.util.*;

import static javax.swing.JOptionPane.*;
import static org.gridgain.grid.GridClosureCallMode.*;

/**
 * Example that shows using of {@link GridAuthenticationSpi}. It sends broadcast
 * text message to all nodes in authentication-restricted topology. To start
 * remote node, you can run {@link GridAuthenticationNodeStartup} class.
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/spring-authentication-passcode.xml'}.
 */
public final class GridAuthenticationNodeExample {
    /**
     * Executes <tt>Authentication</tt> example on the grid and sends broadcast message
     * to all nodes in the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Typedefs:
        // ---------
        // G -> GridFactory
        // CIX1 -> GridInClosureX
        // F -> GridFunc
        // U -> GridUtils

        // When you start remote nodes, authentication process is invoked automatically,
        // so if you see topology change events, it means that authentication succeeded.

        G.in(args.length == 0 ? "examples/config/spring-authentication-passcode.xml" : args[0], new CIX1<Grid>() {
            @Override public void applyx(Grid g) throws GridException {
                String title = "GridGain started at " + new Date();
                String msg = "Press OK to send broadcast message, cancel to exit.";

                // Ask user to send broadcast message.
                while (confirm(title, msg)) {
                    // Send notification message to all nodes in topology.
                    g.run(BROADCAST, F.println(">>> Broadcast message sent from node=" + g.localNode().id()));
                }
            }
        });
    }

    /**
     * Display confirmation dialog.
     *
     * @param title Dialog title.
     * @param msg Dialog message.
     * @return {@code true} if user presses OK button, {@code false} in all other cases.
     */
    private static boolean confirm(String title, String msg) {
        return OK_OPTION == JOptionPane.showConfirmDialog(
            null,
            new JComponent[] {
                new JLabel(title),
                new JLabel(msg)
            },
            "GridGain",
            OK_CANCEL_OPTION
        );
    }
}
