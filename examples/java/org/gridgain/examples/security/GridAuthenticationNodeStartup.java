// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.security;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.*;

import javax.swing.*;

import static javax.swing.JOptionPane.*;

/**
 * Starts up an empty node with passcode-based authentication configuration.
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/spring-authentication-passcode.xml'}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.2c.12042012
 */
public class GridAuthenticationNodeStartup {
    /**
     * Start up an empty node with specified authentication configuration.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     */
    public static void main(String[] args) {
        try {
            G.start(args.length == 0 ? "examples/config/spring-authentication-passcode.xml" : args[0]);

            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("GridGain started."),
                    new JLabel("Press OK to stop GridGain.")
                },
                "GridGain",
                INFORMATION_MESSAGE
            );
        }
        catch (GridException e) {
            if (e.hasCause(ClassNotFoundException.class))
                X.println("Failed to create grid " +
                        "('security' is enterprise feature, are you using community edition?): " + e.getMessage());
            else
                X.println("Failed to create grid: " + e.getMessage());
        }
        finally {
            G.stop(true);
        }
    }
}
