// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.swapspace;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.spi.swapspace.leveldb.*;
import org.gridgain.grid.typedef.*;

/**
 * Example that shows using of {@link GridSwapSpaceSpi}.
 * <p>
 * <b>
 * NOTE: On Windows platforms {@code Microsoft Visual C++ Redistributable Package} must be installed
 * in order to use this SPI.
 * </b>
 */
public final class GridSwapSpaceExample {
    /**
     * Execute <tt>SwapSpace</tt> example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      <tt>"examples/config/"</tt> for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Typedefs:
        // ---------
        // G -> GridFactory
        // CIX1 -> GridInClosureX
        // CO -> GridOutClosure
        // CA -> GridAbsClosure
        // F -> GridFunc

        GridConfigurationAdapter cfg = new GridConfigurationAdapter();

        // -----
        // NOTE:
        // GridLevelDbSwapSpaceSpi requires Microsoft Visual C++ Redistributable Package on Windows.
        // -----
        cfg.setSwapSpaceSpi(new GridLevelDbSwapSpaceSpi());

        Grid g = G.start(cfg);

        try {
            String testData = "TestSwapSpaceData";

            // Execute SwapSpace task.
            g.execute(GridSwapSpaceTask.class, testData).get();
        }
        finally {
            G.stop(true);
        }
    }
}
