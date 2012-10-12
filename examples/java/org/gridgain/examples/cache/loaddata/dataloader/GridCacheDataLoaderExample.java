// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.cache.loaddata.dataloader;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.*;

/**
 * Loads data from one node onto the rest of the data grid by utilizing {@link GridDataLoader}
 * API. {@link GridDataLoader} is a lot more efficient to use than standard
 * {@code GridCacheProjection.put(...)} operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * You can startup remote nodes either by starting {@link GridCacheDataLoaderNodeStartup}
 * class or stand alone. In case of stand alone node startup, remote nodes should always
 * be started with configuration which includes cache using following command:
 * {@code 'ggstart.sh examples/config/spring-cache-dataloader.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheDataLoaderExample {
    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 1000000;

    /**
     * Generates and loads data onto data grid directly form {@link GridDataLoader}
     * public API.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Grid g = G.start("examples/config/spring-cache-dataloader.xml");

        GridDataLoader<String, Integer> ldr = g.dataLoader("partitioned");
        // GridDataLoader<String, Integer> ldr = g.dataLoader("replicated");

        try {
            // Configure loader.
            ldr.perNodeBufferSize(10000);
            ldr.perTxKeysCount(100);
            ldr.perNodeParallelLoadOperations(Runtime.getRuntime().availableProcessors());

            // Warm up.
            load(ldr, 100000);

            X.println(">>> JVM is warmed up.");

            // Load.
            load(ldr, ENTRY_COUNT);

        }
        finally {
            ldr.close(false);

            G.stop(false, true);
        }
    }

    /**
     * Loads specified number of keys into cache using provided {@link GridDataLoader} instance.
     *
     * @param ldr Data loader.
     * @param cnt Number of keys to load.
     * @throws GridException If failed.
     */
    private static void load(GridDataLoader<String, Integer> ldr, int cnt) throws GridException {
        long start = System.currentTimeMillis();

        for (int i = 0; i < cnt; i++) {
            ldr.addData(Integer.toString(i), i);

            // Print out progress while loading cache.
            if (i > 0 && i % 10000 == 0)
                X.println("Loaded " + i + " keys.");
        }

        long end = System.currentTimeMillis();

        X.println(">>> Loaded " + cnt + " keys in " + (end - start) + "ms.");
    }
}
