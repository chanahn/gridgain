// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.cache.loaddata.storeloader;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;

import java.util.concurrent.*;

/**
 * Loads data from persistent store at cache startup by calling
 * {@link GridCache#loadCache(GridPredicate2, long, Object...)} method on
 * all nodes.
 * <p>
 * For this example you should startup remote nodes only by calling
 * {@link GridCacheStoreLoaderNodeStartup} class.
 * <p>
 * You should not be using stand-alone nodes because GridGain nodes do not
 * know about the {@link GridCacheLoaderStore} we define in this example.
 * Users can always add their clases to {@code GRIDGAIN_HOME/libs/ext} folder
 * to make them available to GridGain. If this was done here, we could
 * easily startup remote nodes with
 * {@code 'ggstart.sh examples/config/spring-cache-storeloader.xml'} command.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheStoreLoaderExample {
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
        Grid g = G.start("examples/config/spring-cache-storeloader.xml");

        try {
            // Warm up.
            load(g, 100000);

            X.println(">>> JVM is warmed up.");

            // Load.
            load(g, ENTRY_COUNT);
        }
        finally {
            G.stop(false, true);
        }
    }

    /**
     * Loads specified number of keys into cache using provided {@link GridDataLoader} instance.
     *
     * @param g Grid instance.
     * @param cnt Number of keys to load.
     * @throws GridException If failed.
     */
    private static void load(Grid g, final int cnt) throws GridException {
        final GridCache<String, Integer> cache = g.cache("partitioned");

        long start = System.currentTimeMillis();

        // Start loading cache on all nodes.
        g.call(GridClosureCallMode.BROADCAST, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.loadCache(null, 0, cnt);

                return null;
            }
        });

        long end = System.currentTimeMillis();

        X.println(">>> Loaded " + cnt + " keys with backups in " + (end - start) + "ms.");
    }
}
