// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.cache.putget;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.GridEventType.*;

/**
 * This example demonstrates very basic operations on cache, such as 'put' and 'get'.
 * We first populate cache by putting values into it, and then we 'peek' at values on
 * remote nodes, and then we 'get' those values. For replicated cache, values should
 * be everywhere at all times. For partitioned cache, 'peek' on some nodes may return
 * {@code null} due to partitioning, however, 'get' operation should never return {@code null}.
 * <p>
 * When starting remote nodes, make sure to use the same configuration file as follows:
 * <pre>
 *     GRIDGAIN_HOME/bin/ggstart.sh examples/config/spring-cache.xml
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
@GridNotAvailableIn(GridEdition.COMPUTE_GRID)
public class GridCachePutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /**
     * Runs basic cache example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Grid g = args.length == 0 ? G.start("examples/config/spring-cache.xml") : G.start(args[0]);

        try {
            // Subscribe to events on every node, so we can visualize what's
            // happening in remote caches.
            g.run(BROADCAST, new CA() {
                @GridInstanceResource
                private Grid g;

                @Override public void apply() {
                    GridLocalEventListener lsnr = new GridLocalEventListener() {
                        @Override public void onEvent(GridEvent evt) {
                            switch (evt.type()) {
                                case EVT_CACHE_OBJECT_PUT:
                                case EVT_CACHE_OBJECT_READ:
                                case EVT_CACHE_OBJECT_REMOVED: {
                                    GridCacheEvent e = (GridCacheEvent)evt;

                                    X.println("Cache event [name=" + e.name() + ", key=" + e.key() + ']');
                                }
                            }
                        }
                    };

                    GridNodeLocal<String, GridLocalEventListener> loc = g.nodeLocal();

                    GridLocalEventListener prev = loc.remove("lsnr");

                    // If there is a listener subscribed from previous runs, unsubscribe it.
                    if (prev != null)
                        g.removeLocalEventListener(prev);

                    // Record new listener, so we can check it on next run.
                    loc.put("lsnr", lsnr);

                    // Subscribe listener.
                    g.addLocalEventListener(lsnr, EVTS_CACHE);
                }
            });

            GridCacheProjection<String, String> cache = g.cache(CACHE_NAME).
                projection(String.class, String.class);

            final int keyCnt = 20;

            // Store keys in cache.
            for (int i = 0; i < keyCnt; i++)
                cache.putx(Integer.toString(i), Integer.toString(i));

            // Peek and get on local node.
            for (int i = 0; i < keyCnt; i++) {
                X.println("Peeked [key=" + i + ", val=" + cache.peek(Integer.toString(i)) + ']');
                X.println("Got [key=" + i + ", val=" + cache.get(Integer.toString(i)) + ']');
            }

            // Projection (view) for remote nodes.
            GridProjection rmts = g.remoteProjection();

            if (!rmts.isEmpty()) {
                // Peek and get on remote nodes (comment it out if output gets too crowded).
                rmts.run(BROADCAST, new GridAbsClosureX() {
                    @GridInstanceResource
                    private Grid g;

                    @Override public void applyx() throws GridException {
                        GridCacheProjection<String, String> cache = g.cache(CACHE_NAME).
                            projection(String.class, String.class);

                        if (cache == null) {
                            X.println("Cache was not found [locNodeId=" + g.localNode().id() + ", cacheName=" +
                                CACHE_NAME + ']');

                            return;
                        }

                        for (int i = 0; i < keyCnt; i++) {
                            X.println("Peeked [key=" + i + ", val=" + cache.peek(Integer.toString(i)) + ']');
                            X.println("Got [key=" + i + ", val=" + cache.get(Integer.toString(i)) + ']');
                        }
                    }
                });
            }

            // Unsubscribe from events listening on every node.
            g.run(BROADCAST, new CA() {
                @GridInstanceResource
                private Grid g;

                @Override public void apply() {
                    GridNodeLocal<String, GridLocalEventListener> loc = g.nodeLocal();

                    GridLocalEventListener prev = loc.remove("lsnr");

                    // If there is a listener subscribed from previous runs, unsubscribe it.
                    if (prev != null)
                        g.removeLocalEventListener(prev);
                }
            });

        }
        finally {
            G.stop(true);
        }
    }
}
