// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.realtime;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

/**
 * Real time popular numbers counter.
 * <p>
 * Remote nodes should always be started with configuration which includes cache
 * using following command: {@code 'ggstart.sh examples/config/spring-cache-popularcounts.xml'}.
 * <p>
 * The counts are kept in cache on all remote nodes. Top {@code 10} counts from each node are
 * then grabbed to produce an overall top {@code 10} list within the grid.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridPopularNumbersRealTimeExample {
    /** Count of most popular numbers to retrieve from grid. */
    private static final int POPULAR_NUMBERS_CNT = 10;

    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Range within which to generate numbers. */
    private static final int RANGE = 1000;

    /** Count of total numbers to generate. */
    private static final int CNT = 1000000;

    /**
     * Starts counting numbers.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Timer popularNumbersQryTimer = new Timer("numbers-query-worker");

        // Start grid.
        final Grid g = G.start("examples/config/spring-cache-popularcounts.xml");

        try {
            TimerTask task = scheduleQuery(g, popularNumbersQryTimer, POPULAR_NUMBERS_CNT);

            realTimePopulate(g);

            // Force one more run to get final counts.
            task.run();

            popularNumbersQryTimer.cancel();

            // Clean up caches on all nodes after run.
            g.run(GridClosureCallMode.BROADCAST, new Runnable() {
                @Override public void run() {
                    if (g.cache() == null)
                        X.error("Default cache not found (is spring-cache-popularcounts.xml " +
                            "configuration used on all nodes?)");
                    else {
                        X.println("Clearing keys from cache: " + g.cache().keySize());

                        g.cache().clearAll();
                    }
                }
            });
        }
        finally {
            G.stop(true, true);
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void realTimePopulate(final Grid g) throws GridException {
        final GridDataLoader<Integer, Long> ldr = g.dataLoader(null);

        try {
            // Set larger per-node buffer size since our state is relatively small.
            ldr.perNodeBufferSize(2048);

            // Reduce parallel operations since we running
            // the whole grid locally under heavy load.
            ldr.perNodeParallelLoadOperations(8);

            // Set max keys count per TX.
            ldr.perTxKeysCount(128);

            // Count closure which increments a count for a word on remote node.
            final GridClosure<Long, Long> cntClo = new GridClosure<Long, Long>() {
                @Override public Long apply(Long cnt) {
                    return cnt == null ? 1 : cnt + 1;
                }
            };

            for (int i = 0; i < CNT; i++)
                ldr.addData(RAND.nextInt(RANGE), cntClo);
        }
        finally {
            ldr.close(false);
        }
    }

    /**
     * Schedules our popular numbers query to run every 3 seconds.
     *
     * @param g Grid.
     * @param timer Timer.
     * @param cnt Number of popular numbers to return.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Grid g, Timer timer, final int cnt) {
        TimerTask task = new TimerTask() {
            private GridCacheFieldsQuery qry;

            @Override public void run() {
                // Get reference to cache.
                GridCache<Integer, Long> cache = g.cache();

                if (qry == null)
                    qry = cache.createFieldsQuery("select _key, _val from Long order by _val desc limit " + cnt);

                try {
                    List<List<Object>> results = new ArrayList<List<Object>>(qry.execute(g).get());

                    Collections.sort(results, new Comparator<List<Object>>() {
                        @Override public int compare(List<Object> r1, List<Object> r2) {
                            long cnt1 = (Long)r1.get(1);
                            long cnt2 = (Long)r2.get(1);

                            return cnt1 < cnt2 ? 1 : cnt1 > cnt2 ? -1 : 0;
                        }
                    });

                    for (int i = 0; i < cnt; i++) {
                        List<Object> res = results.get(i);

                        X.println(res.get(0) + "=" + res.get(1));
                    }

                    X.println("----------------");
                }
                catch (GridException e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(task, 3000, 3000);

        return task;
    }
}
