// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.popularwords;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.Executors.*;
import static org.gridgain.grid.cache.query.GridCacheQueryType.*;

/**
 * Real time popular words counter.
 *
 * Remote nodes should always be started with configuration which includes cache
 * using following command: {@code 'ggstart.sh examples/config/spring-cache-popularwords.xml'}.
 *
 * The counts are kept in cache on all remote nodes. Top {@code 10} counts from each node are then grabbed to produce
 * an overall top {@code 10} list within the grid.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridPopularWordsRealTimeExample {
    /** Number of most popular words to retrieve from grid. */
    private static final int POPULAR_WORDS_CNT = 10;

    /**
     * Start counting words.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        String ggHome = X.getSystemOrEnv("GRIDGAIN_HOME");

        assert ggHome != null : "GRIDGAIN_HOME must be set to the GridGain installation root.";

        final File inputDir = new File(ggHome, "examples/java/org/gridgain/examples/popularwords/books");

        if (!inputDir.exists()) {
            X.error("Input directory does not exist: " + inputDir.getAbsolutePath());

            return;
        }

        String[] books = inputDir.list();

        ExecutorService threadPool = newFixedThreadPool(books.length);

        Timer popularWordsQryTimer = new Timer("words-query-worker");

        // Start grid.
        final Grid g = G.start("examples/config/spring-cache-popularwords.xml");

        try {
            TimerTask task = scheduleQuery(g, popularWordsQryTimer, POPULAR_WORDS_CNT);

            realTimePopulate(g, new ExecutorCompletionService<Object>(threadPool), inputDir);

            // Force one more run to get final counts.
            task.run();

            popularWordsQryTimer.cancel();

            threadPool.shutdownNow();

            // Clean up caches on all nodes after run.
            g.run(GridClosureCallMode.BROADCAST, new Runnable() {
                @Override public void run() {
                    if (g.cache() == null)
                        X.error("Default cache not found (is spring-cache-popularwords.xml configuration used on all nodes?)");
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
     * Populate cache in real time with words and keep count for every word.
     *
     * @param g Grid.
     * @param threadPool Thread pool.
     * @param inputDir Input folder.
     * @throws Exception If failed.
     */
    private static void realTimePopulate(final Grid g, CompletionService<Object> threadPool, final File inputDir)
        throws Exception {
        String[] books = inputDir.list();

        // Count closure which increments a count for a word on remote node.
        final GridClosure<Integer, Integer> cntClo = new GridClosure<Integer, Integer>() {
            @Override public Integer apply(Integer cnt) {
                return cnt == null ? 1 : cnt + 1;
            }
        };

        final GridDataLoader<String, Integer> ldr = g.dataLoader(null);

        // Set larger per-node buffer size since our state is relatively small.
        ldr.perNodeBufferSize(300);
        //ldr.perNodeBufferSize(30);

        for (final String name : books) {
            // Read text files from multiple threads and cache individual words with their counts.
            threadPool.submit(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    X.println(">>> Storing all words from book in data grid: " + name);

                    BufferedReader in = new BufferedReader(new FileReader(new File(inputDir, name)));

                    try {
                        for (String line = in.readLine(); line != null; line = in.readLine())
                            for (final String w : line.split("[^a-zA-Z0-9]"))
                                if (!w.isEmpty())
                                    // Note that we are loading our closure which
                                    // will then calculate proper value on remote node.
                                    ldr.addData(w, cntClo);
                    }
                    finally {
                        in.close();
                    }

                    X.println(">>> Finished storing all words from book in data grid: " + name);

                    return null;
                }
            });
        }

        try {
            int idx = 0;

            while (++idx <= books.length)
                threadPool.take().get();

            ldr.close(false); // Pass 'false' to wait for loader to complete gracefully.
        }
        catch (Exception e) {
            e.printStackTrace();

            ldr.close(true); // Pass 'true' to cancel outstanding loading jobs in case of error.
        }
    }

    /**
     * Schedule our popular words query to run every 3 seconds.
     *
     * @param g Grid.
     * @param timer Timer.
     * @param cnt Number of popular words to return.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Grid g, Timer timer, final int cnt) {
        TimerTask task = new TimerTask() {
            private GridCacheReduceQuery<String, Integer, Map.Entry<String, Integer>, Object> qry;

            @Override public void run() {
                try {
                    // Get reference to cache.
                    GridCache<String, Integer> cache = g.cache();

                    if (qry == null) {
                        // Don't select words shorter than 3 letters.
                        qry = cache.createReduceQuery(
                            SQL,
                            Integer.class,
                            "length(_key) > 3 order by _val desc limit " + cnt // Standard SQL.
                        );

                        // This step is done locally (not on remote nodes).
                        qry.localReducer(new GridClosure<Object[], GridReducer<Map.Entry<String, Integer>, Object>>() {
                            @Override public GridReducer<Map.Entry<String, Integer>, Object> apply(Object[] o) {
                                return new GridReducer<Map.Entry<String, Integer>, Object>() {
                                    // Sorted map keyed by word counts.
                                    private NavigableMap<Integer, Collection<String>> words =
                                        new TreeMap<Integer, Collection<String>>();

                                    @Override public boolean collect(Map.Entry<String, Integer> entry) {
                                        // Get collection of words for given count.
                                        Collection<String> ws = words.get(entry.getValue());

                                        if (ws == null)
                                            words.put(entry.getValue(), ws = new LinkedList<String>());

                                        // Add word to collection of words for given count.
                                        ws.add(entry.getKey());

                                        return true;
                                    }

                                    @Override public Object apply() {
                                        int idx = 0;

                                        // Print out 10 most popular words.
                                        for (Map.Entry<Integer, Collection<String>> e : words.descendingMap().entrySet()) {
                                            for (String w : e.getValue()) {
                                                X.println(">>> " + e.getKey() + '=' + w);

                                                if (++idx == cnt)
                                                    break;
                                            }

                                            if (idx == cnt)
                                                break;
                                        }

                                        X.println("------------");

                                        return null;
                                    }
                                };
                            }
                        });
                    }

                    // Get projection of grid nodes that have cache
                    // without name (null name) running and execute
                    // our query on it.
                    qry.reduce(g.projectionForCaches(null)).get();
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
