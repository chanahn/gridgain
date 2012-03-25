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
 * Real time popular words counter. In order to run this example, you must start
 * at least one data grid nodes using command {@code 'ggstart.sh examples/config/spring-cache-popularwords.xml'}.
 * The counts are kept in cache on all remote nodes. Top {@code 10} counts from each node are then grabbed to produce
 * an overall top {@code 10} list within the grid.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
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
        Grid g = G.start("examples/config/spring-cache-popularwords.xml");

        try {
            TimerTask task = scheduleQuery(g, popularWordsQryTimer, POPULAR_WORDS_CNT);

            realTimePopulate(g, new ExecutorCompletionService<Object>(threadPool), inputDir);

            // Force one more run to get final counts.
            task.run();
        }
        finally {
            popularWordsQryTimer.cancel();

            threadPool.shutdownNow();

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

        for (final String name : books) {
            // Read text files from multiple threads and cache individual words with their counts.
            threadPool.submit(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    X.println(">>> Storing all words from book in data grid: " + name);

                    BufferedReader in = new BufferedReader(new FileReader(new File(inputDir, name)));

                    try {
                        final GridCache<String, Integer> cache = g.cache();

                        GridFuture<?> fut = null;

                        for (String line = in.readLine(); line != null; line = in.readLine()) {
                            for (final String w : line.split("[^a-zA-Z0-9]")) {
                                if (!w.isEmpty()) {
                                    if (fut != null)
                                        fut.get();

                                    fut = g.affinityCallAsync(null, w, new GridCallable<Object>() {
                                        @Override public Object call() throws Exception {
                                            // Start transaction to make sure that 'get' and 'put' are
                                            // consistent with each other.
                                            GridCacheTx tx = cache.txStart();

                                            try {
                                                Integer cntr = cache.get(w);

                                                cache.put(w, cntr == null ? 1 : cntr + 1);

                                                tx.commit();
                                            }
                                            finally {
                                                tx.end();
                                            }

                                            return null;
                                        }
                                    });
                                }
                            }
                        }
                    }
                    finally {
                        in.close();
                    }

                    X.println(">>> Finished storing all words from book in data grid: " + name);

                    return null;
                }
            });
        }

        int idx = 0;

        while (++idx <= books.length)
            threadPool.take().get();
    }

    /**
     * Query popular words.
     *
     * @param g Grid.
     * @param timer Timer.
     * @param cnt Number of popular words to return.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Grid g, Timer timer, final int cnt) {
        TimerTask task = new TimerTask() {
            private GridCacheReduceQuery<String, Integer, Map<Integer, Collection<String>>, Object> qry;

            @Override public void run() {
                try {
                    GridCache<String, Integer> cache = g.cache();

                    if (qry == null) {
                        // Don't select words shorter than 3 letters.
                        qry = cache.createReduceQuery(
                            SQL,
                            Integer.class,
                            "length(_key) > 3 order by _val desc limit " + cnt
                        );

                        qry.remoteReducer(new C1<Object[], GridReducer<Map.Entry<String, Integer>, Map<Integer, Collection<String>>>>() {
                            @Override
                            public GridReducer<Map.Entry<String, Integer>, Map<Integer, Collection<String>>> apply(Object[] o) {
                                return new GridReducer<Map.Entry<String, Integer>, Map<Integer, Collection<String>>>() {
                                    private Map<Integer, Collection<String>> m = new HashMap<Integer, Collection<String>>();

                                    @Override public boolean collect(Map.Entry<String, Integer> e) {
                                        Collection<String> words = m.get(e.getValue());

                                        if (words == null)
                                            m.put(e.getValue(), words = new LinkedList<String>());

                                        words.add(e.getKey());

                                        return true;
                                    }

                                    @Override public Map<Integer, Collection<String>> apply() {
                                        return m;
                                    }
                                };
                            }
                        });

                        qry.localReducer(new C1<Object[], GridReducer<Map<Integer, Collection<String>>, Object>>() {
                            @Override public GridReducer<Map<Integer, Collection<String>>, Object> apply(Object[] o) {
                                return new GridReducer<Map<Integer, Collection<String>>, Object>() {
                                    private NavigableMap<Integer, Collection<String>> words = new TreeMap<Integer, Collection<String>>();

                                    @Override public boolean collect(Map<Integer, Collection<String>> m) {
                                        words.putAll(m);

                                        return true;
                                    }

                                    @Override public Object apply() {
                                        int idx = 0;

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
