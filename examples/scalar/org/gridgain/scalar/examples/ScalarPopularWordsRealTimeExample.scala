// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.examples

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid.typedef.X
import java.io.File
import io.Source
import java.util.{TimerTask, Timer}
import actors.threadpool.{ExecutorCompletionService, Callable, CompletionService, Executors}
import collection.immutable.{TreeMap}
import org.gridgain.grid.{GridDataLoader, GridClosureCallMode}
import GridClosureCallMode._

/**
 * Real time popular words counter. In order to run this example, you must start
 * at least one data grid nodes using command `ggstart.sh examples/config/spring-cache-popularwords.xml`
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.2c.12042012
 */
object ScalarPopularWordsRealTimeExample {
    private final val POPULAR_WORDS_CNT = 10;
    private final val BOOK_PATH = "examples/java/org/gridgain/examples/popularwords/books"

    type JInt = java.lang.Integer

    def main(args: Array[String]) {
        val bookDir = new File(X.getSystemOrEnv("GRIDGAIN_HOME"), BOOK_PATH)

        if (!bookDir.exists) {
            println("Input directory does not exist: " + bookDir.getAbsolutePath)

            return
        }

        scalar("examples/config/spring-cache-popularwords.xml") {
            val threadPool = Executors.newFixedThreadPool(bookDir.list.length);

            val popWordsQryTimer = new Timer("words-query-worker");

            try {
                // Schedule word queries to run every 3 seconds.
                popWordsQryTimer.schedule(new TimerTask {
                    def run() {
                        queryPopularWords(POPULAR_WORDS_CNT)
                    }
                }, 3000, 3000)

                // Populate cache with word counts.
                populate(new ExecutorCompletionService(threadPool), bookDir)

                // Force one more run to get final counts.
                queryPopularWords(POPULAR_WORDS_CNT)

                // Clean up after ourselves.
                grid$.projectionForCaches(null).run(BROADCAST, () => grid$.cache().clearAll())
            }
            finally {
                popWordsQryTimer.cancel()

                threadPool.shutdownNow()
            }
        }
    }

    /**
     * Cache word counters in data grid. For more parallel processing, process every book in a separate thread.
     *
     * @param threadPool Thread pool.
     * @param bookDir Directory where books are.
     */
    def populate(threadPool: CompletionService, bookDir: File) {
        val books = bookDir.list()

        val ldr: GridDataLoader[String, JInt] = grid$.dataLoader(null)

        // Set larger per-node buffer size since our state is relatively small.
        ldr.perNodeBufferSize(300);

        // For every book, start a new thread and start populating cache
        // with words and their counts.
        for (name <- books) {
            threadPool.submit(new Callable {
                def call() = {
                    println(">>> Storing all words from book in data grid: " + name);

                    Source.fromFile(new File(bookDir, name), "ISO-8859-1").getLines().foreach(line => {
                        line.split("[^a-zA-Z0-9]").foreach(word => {
                            if (!word.isEmpty)
                                ldr.addData(word, (cnt: JInt) => (if (cnt == null) 1 else cnt + 1).asInstanceOf[JInt])
                        })
                    })

                    println(">>> Finished storing all words from book in data grid: " + name)

                    None
                }
            })
        }

        // Wait for all threads to finish.
        books.foreach(_ => threadPool.take().get())
    }

    /**
     * Query a subset of most popular words from data grid.
     *
     * @param cnt Number of most popular words to return.
     */
    def queryPopularWords(cnt: Int) {
        type SSeq = Seq[String] // Type alias for sequences of strings (for readability only).

        grid$.cache[String, JInt].sqlReduce(
            // PROJECTION (where to run):
            grid$.projectionForCaches(null),

            // SQL QUERY (what to run):
            "length(_key) > 3 order by _val desc limit " + cnt,

            // REMOTE REDUCER (how to reduce on remote nodes):
            (it: Iterable[(String, JInt)]) =>
                // Pre-reduce by converting Seq[(String, JInt)] to Map[JInt, Seq[String]].
                (it :\ Map.empty[JInt, SSeq])((e, m) => m + (e._2 -> (m.getOrElse(e._2, Seq.empty[String]) :+ e._1))),

            // LOCAL REDUCER (how to finally reduce on local node):
            (it: Iterable[Map[JInt, SSeq]]) => {
                // Print 'cnt' or most popular words collected from all remote nodes.
                (new TreeMap()(implicitly[Ordering[JInt]].reverse) ++ it.flatten).take(cnt).foreach(println _)

                println("------------")
            }
        )
    }
}
