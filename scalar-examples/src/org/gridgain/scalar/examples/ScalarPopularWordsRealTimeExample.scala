// @scala.file.header

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
import java.util.Timer

/**
 * Real time popular words counter. In order to run this example, you must start
 * at least one data grid nodes using command `ggstart.sh examples/config/spring-cache-popularcounts.xml`
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 *
 * @author @java.author
 * @version @java.version
 */
object ScalarPopularWordsRealTimeExample extends App {
    private final val WORDS_CNT = 10
    private final val BOOK_PATH = "examples/java/org/gridgain/examples/realtime/books"

    val dir = new File(X.getSystemOrEnv("GRIDGAIN_HOME"), BOOK_PATH)

    if (!dir.exists)
        sys.error("Input directory does not exist: " + dir.getAbsolutePath)
    else
        scalar("examples/config/spring-cache-popularcounts.xml") {
            // Data cache instance (default cache).
            val c = cache$[String, Int].get

            val timer = new Timer("query-worker")

            try {
                // Schedule word queries to run every 3 seconds.
                // Note that queries will run during ingestion phase.
                timer.schedule(timerTask(query(WORDS_CNT)), 2000, 2000)

                // Populate cache & force one more run to get the final counts.
                ingest(dir)
                query(WORDS_CNT)

                // Clean up after ourselves.
                c.globalClearAll()
            }
            finally {
                timer.cancel()
            }

            /**
             * Caches word counters in data grid.
             *
             * @param dir Directory where books are.
             */
            def ingest(dir: File) {
                // Set larger per-node buffer size since our state is relatively small.
                // Reduce parallel operations since we running the whole grid locally under heavy load.
                val ldr = dataLoader$[String, Int](null, 2048, 8, 128)

                val f = (i: Int) => if (i == null) 1 else i + 1

                // For every book, allocate a new thread from the pool and start populating cache
                // with words and their counts.
                try
                    for (book <- dir.list()) yield
                        Source.fromFile(new File(dir, book)).getLines().foreach(
                            line => for (w <- line.split("[^a-zA-Z0-9]") if w.length > 3) ldr.addData(w, f))
                finally
                    ldr.close(false) // Wait for data loader to complete.
            }

            /**
             * Queries a subset of most popular words from data grid.
             *
             * @param cnt Number of most popular words to return.
             */
            def query(cnt: Int) {
                c.sql("select * from Integer order by _val desc limit " + cnt).
                    toSeq.sortBy[Int](_._2).reverse.take(cnt).foreach(println _)

                println("------------------")
            }
        }
}
