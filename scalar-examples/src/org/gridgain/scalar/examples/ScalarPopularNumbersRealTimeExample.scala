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
import java.util.{Random, Timer}

/**
 * Real time popular number counter. In order to run this example, you must start
 * at least one data grid nodes using command `ggstart.sh examples/config/spring-cache-popularcounts.xml`
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 *
 * @author @java.author
 * @version @java.version
 */
object ScalarPopularNumbersRealTimeExample extends App {
    private final val NUM_CNT = 10
    private final val RANGE = 1000
    private final val TOTAL_CNT = 100000

    // Will generate random numbers to get most popular ones.
    private final val RAND = new Random

    scalar("examples/config/spring-cache-popularcounts.xml") {
        val timer = new Timer("query-worker")

        try {
            // Schedule queries to run every 3 seconds during ingestion phase.
            timer.schedule(timerTask(query(NUM_CNT)), 3000, 3000)

            // Ingest data & force one more run to get the final counts.
            ingest()
            query(NUM_CNT)

            // Clean up after ourselves.
            grid$.projectionForCaches(null).bcastRun(() => grid$.cache().clearAll())
        }
        finally {
            timer.cancel()
        }
    }

    /**
     * Caches number counters in data grid.
     */
    def ingest() {
        // Set larger per-node buffer size since our state is relatively small.
        // Reduce parallel operations since we running the whole grid locally under heavy load.
        val ldr = dataLoader$[Int, Long](null, 2048, 8, 128)

        val f = (i: Long) => if (i == null) 1 else i + 1

        (0 until TOTAL_CNT) foreach (_ => ldr.addData(RAND.nextInt(RANGE), f))

        ldr.close(false)
    }

    /**
     * Queries a subset of most popular numbers from data grid.
     *
     * @param cnt Number of most popular numbers to return.
     */
    def query(cnt: Int) {
        cache$[Int, Long].get.sqlFields(clause = "select * from Long order by _val desc limit " + cnt).
            sortBy(_(1).asInstanceOf[Long]).reverse.take(cnt).foreach(println _)

        println("------------------")
    }
}
