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

import org.gridgain.scalar._
import scalar._
import scala.math._

/**
 * This example calculates Pi number in parallel on the grid. Note that these few
 * lines of code work on one node, two nodes or hundreds of nodes without any changes
 * or any explicit deployment.
 *
 * @author @java.author
 * @version @java.version
 */
object ScalarPiCalculationExample {
    /** Number of iterations per node. */
    private val N = 10000

    def main(args: Array[String]) {
        scalar {
            println("Pi estimate: " +
                grid$.spreadReduce(for (i <- 0 until grid$.size()) yield () => calcPi(i * N))(_.sum))
        }
    }

    /**
      * Calculates Pi range starting with given number.
      *
      * @param start Start the of the `{start, start + N}` range.
      * @return Range calculation.
      */
    def calcPi(start: Int): Double =
        // Nilakantha algorithm.
        ((max(start, 1) until (start + N)) map (i => 4.0 * (2 * (i % 2) - 1) / (2 * i) / (2 * i + 1) / (2 * i + 2)))
            .sum + (if (start == 0) 3 else 0)
}
