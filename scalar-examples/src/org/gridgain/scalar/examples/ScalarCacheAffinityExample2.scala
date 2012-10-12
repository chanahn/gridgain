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
import org.gridgain.grid._
import GridClosureCallMode._
import lang.{GridFunc => F}
import collection.JavaConversions._
import scala.util.control.Breaks._

/**
 * Note that affinity routing is enabled for all caches.
 *
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ggstart.sh examples/config/spring-cache.xml'`. Local node can
 * be started with or without cache.
 *
 * @author @java.author
 * @version @java.version
 */
object ScalarCacheAffinityExample2 {
    /** Configuration file name. */
    //private val CONFIG = "examples/config/spring-cache-none.xml" // No cache - remote node with cache is required.
    private val CONFIG = "examples/config/spring-cache.xml" // Cache.

    /** Name of cache specified in spring configuration. */
    private val NAME = "partitioned"

    /**
     * Example entry point. No arguments required.
     *
     * Note that in case of `LOCAL` configuration,
     * since there is no distribution, values may come back as `nulls`.
     */
    def main(args: Array[String]) {
        scalar(CONFIG) {
            var keys = Seq.empty[String]

            ('A' to 'Z').foreach(keys :+= _.toString)

            populateCache(grid$, keys)

            // Map all keys to nodes.
            var mappings = grid$.mapKeysToNodes(NAME, keys);

            // If on community edition, we have to get mappings from GridCache
            // directly as affinity mapping without cache started
            // is not supported on community edition.
            if (mappings == null)
                mappings = cache$[String, String](NAME).get.mapKeysToNodes(keys);

            mappings.foreach(mapping => {
                val node = mapping._1
                val mappedKeys = mapping._2

                if (node != null) {
                    node *< (() => {
                        breakable {
                            println(">>> Executing affinity job for keys: " + mappedKeys);

                            // Get cache.
                            val cache = cache$[String, String](NAME);

                            // If cache is not defined at this point then it means that
                            // job was not routed by affinity.
                            if (!cache.isDefined)
                                println(">>> Cache not found [nodeId=" + grid$.localNode().id() +
                                    ", cacheName=" + NAME + ']').^^

                            // Check cache without loading the value.
                            mappedKeys.foreach(key => println(">>> Peeked at: " + cache.get.peek(key)))
                        }
                    })
                }
            })
        }
    }

    /**
     * Populates cache with given keys. This method accounts for the case when
     * cache is not started on local node. In that case a job which populates
     * the cache will be sent to the node where cache is started.
     *
     * @param g Grid.
     * @param keys Keys to populate.
     */
    private def populateCache(g: Grid, keys: Seq[String]) {
        var prj = g.projectionForPredicate(F.cacheNodesForNames(NAME))

        // Give preference to local node.
        if (prj.nodes().contains(g.localNode))
            prj = g.localNode

        // Populate cache on some node (possibly this node)
        // which has cache with given name started.
        prj.ucastRun(
            () => {
                println(">>> Storing keys in cache: " + keys)

                val c = cache$[String, String](NAME).get

                keys.foreach(key => c += (key -> key.toLowerCase))
            }
        )
    }
}
