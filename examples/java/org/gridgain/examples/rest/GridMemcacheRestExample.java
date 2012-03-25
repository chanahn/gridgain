// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.rest;

import net.spy.memcached.*;
import org.gridgain.examples.cache.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.typedef.*;

import java.io.*;
import java.net.*;

/**
 * This example shows how to use Memcache client for manipulating GridGain cache.
 * <p>
 * GridGain implements Memcache binary protocol and it's available if
 * REST is enabled on the node.
 * <p>
 * If you want to test this example with remote node, please don't start remote
 * node from command line but start {@link GridCacheDefaultNodeStartup} from IDE
 * instead because this example requires that some classes are present in classpath
 * on all nodes,
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridMemcacheRestExample {
    /**
     * @param args Command line arguments.
     * @throws GridException In case of error.
     */
    public static void main(String[] args) throws Exception {
        Grid grid = G.start("examples/config/spring-cache-default.xml");

        MemcachedClient client = null;

        try {
            GridCache<String, Object> cache = grid.cache();

            String host = grid.configuration().getRestTcpHost();
            int port = grid.configuration().getRestTcpPort();

            if (host == null)
                host = "localhost";

            client = startClient(host, port);

            // Put string value to cache using Memcache binary protocol.
            if (client.add("strKey", 0, "strVal").get())
                X.println(">>> Successfully put string value using Memcache client.");

            // Check that string value is actually in cache using traditional
            // GridGain API and Memcache binary protocol.
            X.println(">>> Getting value for 'strKey' using GridGain cache API: " + cache.get("strKey"));
            X.println(">>> Getting value for 'strKey' using Memcache client: " + client.get("strKey"));

            // Remove string value from cache using Memcache binary protocol.
            if (client.delete("strKey").get())
                X.println(">>> Successfully removed string value using Memcache client.");

            // Check that cache is empty.
            X.println(">>> Current cache size: " + cache.size() + " (expected: 0).");

            X.println(">>>");

            // Put integer value to cache using Memcache binary protocol.
            if (client.add("intKey", 0, 100).get())
                X.println(">>> Successfully put integer value using Memcache client.");

            // Check that integer value is actually in cache using traditional
            // GridGain API and Memcache binary protocol.
            X.println(">>> Getting value for 'intKey' using GridGain cache API: " + cache.get("intKey"));
            X.println(">>> Getting value for 'intKey' using Memcache client: " + client.get("intKey"));

            // Remove string value from cache using Memcache binary protocol.
            if (client.delete("intKey").get())
                X.println(">>> Successfully removed integer value using Memcache client.");

            // Check that cache is empty.
            X.println(">>> Current cache size: " + cache.size() + " (expected: 0).");

            X.println(">>>");

            // Put Person instance to cache using Memcache binary protocol.
            if (client.add("personKey", 0, new Person("John", "Smith", 1500.0, "Bachelor degree")).get())
                X.println(">>> Successfully put person using Memcache client.");

            // Check that Person is actually in cache using traditional
            // GridGain API and Memcache binary protocol.
            X.println(">>> Getting value for 'personKey' using GridGain cache API: " + cache.get("personKey"));
            X.println(">>> Getting value for 'personKey' using Memcache client: " + client.get("personKey"));

            // Remove Person from cache using Memcache binary protocol.
            if (client.delete("personKey").get())
                X.println(">>> Successfully removed person using Memcache client.");

            // Check that cache is empty.
            X.println(">>> Current cache size: " + cache.size() + " (expected: 0).");

            X.println(">>>");

            // Create atomic long.
            GridCacheAtomicLong l = cache.atomicLong("atomicLong", 10, false);

            // Increment atomic long using Memcache client.
            if (client.incr("atomicLong", 5, 0) == 15)
                X.println(">>> Successfully incremented atomic long by 5.");

            // Check that atomic long value is correct.
            X.println(">>> Current atomic long value: " + l.get() + " (expected: 15).");

            // Decrement atomic long using Memcache client.
            if (client.decr("atomicLong", 3, 0) == 12)
                X.println(">>> Successfully decremented atomic long by 2.");

            // Check that atomic long value is correct.
            X.println(">>> Current atomic long value: " + l.get() + " (expected: 12).");
        }
        finally {
            if (client != null)
                client.shutdown();

            G.stop(true);
        }
    }

    /**
     * Creates Memcache client that uses binary protocol and connects to GridGain.
     *
     * @param host Hostname.
     * @param port Port number.
     * @return Client.
     * @throws IOException If connection failed.
     */
    private static MemcachedClient startClient(String host, int port) throws IOException {
        assert host != null;
        assert port > 0;

        return new MemcachedClient(new BinaryConnectionFactory(),
            F.asList(new InetSocketAddress(host, port)));
    }
}
