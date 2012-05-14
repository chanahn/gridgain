// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.zookeeper;

import org.apache.zookeeper.*;
import org.gridgain.grid.spi.*;

/**
 * The primary use of this interface is to be able to instantiate a specially instrumented
 * ZooKeeper client class for testing or benchmarking. By default, the IP Finder creates on instance of
 * the regular ZooKeeper client using {@link GridTcpDiscoveryDefaultZooKeeperFactory} factory.
 */
public interface GridTcpDiscoveryZooKeeperFactory {
    /**
     * Create an instance of ZooKeeper client.
     *
     * @param watcher The IP Finder implementation will provide an instance of the watcher.
     * @return An instance of ZooKeeper client.
     * @throws GridSpiException If ZooKeeper initialization failed. This may happen due to
     *      a malformed connection string, for example.
     * @throws IllegalStateException If factory was not properly initialized.
     */
    public ZooKeeper create(Watcher watcher) throws GridSpiException, IllegalStateException;
}
