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
import org.gridgain.grid.typedef.internal.*;

import java.io.*;

/**
 * The default ZooKeeper factory simply creates a new instance of ZooKeeper handle
 * every time its {@link #create(Watcher) } method is called.
 *
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *     <li>ZooKeeper connect string (see {@link #setConnectString(String)}).</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>ZooKeeper session timeout (see {@link #setSessionTimeout(int)})</li>
 * </ul>
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridTcpDiscoveryDefaultZooKeeperFactory implements GridTcpDiscoveryZooKeeperFactory {
    /** Default ZooKeeper client session timeout. */
    public static final int DFLT_SESSION_TIMEOUT = 3000;

    /** ZooKeeper connect string. */
    private String connStr;

    /** ZooKeeper session timeout. */
    private int sesTimeout = DFLT_SESSION_TIMEOUT;

    /**
     * Default constructor.
     */
    public GridTcpDiscoveryDefaultZooKeeperFactory() {
        // No-op.
    }

    /**
     * Non-default constructor.
     *
     * @param connStr ZooKeeper connection string.
     * @param sesTimeout ZooKeeper session timeout.
     */
    public GridTcpDiscoveryDefaultZooKeeperFactory(String connStr, int sesTimeout) {
        A.notNull(connStr, "connStr");
        A.ensure(sesTimeout > 0, "sesTimeout > 0");

        this.connStr = connStr;
        this.sesTimeout = sesTimeout;
    }

    /** {@inheritDoc} */
    @Override public ZooKeeper create(Watcher watcher) throws GridSpiException {
        // Validate the mandatory parameter is set.
        if (connStr == null)
            throw new IllegalStateException("ZooKeeper 'connectString' cannot be null.");

        try {
            return new ZooKeeper(connStr, sesTimeout, watcher);
        }
        catch (IOException e) {
            // This may happen due to a malformed connection string, for example.
            throw new IllegalStateException("Unable to initialize ZooKeeper connection [connStr=" + connStr +
                ", sesTimeout=" + sesTimeout + ']', e);
        }
    }

    /**
     * Specifies the ZooKeeper connect string. The value of this property will
     * be without any changes passed to the ZooKeeper client at initialization time.
     *
     * @param connStr Comma-separated list of host:port pairs optionally
     * followed by the chroot path.
     */
    @GridSpiConfiguration(optional = false)
    public void setConnectString(String connStr) {
        A.notNull(connStr, "connectString");

        this.connStr = connStr;
    }

    /**
     * Sets the ZooKeeper client session timeout. The value of this property
     * will be without any changes passed to the ZooKeeper client at initialization time.
     *
     * @param sesTimeout Time in milliseconds.
     */
    @GridSpiConfiguration(optional = true)
    public void setSessionTimeout(int sesTimeout) {
        A.ensure(sesTimeout > 0, "sessionTimeout > 0");

        this.sesTimeout = sesTimeout;
    }
}
