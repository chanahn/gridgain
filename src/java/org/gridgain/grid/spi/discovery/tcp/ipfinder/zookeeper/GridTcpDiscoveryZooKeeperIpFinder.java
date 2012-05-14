// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder.zookeeper;

import org.apache.zookeeper.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.Collections.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.Watcher.Event.EventType.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

/**
 * IP finder that uses Apache ZooKeeper for sharing node's IP end-point details.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>ZooKeeper factory (see {@link #setZooKeeperFactory})</li>
 *      <li>ZooKeeper connection timeout (see {@link #setConnectionTimeout(int)})</li>
 *      <li>Rendezvous point path (see {@link #setRendezvousPointPath})</li>
 * </ul>
 * The finder maintains the node IP addresses as ephemeral children of the top level
 * node whose path is given by the {@link #setRendezvousPointPath} parameter. The node default
 * name is {@link #DEFAULT_RENDEZVOUS_PATH}.
 * <p>
 * The finder will survive ZooKeeper disconnects and session expirations,
 * although the newly launched GridGain nodes won't be able to join the topology while
 * the ZooKeeper connection is down. However it should not be necessary to restart the already
 * running nodes when ZooKeeper connection is restored.
 * <p>
 * By specifying different rendezvous paths via {@link #setRendezvousPointPath} it must be
 * possible to run multiple disjoint GridGain topologies against the same instance
 * of ZooKeeper.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridTcpDiscoveryZooKeeperIpFinder extends GridTcpDiscoveryIpFinderAdapter {
    /** How long the Finder will wait to connect the Zookeeper */
    public static final int CONNECTION_TIMEOUT = 10000;

    /** Default top level Zookeeper znode name, which serves as a rendezvous point. */
    public static final String DEFAULT_RENDEZVOUS_PATH = "/GridGainIpFinder";

    /** Delimiter to use between address and port tokens in the child name. */
    public static final String DELIM = "#";

    /** ZooKeeper connection timeout. */
    private int connTimeout = CONNECTION_TIMEOUT;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * The actual Zookeeper path to the znode that serves as a rendezvous point for
     * nodes in the same topology.
     */
    private String rvPath = DEFAULT_RENDEZVOUS_PATH;

    /** Grid logger. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    @GridLoggerResource
    private GridLogger log;

    /** ZooKeeper connection handler */
    private volatile ConnectionStateHandler zkHnd;

    /** The ZooKeeper client factory */
    private GridTcpDiscoveryZooKeeperFactory zkFactory = new GridTcpDiscoveryDefaultZooKeeperFactory();

    /**
     * Default constructor.
     */
    public GridTcpDiscoveryZooKeeperIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws GridSpiException {
        ZooKeeper zk = initiateConnectAndWait();

        return doGetRegisteredAddresses(zk);
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws GridSpiException {
        ZooKeeper zk = initiateConnectAndWait();

        doRegisterAddresses(zk, addrs);
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws GridSpiException {
        ZooKeeper zk = initiateConnectAndWait();

        doUnregisterAddresses(zk, addrs);
    }

    /**
     * Sets the rendezvous point path. Only nodes with the same rendezvous path are
     * able to see each other. This property makes it possible to configure multiple
     * independent grids on a single ZooKeeper instance.
     *
     * @param rvPath Rendezvous point path.
     */
    @GridSpiConfiguration(optional = true)
    public void setRendezvousPointPath(String rvPath) {
        A.notNull(rvPath, "rendezvousPointPath");

        this.rvPath = rvPath;
    }

    /**
     * Sets the ZooKeeper connection timeout: how long the initial connect wait
     * for ZooKeeper's connection handshake.
     *
     * @param connTimeout Time in milliseconds.
     */
    @GridSpiConfiguration(optional = true)
    public void setConnectionTimeout(int connTimeout) {
        A.ensure(connTimeout > 0, "connectionTimeout > 0");

        this.connTimeout = connTimeout;
    }

    /**
     * Sets an instance of the ZooKeeper factory. The finder uses the factory to create
     * an instance of ZooKeeper client. By default, the finder uses {@link GridTcpDiscoveryDefaultZooKeeperFactory}.
     *
     * @param zkFactory ZooKeeper client factory instance.
     */
    @GridSpiConfiguration(optional = true)
    public void setZooKeeperFactory(GridTcpDiscoveryZooKeeperFactory zkFactory) {
        A.notNull(zkFactory, "zooKeeperFactory");

        this.zkFactory = zkFactory;
    }

    /**
     * Builds the list of registered addresses that according to ZooKeeper are still alive.
     *
     * @return list of internet addresses of the nodes in the group.
     * @throws GridSpiException If failed.
     */
    private Collection<InetSocketAddress> doGetRegisteredAddresses(ZooKeeper zk) throws GridSpiException {
        if (log.isDebugEnabled())
            log.debug("Requesting the list of registered addresses.");

        List<String> children = children(zk);

        try {
            Collection<InetSocketAddress> res = unmodifiableCollection(new ArrayList<InetSocketAddress>(
                F.transform(children, new GridClosure<String, InetSocketAddress>() {
                    @Override public InetSocketAddress apply(String s) {
                        return toInetSocketAddress(s);
                    }
                })));

            if (log.isDebugEnabled())
                log.debug("Successfully retrieved a list of registered addresses.");

            return res;
        }
        catch (IllegalArgumentException e) {
            throw new GridSpiException("Failed to parse the address entries.", e);
        }
    }

    /**
     * Retrieves the current list of children of the parent znode.
     *
     * @return List of children.
     * @throws GridSpiException If a ZooKeeper error occurs.
     */
    private List<String> children(ZooKeeper zk) throws GridSpiException {
        try {
            return zk.getChildren(rvPath, null);
        }
        catch (KeeperException e) {
            throw new GridSpiException("Failed to obtain the list of registered addresses from ZooKeeper path: " +
                rvPath, e);
        }
        catch (InterruptedException e) {
            throw new GridSpiException("ZooKeeper operation interrupted.", e);
        }
    }

    /**
     * Unregisters a list of inet addresses with the given ZooKeeper instance.
     *
     * @param zk ZooKeeper instance.
     * @param addrs The list of addresses to unregister.
     * @throws GridSpiException If failed.
     */
    private void doUnregisterAddresses(ZooKeeper zk, Iterable<InetSocketAddress> addrs) throws GridSpiException {
        if (log.isDebugEnabled())
            log.debug("Unregistering a list of node addresses: " + addrs);

        try {
            for (InetSocketAddress addr : addrs) {
                try {
                    zk.delete(rvPath + '/' + toZnodeName(addr), -1);
                }
                catch (KeeperException.NoNodeException ignored) {
                    // This node is already gone. This is ok.
                    if (log.isDebugEnabled())
                        log.debug("Entry not found in ZooKeeper: " + addr);
                }
                catch (KeeperException e) {
                    throw new GridSpiException("Failed to unregister a node address with ZooKeeper: " + addr, e);
                }
            }

            if (log.isDebugEnabled())
                log.debug("Successfully unregistered a list of node addresses: " + addrs);
        }
        catch (InterruptedException e) {
            throw new GridSpiException("ZooKeeper operation interrupted.", e);
        }
    }

    /**
     * Registers a list of inet addresses with the given ZooKeeper instance.
     *
     * @param zk ZooKeeper instance.
     * @param addrs The list of inet addresses to register.
     * @throws GridSpiException If failed.
     */
    private void doRegisterAddresses(ZooKeeper zk, Iterable<InetSocketAddress> addrs) throws GridSpiException {
        if (log.isDebugEnabled())
            log.debug("Registering a list of node addresses: " + addrs);

        try {
            for (InetSocketAddress addr : addrs) {
                try {
                    zk.create(rvPath + '/' + toZnodeName(addr), null, OPEN_ACL_UNSAFE, EPHEMERAL);
                }
                catch (KeeperException.NodeExistsException ignored) {
                    // This node already exists. This is ok.
                    if (log.isDebugEnabled())
                        log.debug("Entry not found in ZooKeeper: " + addr);
                }
                catch (KeeperException e) {
                    throw new GridSpiException("Failed to register a node address with ZooKeeper: " + addr, e);
                }
            }

            if (log.isDebugEnabled())
                log.debug("Successfully registered a list of node addresses.");
        }
        catch (InterruptedException e) {
            throw new GridSpiException("ZooKeeper operation interrupted.", e);
        }
    }

    /**
     * Coverts the znode name to the internet address.
     *
     * @param znodeName name of the ZooKeeper znode.
     * @return internet socket address corresponding to the given znode.
     * @throws IllegalArgumentException If the znode has a wrong format.
     */
    private static InetSocketAddress toInetSocketAddress(String znodeName) {
        String[] tkns = znodeName.split(DELIM);

        if (tkns.length != 2)
            throw new IllegalArgumentException("Malformed ZooKeeper znode name: " + znodeName);

        return new InetSocketAddress(tkns[0], Integer.parseInt(tkns[1]));
    }

    /**
     * Coverts the internet address to the znode name.
     *
     * @param addr socket address
     * @return a string that represents the given socket address.
     */
    private static String toZnodeName(InetSocketAddress addr) {
        assert addr != null;

        SB sb = new SB();

        sb.a(addr.getAddress().getHostAddress()).a(DELIM).a(addr.getPort());

        return sb.toString();
    }

    /**
     * Launches the ZooKeeper connection protocol. Blocks until connection is established
     * or an error has occurred.
     *
     * @throws GridSpiException If unable to establish connection.
     */
    private ZooKeeper initiateConnectAndWait() throws GridSpiException {
        synchronized (mux) {
            ConnectionStateHandler zkHnd = this.zkHnd;

            if (zkHnd == null) {
                if (log.isDebugEnabled())
                    log.debug("Initializing ZooKeeper connection.");

                zkHnd = this.zkHnd = new ConnectionStateHandler();

                zkHnd.connect();

                if (log.isDebugEnabled())
                    log.debug("Successfully connected to ZooKeeper.");
            }

            return zkHnd.zookeeper();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ConnectionStateHandler hnd = zkHnd;

        if (hnd != null)
            hnd.closeZooKeeper();
    }

    /**
     * ZooKeeper connection handler. Monitors the connection status and ZooKeeper session expiration events.
     */
    private class ConnectionStateHandler implements Watcher {
        /** ZooKeeper handle. */
        private volatile ZooKeeper zk;

        /** Flag indicating that this session has been expired by the ZooKeeper server. */
        private volatile boolean sesExpired;

        /** TThis is for synchronization between the ZooKeeper event thread and the finder caller thread. */
        private volatile CountDownLatch latch;

        /**
         * Zookeeper watcher callback is called on the Zookeeper's event thread.
         * This callback handles connection related events only.
         *
         * @param evt Zookeeper event.
         */
        @Override public void process(WatchedEvent evt) {
            if (log.isDebugEnabled())
                log.debug("Received ZooKeeper event: " + evt);

            try {
                handle(evt);
            }
            catch (InterruptedException ignored) {
                // Swallow the interrupt: the ZK event thread must not be interrupted.
                log.warning("ZooKeeper event thread is interrupted.");
            }
        }

        /**
         * @return ZooKeeper.
         */
        ZooKeeper zookeeper() {
            return zk;
        }

        /**
         * This method is called on ZooKeeper's event thread.
         *
         * @param evt ZooKeeper event.
         * @throws InterruptedException
         */
        void handle(WatchedEvent evt) throws InterruptedException {
            if (evt.getType() == None) {
                // ZooKeeper session-related events.
                switch (evt.getState()) {
                    case SyncConnected:
                        if (log.isDebugEnabled())
                            log.debug("ZooKeeper server has acknowledged the connection.");

                        try {
                            ensureParentNode();
                        }
                        catch (KeeperException.NoNodeException e) {
                            // Special case this exception: this most likely is caused by the use of chroot which
                            // has not been created yet. There is no way to recover from this: this node must be
                            // created by the ZooKeeper administrator.
                            closeZooKeeper();

                            log.error("Unable to create the rendezvous node. Most likely caused by a missing chroot " +
                                "directory.", e);
                        }
                        catch (KeeperException e) {
                            log.error("ZooKeeper error occurred after a reconnect. The finder will attempt to " +
                                "recover.", e);
                        }

                        break;

                    case Disconnected:
                        if (log.isDebugEnabled())
                            log.debug("ZooKeeper server connection loss detected.");

                        break;

                    case Expired:
                        if (log.isDebugEnabled())
                            log.debug("ZooKeeper server indicated session expiration.");

                        sesExpired = true;

                        // Reset the finder to the initial state.
                        closeZooKeeper();

                        zkHnd = null;

                        break;

                    default:
                        // Ignore unexpected state.
                }

                // Let the waiting connect thread continue.
                latch.countDown();
            }
        }

        /**
         * Creates the top level parent znode. Its ephemeral children will represent
         * the actual GridGain nodes.
         *
         * @throws KeeperException If unable to create the parent node due to a ZooKeeper failure.
         * @throws InterruptedException If thread was interrupted.
         */
        private void ensureParentNode() throws InterruptedException, KeeperException {
            try {
                zk.create(rvPath, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                if (log.isDebugEnabled())
                    log.debug("Successfully created the group znode: " + rvPath);
            }
            catch (KeeperException.NodeExistsException ignored) {
                // It's ok if the node already exists. We get this exception if another
                // node beat us to it.
                if (log.isDebugEnabled())
                    log.debug("Parent znode already exists: " + rvPath);
            }
        }

        /**
         * Synchronously connects to Zookeeper.
         *
         * @throws GridSpiException If unable to connect due to a malformed connection string,
         * or timed out waiting for connection establishment, or the thread got interrupted.
         */
        void connect() throws GridSpiException {
            while (true) {
                sesExpired = false;

                latch = new CountDownLatch(1);

                zk = zkFactory.create(this);

                try {
                    boolean connected = latch.await(connTimeout, TimeUnit.MILLISECONDS);

                    // Possible outcomes are:
                    // 1. ZK session expired.
                    if (sesExpired) {
                        if (log.isDebugEnabled())
                            log.debug("ZooKeeper session has expired, retrying...");

                        // Go back and attempt to connect again.
                        continue;
                    }

                    // 2. Timed out waiting for connect.
                    if (!connected) {
                        // ZooKeeper client continues to re-connect in background.
                        throw new GridSpiException("Failed to connect due to timeout: " + connTimeout);
                    }

                    // 3. Connected successfully.
                    break;
                }
                catch (InterruptedException e) {
                    throw new GridSpiException("Connect request was interrupted.", e);
                }
            }
        }

        /**
         * Closes the ZooKeeper handle.
         */
        void closeZooKeeper() {
            if (log.isDebugEnabled())
                log.debug("Closing ZooKeeper handle.");

            try {
                if (zk != null)
                    zk.close();
            }
            catch (InterruptedException ignored) {
                // In fact, ZooKeeper client never throws any exception from its close() method!
            }
        }
    }
}
