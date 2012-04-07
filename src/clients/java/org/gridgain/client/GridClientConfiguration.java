// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.client.balancer.*;
import org.gridgain.client.ssl.*;

import java.util.*;

/**
 * Java client configuration.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public interface GridClientConfiguration {
    /** Default client protocol. */
    public static final GridClientProtocol DFLT_CLIENT_PROTOCOL = GridClientProtocol.TCP;

    /** Default topology refresh frequency is 2 sec. */
    public static final int DFLT_TOP_REFRESH_FREQ = 2000;

    /** Default maximum time connection can be idle. */
    public static final long DFLT_MAX_CONN_IDLE_TIME = 30000;

    /** Default flag setting for TCP_NODELAY option. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /**
     * Collection of {@code 'host:port'} pairs representing
     * remote grid servers used to establish initial connection to
     * the grid. Once connection is established, GridGain will get
     * a full view on grid topology and will be able to connect to
     * any available remote node.
     *
     * @return Collection of {@code 'host:port'} pairs representing remote
     *      grid servers.
     */
    public Collection<String> getServers();

    /**
     * Gets protocol for communication between client and remote grid.
     *
     * @return Protocol for communication between client and remote grid.
     */
    public GridClientProtocol getProtocol();

    /**
     * Gets timeout for socket connect operation.
     *
     * @return Socket connect timeout in milliseconds.
     */
    public int getConnectTimeout();

    /**
     * Gets flag indicating whether {@code TCP_NODELAY} flag should be enabled for outgoing connections.
     * This flag reduces communication latency and in the majority of cases should be set to true. For more
     * information, see {@link java.net.Socket#setTcpNoDelay(boolean)}
     * <p/>
     * If not set, default value is {@link #DFLT_TCP_NODELAY}
     *
     * @return If {@code TCP_NODELAY} should be set on underlying sockets.
     */
    public boolean isTcpNoDelay();

    /**
     * Flag indicating whether client should try to connect server with secure
     * socket layer enabled (regardless of protocol used).
     *
     * @return {@code True} if SSL should be enabled.
     */
    public boolean isSslEnabled();

    /**
     * Gets a factory that should be used for SSL context creation if SSL is enabled.
     *
     * @return Factory instance.
     * @see GridSslContextFactory
     * @see #isSslEnabled()
     */
    public GridSslContextFactory getSslContextFactory();

    /**
     * Default balancer to be used for computational client. It can be overridden
     * for different compute instances.
     *
     * @return Default balancer to be used for computational client.
     */
    public GridClientLoadBalancer getBalancer();

    /**
     * Gets a collection of data configurations specified by user.
     *
     * @return Collection of data configurations (possibly empty).
     */
    public Collection<GridClientDataConfiguration> getDataConfigurations();

    /**
     * Gets client credentials to authenticate with.
     *
     * @return Credentials object (possibly {@code null})
     */
    public Object getCredentials();

    /**
     * Enables client to cache topology internally, so it does not have to
     * be always refreshed. Topology cache will be automatically refreshed
     * in the background every {@link #getTopologyRefreshFrequency()} interval.
     *
     * @return {@code True} if topology cache is enabled, {@code false} otherwise.
     */
    public boolean isEnableTopologyCache();

    /**
     * Gets topology refresh frequency.
     *
     * @return Topology refresh frequency.
     */
    public long getTopologyRefreshFrequency();

    /**
     * Gets maximum amount of time that client connection can be idle before it is closed.
     *
     * @return Maximum idle time in milliseconds.
     */
    public long getMaxConnectionIdleTime();
}
