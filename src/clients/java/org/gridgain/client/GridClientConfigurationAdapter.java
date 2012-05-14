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
 * Client configuration adapter.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridClientConfigurationAdapter implements GridClientConfiguration {
    /** List of servers to connect to. */
    private Collection<String> srvs;

    /** Client protocol. */
    private GridClientProtocol proto = DFLT_CLIENT_PROTOCOL;

    /** Socket connect timeout. */
    private int connectTimeout;

    /** TCP_NODELAY flag. */
    private boolean tcpNoDelay = DFLT_TCP_NODELAY;

    /** Flag indicating whether ssl is enabled. */
    private boolean sslEnabled;

    /** SSL context factory  */
    private GridSslContextFactory sslCtxFactory;

    /** Flag indicating whether topology cache is enabled. */
    private boolean enableTopCache;

    /** Topology refresh  frequency. */
    private long topRefreshFreq = DFLT_TOP_REFRESH_FREQ;

    /** Max time of connection idleness. */
    private long maxConnIdleTime = DFLT_MAX_CONN_IDLE_TIME;

    /** Default balancer. */
    private GridClientLoadBalancer balancer = new GridClientRandomBalancer();

    /** Collection of data configurations. */
    private Map<String, GridClientDataConfiguration> dataCfgs = Collections.emptyMap();

    /** Credentials. */
    private Object cred;

    /**
     * Creates default configuration.
     */
    public GridClientConfigurationAdapter() {
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to be copied.
     */
    public GridClientConfigurationAdapter(GridClientConfiguration cfg) {
        // Preserve alphabetical order for maintenance;
        balancer = cfg.getBalancer();
        connectTimeout = cfg.getConnectTimeout();
        cred = cfg.getCredentials();
        enableTopCache = cfg.isEnableTopologyCache();
        maxConnIdleTime = cfg.getMaxConnectionIdleTime();
        proto = cfg.getProtocol();
        srvs = cfg.getServers();
        sslCtxFactory = cfg.getSslContextFactory();
        sslEnabled  = cfg.isSslEnabled();
        tcpNoDelay = cfg.isTcpNoDelay();
        topRefreshFreq = cfg.getTopologyRefreshFrequency();

        setDataConfigurations(cfg.getDataConfigurations());
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getServers() {
        return Collections.unmodifiableCollection(srvs);
    }

    /**
     * Sets list of servers this client should connect to.
     *
     * @param srvs List of servers.
     */
    public void setServers(Collection<String> srvs) {
        this.srvs = srvs;
    }

    /** {@inheritDoc} */
    @Override public GridClientProtocol getProtocol() {
        return proto;
    }

    /**
     * Sets protocol type that should be used in communication. Protocol type cannot be changed after
     * client is created.
     *
     * @param proto Protocol type.
     * @see GridClientProtocol
     */
    public void setProtocol(GridClientProtocol proto) {
        this.proto = proto;
    }

    /** {@inheritDoc} */
    @Override public int getConnectTimeout() {
        return connectTimeout;
    }

    /** {@inheritDoc} */
    @Override public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Sets whether {@code TCP_NODELAY} flag should be set on underlying socket connections.
     *
     * @param tcpNoDelay {@code True} if flag should be set.
     */
    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * Sets timeout for socket connect operation.
     *
     * @param connectTimeout Connect timeout in milliseconds.
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /** {@inheritDoc} */
    @Override public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * Sets whether ssl should be enabled or not. If this flag set to {@code true},
     * an instance of {@link GridSslContextFactory} must be provided in configuration.
     *
     * @param sslEnabled {@code True} if SSL should be enabled.
     */
    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    /** {@inheritDoc} */
    @Override public GridSslContextFactory getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * Sets SSL context factory that will be used for creation of secure connections.
     *
     * @param sslCtxFactory Context factory.
     */
    public void setSslContextFactory(GridSslContextFactory sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;
    }

    /** {@inheritDoc} */
    @Override public GridClientLoadBalancer getBalancer() {
        return balancer;
    }

    /**
     * Sets default compute balancer.
     *
     * @param balancer Balancer to use.
     */
    public void setBalancer(GridClientLoadBalancer balancer) {
        this.balancer = balancer;
    }

    /** {@inheritDoc} */
    @Override public Object getCredentials() {
        return cred;
    }

    /**
     * Sets client credentials object used in authentication process.
     *
     * @param cred Client credentials.
     */
    public void setCredentials(Object cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientDataConfiguration> getDataConfigurations() {
        return dataCfgs.values();
    }

    /**
     * Sets data configurations.
     *
     * @param dataCfgs Data configurations.
     */
    public void setDataConfigurations(Collection<? extends GridClientDataConfiguration> dataCfgs) {
        this.dataCfgs = new HashMap<String, GridClientDataConfiguration>(dataCfgs.size());

        for (GridClientDataConfiguration dataCfg : dataCfgs)
            this.dataCfgs.put(dataCfg.getName(), new GridClientDataConfigurationAdapter(dataCfg));
    }

    /**
     * Gets data configuration for a cache with specified name.
     *
     * @param name Name of grid cache.
     * @return Configuration or {@code null} if there is not configuration for specified name.
     */
    public GridClientDataConfiguration getDataConfiguration(String name) {
        return dataCfgs.get(name);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnableTopologyCache() {
        return enableTopCache;
    }

    /**
     * Sets flag indicating whether topology should be cached by client.
     *
     * @param enableTopCache {@code True} if cache should be enabled.
     */
    public void setEnableTopologyCache(boolean enableTopCache) {
        this.enableTopCache = enableTopCache;
    }

    /** {@inheritDoc} */
    @Override public long getTopologyRefreshFrequency() {
        return topRefreshFreq;
    }

    /**
     * Sets topology refresh frequency. If topology cache is enabled, grid topology
     * will be refreshed every {@code topRefreshFreq} milliseconds.
     *
     * @param topRefreshFreq Topology refresh frequency in milliseconds.
     */
    public void setTopologyRefreshFrequency(long topRefreshFreq) {
        this.topRefreshFreq = topRefreshFreq;
    }

    /** {@inheritDoc} */
    @Override public long getMaxConnectionIdleTime() {
        return maxConnIdleTime;
    }

    /**
     * Sets maximum time in milliseconds which connection can be idle before it is closed by client.
     *
     * @param maxConnIdleTime Maximum time of connection idleness in milliseconds.
     */
    public void setMaxConnectionIdleTime(long maxConnIdleTime) {
        this.maxConnIdleTime = maxConnIdleTime;
    }
}
