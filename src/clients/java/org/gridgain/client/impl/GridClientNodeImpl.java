// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Client node implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridClientNodeImpl implements GridClientNode {
    /** Node id. */
    private UUID nodeId;
    
    /** Internal addresses. */
    private List<String> internalAddrs = Collections.emptyList();

    /** External addresses. */
    private List<String> externalAddrs = Collections.emptyList();

    /** Port for TCP rest binary protocol. */
    private int tcpPort;

    /** Port for HTTP(S) rest binary protocol. */
    private int httpPort;

    /** Node attributes. */
    private Map<String, Object> attrs = Collections.emptyMap();

    /** Node metrics. */
    private Map<String, Object> metrics = Collections.emptyMap();

    /** Node caches. */
    private Map<String, GridClientCacheMode> caches = Collections.emptyMap();

    /** Reference to a list of addresses. */
    private AtomicReference<List<InetSocketAddress>> restAddresses = new AtomicReference<List<InetSocketAddress>>();
    
    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /**
     * Node id to be set.
     *
     * @param nodeId Node id.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public List<String> internalAddresses() {
        return Collections.unmodifiableList(internalAddrs);
    }

    /**
     * Sets list of node internal addresses.
     *
     * @param internalAddrs List of addresses.
     */
    public void internalAddresses(Collection<String> internalAddrs) {
        this.internalAddrs = new ArrayList<String>(internalAddrs);
    }

    /** {@inheritDoc} */
    @Override public List<String> externalAddresses() {
        return Collections.unmodifiableList(externalAddrs);
    }

    /**
     * Sets list of node external addresses.
     *
     * @param externalAddrs List of addresses.
     */
    public void externalAddresses(Collection<String> externalAddrs) {
        this.externalAddrs = new ArrayList<String>(externalAddrs);
    }

    /** {@inheritDoc} */
    @Override public int tcpPort() {
        return tcpPort;
    }

    /**
     * @param tcpPort Sets remote port value.
     */
    public void tcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /** {@inheritDoc} */
    @Override public int httpPort() {
        return httpPort;
    }

    /**
     * Sets remote http port value.
     *
     * @param httpPort Http(s) port value.
     */
    public void httpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return Collections.unmodifiableMap(attrs);
    }

    /**
     * Sets node attributes.
     *
     * @param attrs Node attributes.
     */
    public void attributes(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T attribute(String name) {
        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> metrics() {
        return metrics;
    }

    /**
     * Sets node metrics.
     *
     * @param metrics Metrics.
     */
    public void metrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<String, GridClientCacheMode> caches() {
        return caches;
    }

    /**
     * Sets caches available on remote node.
     *
     * @param caches Cache map.
     */
    public void caches(Map<String, GridClientCacheMode> caches) {
        this.caches = caches;
    }

    /**
     * Gets list of all addresses available for connection for tcp rest binary protocol.
     *
     * @param proto Protocol type.
     * @return List of socket addresses.
     */
    @Override public List<InetSocketAddress> availableAddresses(GridClientProtocol proto) {
        List<InetSocketAddress> res = restAddresses.get();
        
        if (res == null) {
            res = new ArrayList<InetSocketAddress>(internalAddrs.size() + externalAddrs.size());

            int port = proto == GridClientProtocol.TCP ? tcpPort : httpPort;

            if (port != 0) {
                for (String internalAddr : internalAddrs)
                    res.add(new InetSocketAddress(internalAddr, port));

                for (String internalAddr : internalAddrs)
                    res.add(new InetSocketAddress(internalAddr, port));
            }
            
            restAddresses.compareAndSet(null, res);
            
            res = restAddresses.get();
        }
        
        assert res != null;
        
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof GridClientNodeImpl)) return false;

        GridClientNodeImpl that = (GridClientNodeImpl)o;

        return nodeId.equals(that.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GridClientNodeImpl [nodeId=");

        sb.append(nodeId);
        sb.append(", internalAddrs=").append(internalAddrs);
        sb.append(", externalAddrs=").append(externalAddrs);
        sb.append(", binaryPort=").append(tcpPort);
        sb.append(", httpPort=").append(httpPort);
        sb.append(']');

        return sb.toString();
    }
}
