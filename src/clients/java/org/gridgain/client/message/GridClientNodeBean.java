// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.message;

import java.io.*;
import java.util.*;

/**
 * Node bean.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridClientNodeBean implements Serializable {
    /** Node ID */
    private String nodeId;

    /** Internal addresses. */
    private Collection<String> intAddrs;

    /** External addresses. */
    private Collection<String> extAddrs;

    /** Rest binary port. */
    private int tcpPort;

    /** Rest HTTP(S) port. */
    private int jettyPort;

    /** Metrics. */
    private GridClientNodeMetricsBean metrics;

    /** Node attributes. */
    private Map<String, Object> attrs;

    /** Mode for cache with {@code null} name. */
    private String dfltCacheMode;
    
    /** Node caches. */
    private Map<String, String> caches;

    /**
     * Gets node ID.
     *
     * @return Node Id.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Sets node ID.
     *
     * @param nodeId Node ID.
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Gets internal addresses.
     *
     * @return Internal addresses.
     */
    public Collection<String> getInternalAddresses() {
        return intAddrs;
    }

    /**
     * Sets internal addresses.
     *
     * @param intAddrs Internal addresses.
     */
    public void setInternalAddresses(Collection<String> intAddrs) {
        this.intAddrs = intAddrs;
    }

    /**
     * Gets external addresses.
     *
     * @return External addresses.
     */
    public Collection<String> getExternalAddresses() {
        return extAddrs;
    }

    /**
     * Sets external addresses.
     *
     * @param extAddrs External addresses.
     */
    public void setExternalAddresses(Collection<String> extAddrs) {
        this.extAddrs = extAddrs;
    }

    /**
     * Gets metrics.
     *
     * @return Metrics.
     */
    public GridClientNodeMetricsBean getMetrics() {
        return metrics;
    }

    /**
     * Sets metrics.
     *
     * @param metrics Metrics.
     */
    public void setMetrics(GridClientNodeMetricsBean metrics) {
        this.metrics = metrics;
    }

    /**
     * Gets attributes.
     *
     * @return Attributes.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }

    /**
     * Sets attributes.
     *
     * @param attrs Attributes.
     */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /**
     * Gets REST binary protocol port.
     *
     * @return Port on which REST binary protocol is bound.
     */
    public int getTcpPort() {
        return tcpPort;
    }

    /**
     * Gets REST http protocol port.
     *
     * @return Http port.
     */
    public int getJettyPort() {
        return jettyPort;
    }

    /**
     * Sets REST http port.
     *
     * @param jettyPort REST http port.
     */
    public void setJettyPort(int jettyPort) {
        this.jettyPort = jettyPort;
    }
    
    /**
     * Gets configured node caches.
     *
     * @return Map where key is cache name and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").
     */
    public Map<String, String> getCaches() {
        return caches;
    }

    /**
     * Sets configured node caches.
     *
     * @param caches Map where key is cache name and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").
     */
    public void setCaches(Map<String, String> caches) {
        this.caches = caches;
    }

    /**
     * Gets mode for cache with null name.
     *
     * @return Default cache mode.
     */
    public String getDefaultCacheMode() {
        return dfltCacheMode;
    }

    /**
     * Sets mode for default cache.
     *
     * @param dfltCacheMode Default cache mode.
     */
    public void setDefaultCacheMode(String dfltCacheMode) {
        this.dfltCacheMode = dfltCacheMode;
    }

    /**
     * Sets REST binary protocol port.
     *
     * @param tcpPort Port on which REST binary protocol is bound.
     */
    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeId != null ? nodeId.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        GridClientNodeBean other = (GridClientNodeBean)obj;

        return nodeId == null ? other.nodeId == null : nodeId.equals(other.nodeId);
    }

    @Override public String toString() {
        return new StringBuilder().
            append("GridClientNodeBean [id=").
            append(nodeId).
            append("]").
            toString();
    }
}
