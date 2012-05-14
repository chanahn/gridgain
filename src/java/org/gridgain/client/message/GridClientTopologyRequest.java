// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.message;

/**
 * {@code Topology} command request.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridClientTopologyRequest extends GridClientAbstractMessage {
    /** Id of requested node. */
    private String nodeId;

    /** IP address of requested node. */
    private String nodeIp;

    /** Include metrics flag. */
    private boolean includeMetrics;

    /** Include node attributes flag. */
    private boolean includeAttrs;

    /**
     * @return Include metrics flag.
     */
    public boolean includeMetrics() {
        return includeMetrics;
    }

    /**
     * @param includeMetrics Include metrics flag.
     */
    public void includeMetrics(boolean includeMetrics) {
        this.includeMetrics = includeMetrics;
    }

    /**
     * @return Include node attributes flag.
     */
    public boolean includeAttributes() {
        return includeAttrs;
    }

    /**
     * @param includeAttrs Include node attributes flag.
     */
    public void includeAttributes(boolean includeAttrs) {
        this.includeAttrs = includeAttrs;
    }

    /**
     * @return Node identifier, if specified, {@code null} otherwise.
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node identifier to lookup.
     */
    public void nodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Node ip address if specified, {@code null} otherwise.
     */
    public String nodeIp() {
        return nodeIp;
    }

    /**
     * @param nodeIp Node ip address to lookup.
     */
    public void nodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridClientTopologyRequest other = (GridClientTopologyRequest)o;

        return includeAttrs == other.includeAttrs &&
            includeMetrics == other.includeMetrics;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (includeMetrics ? 1 : 0) +
            (includeAttrs ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new StringBuilder().
            append("GridClientTopologyRequest [includeMetrics=").
            append(includeMetrics).
            append(", includeAttrs=").
            append(includeAttrs).
            append("]").
            toString();
    }
}
