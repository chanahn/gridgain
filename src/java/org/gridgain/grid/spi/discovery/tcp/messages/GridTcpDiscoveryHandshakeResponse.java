// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.messages;

import org.gridgain.grid.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Handshake response.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.1c.21112011
 */
public class GridTcpDiscoveryHandshakeResponse extends GridTcpDiscoveryAbstractMessage {
    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param locNodeOrder Local node order.
     */
    public GridTcpDiscoveryHandshakeResponse(UUID creatorNodeId, long locNodeOrder) {
        super(creatorNodeId);

        topologyVersion(locNodeOrder);
    }

    /**
     * Gets order of the node sent the response.
     *
     * @return Order of the node sent the response.
     */
    @Override public long topologyVersion() {
        return super.topologyVersion();
    }

    /**
     * Sets order of the node sent the response.
     *
     * @param topVer Order of the node sent the response.
     */
    @Override public void topologyVersion(long topVer) {
        super.topologyVersion(topVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryHandshakeResponse.class, this, "super", super.toString());
    }
}
