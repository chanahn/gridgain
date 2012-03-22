// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import java.net.*;
import java.util.*;

/**
 * Node descriptor.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public interface GridClientNode {
    /**
     * @return Node ID.
     */
    public UUID nodeId();

    /**
     * @return Internal addresses.
     */
    public List<String> internalAddresses();

    /**
     * @return External addresses.
     */
    public List<String> externalAddresses();

    /**
     * @return Remote tcp port.
     */
    public int tcpPort();

    /**
     * @return Remote http port.
     */
    public int httpPort();

    /**
     * @return Node attributes.
     */
    public Map<String, Object> attributes();

    /**
     * @param name Attribute name.
     * @return Attribute value.
     */
    public <T> T attribute(String name);

    /**
     * @return Metrics.
     */
    public Map<String, Object> metrics();

    /**
     * Gets all configured caches and their types on remote node.
     *
     * @return Map in which key is a configured cache name on the node, value is mode of configured cache.
     */
    public Map<String, GridClientCacheMode> caches();

    /**
     * Gets list of addresses on which REST binary protocol is bound.
     *
     * @param proto Protocol for which addresses are obtained.
     * @return List of addresses.
     */
    List<InetSocketAddress> availableAddresses(GridClientProtocol proto);
}
