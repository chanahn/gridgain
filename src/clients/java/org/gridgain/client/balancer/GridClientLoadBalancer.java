// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.balancer;

import org.gridgain.client.*;

import java.util.*;

/**
 * Interface that defines a selection logic of a server node for a particular operation
 * (e.g. task run or cache operation in case of pinned mode).
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public interface GridClientLoadBalancer {
    /**
     * Gets next node for executing client command.
     *
     * @param nodes Nodes to pick from.
     * @return Next node to pick.
     * @throws GridServerUnreachableException If none of the nodes given to the balancer can be reached.
     */
    public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes)
        throws GridServerUnreachableException;
}
