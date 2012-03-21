// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import java.util.*;

/**
 * Interface that will determine which node should be connected by the client when
 * operation on a key is requested.
 * <p>
 * If implementation of data affinity implements {@link GridClientTopologyListener} interface as well,
 * then affinity will be added to topology listeners on client start before firs connection is established
 * and will be removed after last connection is closed.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public interface GridClientDataAffinity {
    /**
     * Gets affinity nodes for a key. In case of replicated cache, all returned
     * nodes are updated in the same manner. In case of partitioned cache, the returned
     * list should contain only the primary and back up nodes with primary node being
     * always first.
     *
     * @param key Key to get affinity for.
     * @param nodes Nodes to choose from.
     * @return Affinity nodes for the given partition.
     */
    public GridClientNode node(Object key, Collection<? extends GridClientNode> nodes);
}
