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
import java.util.concurrent.locks.*;

/**
 * Simple balancer that implements round-robin balancing.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridClientRoundRobinBalancer implements GridClientLoadBalancer, GridClientTopologyListener {
    /** Lock. */
    private Lock lock = new ReentrantLock();

    /** Nodes to share load. */
    private LinkedList<UUID> nodeQueue = new LinkedList<UUID>();
    
    /** {@inheritDoc} */
    @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes)
        throws GridServerUnreachableException {
        Map<UUID, GridClientNode> lookup = new HashMap<UUID, GridClientNode>(nodes.size());

        for (GridClientNode node : nodes)
            lookup.put(node.nodeId(), node);

        lock.lock();

        try {
            GridClientNode balanced = null;

            for (Iterator<UUID> iter = nodeQueue.iterator(); iter.hasNext();) {
                UUID nodeId = iter.next();

                balanced = lookup.get(nodeId);

                if (balanced != null) {
                    iter.remove();

                    break;
                }
            }

            if (balanced != null) {
                nodeQueue.addLast(balanced.nodeId());

                return balanced;
            }

            throw new GridServerUnreachableException("Failed to get balanced node (topology does not have alive " +
                "nodes): " + nodes);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeAdded(GridClientNode node) {
        lock.lock();

        try {
            nodeQueue.addFirst(node.nodeId());
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeRemoved(GridClientNode node) {
        lock.lock();
        
        try {
            nodeQueue.remove(node.nodeId());
        }
        finally {
            lock.unlock();
        }
    }
}
