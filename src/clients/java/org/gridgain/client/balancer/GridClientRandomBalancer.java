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
 * Simple balancer that relies on random node selection from a given collection. This implementation
 * has no any caches and treats each given collection as a new one.
 * <p>
 * More strictly, for any non-empty collection of size <tt>n</tt> the probability of selection of any
 * node in this collection will be <tt>1/n</tt>.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridClientRandomBalancer implements GridClientLoadBalancer {
    /** Random for node selection. */
    private Random random = new Random();

    /**
     * Picks up a random node from a collection.
     *
     * @param nodes Nodes to pick from.
     * @return Random node from collection.
     */
    @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes) {
        int size = nodes.size();

        if (size == 0)
            throw new IllegalArgumentException("Failed to pick up balanced node (collection of nodes is empty)");
        
        int idx = random.nextInt(size);

        if (nodes instanceof List)
            return ((List<GridClientNode>)nodes).get(idx);
        else {
            Iterator<? extends GridClientNode> it = nodes.iterator();

            while (idx > 0) {
                it.next();

                idx--;
            }

            return it.next();
        }
    }
}
