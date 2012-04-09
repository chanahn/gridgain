// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

/**
 * Listener interface for notifying on nodes joining or leaving remote grid.
 * <p>
 * Since the topology refresh is performed in background, the listeners will not be notified
 * immediately after the node leaves grid, but the maximum time window between remote grid detects
 * node leaving and client receives topology update is {@link GridClientConfiguration#getTopologyRefreshFrequency()}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public interface GridClientTopologyListener {
    /**
     * Callback for new nodes joining the remote grid.
     *
     * @param node New remote node.
     */
    public void onNodeAdded(GridClientNode node);

    /**
     * Callback for nodes leaving the remote grid.
     *
     * @param node Left node.
     */
    public void onNodeRemoved(GridClientNode node);
}
