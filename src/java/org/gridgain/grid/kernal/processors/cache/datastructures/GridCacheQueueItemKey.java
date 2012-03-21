// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.editions.*;

/**
 * Queue item key.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public interface GridCacheQueueItemKey {
    /**
     * Gets queue item id.
     *
     * @return Queue item id.
     */
    public long sequence();

    /**
     * Gets queue id.
     *
     * @return Queue id.
     */
    public String queueId();

    /**
     * Gets item affinity key.
     *
     * @return Item affinity key.
     */
    public String affinityKey();
}
