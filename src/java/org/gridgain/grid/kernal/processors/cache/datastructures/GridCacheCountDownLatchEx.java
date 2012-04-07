// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.editions.*;

/**
 * Grid cache count down latch ({@code 'Ex'} stands for external).
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public interface GridCacheCountDownLatchEx extends GridCacheCountDownLatch, GridCacheRemovable {
    /**
     * Get current count down latch key.
     *
     * @return Latch key.
     */
    public GridCacheInternalKey key();

    /**
     * Callback to notify latch on changes.
     *
     * @param count New count.
     */
    public void onUpdate(int count);
}
