// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Adds affinity node ID to cache responses.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridCacheRestResponse extends GridRestResponse {
    /** Affinity node ID. */
    private String affinityNodeId;

    /**
     *
     */
    public GridCacheRestResponse() {
        // No-op.
    }

    /**
     * @param success Success.
     */
    public GridCacheRestResponse(boolean success) {
        super(success ? STATUS_SUCCESS : STATUS_FAILED);
    }

    /**
     * @param success Success.
     * @param obj Response object.
     */
    public GridCacheRestResponse(boolean success, Object obj) {
        super(success ? STATUS_SUCCESS : STATUS_FAILED, obj);
    }

    /**
     * @param success Success.
     * @param obj Response object.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public GridCacheRestResponse(boolean success, @Nullable Object obj, @Nullable String err) {
        super(success ? STATUS_SUCCESS : STATUS_FAILED, obj, err);
    }

    /**
     * @return Affinity node ID.
     */
    public String getAffinityNodeId() {
        return affinityNodeId;
    }

    /**
     * @param affinityNodeId Affinity node ID.
     */
    public void setAffinityNodeId(String affinityNodeId) {
        this.affinityNodeId = affinityNodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheRestResponse.class, this, super.toString());
    }
}
