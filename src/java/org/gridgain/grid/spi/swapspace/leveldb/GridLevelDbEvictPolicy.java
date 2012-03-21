// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace.leveldb;

import org.jetbrains.annotations.*;

/**
 * Enumeration of all supported eviction policies.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public enum GridLevelDbEvictPolicy {
    /**
     * Specifies swap space behaviour without evictions. Users of such space must delete keys themselves
     * whenever needed to avoid unlimited database growth. On the other hand, this is the most efficient
     * policy because it doesn't have overhead of maintaining eviction order. This policy doesn't keep
     * track of data stored in it but relies on LevelDB API to get approximate size and count.
     */
    EVICT_DISABLED,

    /**
     * Specifies swap space that supports evicts. The internal data structures are optimized for cases
     * when stored values are small, i.e. it is relatively inexpensive to read the whole value for any
     * swap operation.
     */
    EVICT_OPTIMIZED_SMALL,

    /**
     * Specifies swap space that supports evicts. The internal data structures are optimized for cases
     * when stored values are large, i.e. values should not be read unless required by swap operation.
     */
    EVICT_OPTIMIZED_LARGE;

    /** Enumerated values. */
    private static final GridLevelDbEvictPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridLevelDbEvictPolicy fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
