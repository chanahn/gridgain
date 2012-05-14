// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * This runtime exception gets thrown if attempt to access a removed data structure has been made.
 * <p>
 * Note that data structures throw runtime exceptions out of methods that don't have
 * checked exceptions in the signature. If you prefer to handle checked exceptions,
 * then use methods that end with {@code 'x'}, e.g. {@link GridCacheQueue#addx(Object)}
 * vs. {@link GridCacheQueue#add(Object)}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridCacheDataStructureRemovedRuntimeException extends GridRuntimeException {
    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public GridCacheDataStructureRemovedRuntimeException(String msg) {
        super(msg);
    }

    /**
     * Creates new exception with given throwable as a nested cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridCacheDataStructureRemovedRuntimeException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridCacheDataStructureRemovedRuntimeException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
