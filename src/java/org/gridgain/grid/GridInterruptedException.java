// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

/**
 * This exception is used to wrap standard {@link InterruptedException} into {@link GridException}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
@SuppressWarnings({"TypeMayBeWeakened"})
public class GridInterruptedException extends GridException {
    /**
     * Creates new exception with given throwable as a nested cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridInterruptedException(InterruptedException cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     */
    public GridInterruptedException(String msg) {
        super(msg);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridInterruptedException(String msg, InterruptedException cause) {
        super(msg, cause);
    }
}