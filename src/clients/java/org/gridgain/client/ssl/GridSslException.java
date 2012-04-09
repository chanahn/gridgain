// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.ssl;

/**
 * General exception indicating that ssl context could not be created.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public class GridSslException extends Exception {
    /**
     * Creates exception with given error message.
     *
     * @param msg Error message.
     */
    public GridSslException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with given error message and error cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     */
    public GridSslException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
