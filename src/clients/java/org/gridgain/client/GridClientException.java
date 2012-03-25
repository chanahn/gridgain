// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

/**
 * Client exception.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridClientException extends Exception {
    /**
     * @param msg Message.
     */
    public GridClientException(String msg) {
        super(msg);
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     */
    public GridClientException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * @param cause Cause.
     */
    public GridClientException(Throwable cause) {
        super(cause);
    }
}
