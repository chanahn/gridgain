// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client;

/**
 * This exception is thrown when none of the servers specified in client configuration can be
 * connected to within timeout.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridServerUnreachableException extends GridClientException {
    /**
     * Creates exception with specified error message.
     *
     * @param msg Error message.
     */
    public GridServerUnreachableException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with specified error message and cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     */
    public GridServerUnreachableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
