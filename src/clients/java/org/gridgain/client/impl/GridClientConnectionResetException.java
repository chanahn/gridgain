// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

/**
 * This exception is thrown when ongoing packet should be sent, but network connection is broken.
 * In this case client will try to reconnect to any of the servers specified in configuration.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
class GridClientConnectionResetException extends GridClientException {
    /**
     * Creates an exception with given message.
     *
     * @param msg Error message.
     */
    GridClientConnectionResetException(String msg) {
        super(msg);
    }

    /**
     * Creates an exception with given message and error cause.
     *
     * @param msg Error message.
     * @param cause Wrapped exception.
     */
    GridClientConnectionResetException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
