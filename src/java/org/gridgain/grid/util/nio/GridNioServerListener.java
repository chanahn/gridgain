// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.jetbrains.annotations.*;

/**
 * Listener passed in to the {@link org.gridgain.grid.util.nio.impl.GridNioServerImpl} that will be notified on client events.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public interface GridNioServerListener<T> {
    /**
     * This method is called whenever a new client is connected and session is created.
     *
     * @param ses Newly created session for remote client.
     */
    public void onConnected(GridNioSession ses);

    /**
     * This method is called whenever client is disconnected due to correct connection close
     * or due to {@code IOException} during network operations.
     *
     * @param ses Closed session.
     * @param e Exception occurred, if any.
     */
    public void onDisconnected(GridNioSession ses, @Nullable Exception e);

    /**
     * This method is called whenever a {@link GridNioParser} returns non-null value.
     *
     * @param ses Session on which message was received.
     * @param msg Parsed message.
     */
    public void onMessage(GridNioSession ses, T msg);
}
