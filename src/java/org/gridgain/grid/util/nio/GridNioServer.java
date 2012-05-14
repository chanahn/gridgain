// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * The general interface of TCP server. Due to asynchronous nature of connections processing
 * network events such as client connection, disconnection and message receiving are passed to
 * the server listener. Once client connected, an associated {@link GridNioSession} object is
 * created and can be used in communication.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public interface GridNioServer<T> {
    /**
     * Starts all associated threads to perform accept and read activities.
     */
    void start();

    /**
     * Closes all threads.
     */
    void stop();

    /**
     * Asynchronously closes given session. Returned future will return {@code true} if exactly this
     * call was the cause of session being closed. If another concurrent call has closed this session
     * or remote client has disconnected, returned future will return {@code false}.
     *
     * @param ses Session to be closed.
     * @return Operation future.
     */
    GridNioFuture<Boolean> close(GridNioSession ses);

    /**
     * Checks if there is a session with remote client with given address and returns this session, if any.
     * <p>
     * Note that due to asynchronous request processing returned session may be closed.
     *
     * @param rmtAddr Remote address which session is expected to be connected with.
     * @return Session instance or {@code null} if there is no such session.
     */
    @Nullable GridNioSession session(InetSocketAddress rmtAddr);

    /**
     * Gets all currently opened sessions on this server.
     * <p>
     * Note that due to asynchronous request processing some of returned sessions may be closed.
     *
     * @return Collection of opened sessions.
     */
    Collection<GridNioSession> sessions();
}
