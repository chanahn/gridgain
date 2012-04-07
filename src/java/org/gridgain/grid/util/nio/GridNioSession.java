// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import java.net.*;

/**
 * This interface represents established or closed connection between nio server and remote client.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public interface GridNioSession {
    /**
     * Gets local address of this session.
     *
     * @return Local network address.
     */
    public InetSocketAddress localAddress();

    /**
     * Gets address of remote peer on this session.
     *
     * @return Address of remote peer.
     */
    public InetSocketAddress remoteAddress();

    /**
     * Gets the total count of bytes sent since the session was created.
     *
     * @return Total count of bytes sent.
     */
    public long bytesSent();

    /**
     * Gets the total count of bytes received since the session was created.
     *
     * @return Total count of bytes received.
     */
    public long bytesReceived();

    /**
     * Gets the time when the session was created.
     *
     * @return Time when this session was created returned by {@link System#currentTimeMillis()}.
     */
    public long createTime();

    /**
     * If session is closed, this method will return session close time returned by {@link System#currentTimeMillis()}.
     * If session is not closed, this method will return {@code 0}.
     * 
     * @return Session close time.
     */
    public long closeTime();

    /**
     * Returns the time when last read activity was performed on this session.
     *
     * @return Lats receive time.
     */
    public long lastReceiveTime();

    /**
     * Returns time when last send activity was performed on this session.
     *
     * @return Last send time.
     */
    public long lastSendTime();

    /**
     * Performs a request for asynchronous session close.
     *
     * @return Future representing result.
     */
    public GridNioFuture<Boolean> close();

    /**
     * Performs a request for asynchronous data send.
     *
     * @param msg Message to be sent. This message will be eventually passed in to a parser plugged
     *            to the nio server.
     * @return Future representing result.
     */
    public GridNioFuture<?> send(Object msg);

    /**
     * Gets metadata associated with specified key.
     *
     * @param key Key to look up.
     * @return Associated meta object or {@code null} if meta was not found.
     */
    public <T> T meta(String key);

    /**
     * Adds metadata associated with specified key.
     *
     * @param key Metadata Key.
     * @param val Metadata value.
     * @return Previously associated object or {@code null} if no objects were associated.
     */
    public <T> T addMeta(String key, T val);

    /**
     * Puts meta associated with the specified key only if no data were associated with the given key.
     * Check is performed atomically.
     *
     * @param key Metadata key.
     * @param val Metadata value.
     * @return {@code null} If no metadata were associated or object that is already set.
     */
    public <T> T putMetaIfAbsent(String key, T val);

    /**
     * Removes metadata with the specified key.
     *
     * @param key Metadata key.
     * @return Object that was associated with the key or {@code null}.
     */
    public <T> T removeMeta(String key);
}
