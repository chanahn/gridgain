// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.message;

import java.io.*;
import java.util.*;

/**
 * Interface for all client messages.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public interface GridClientMessage extends Serializable {
    /**
     * This method is used to match request and response messages.
     *
     * @return request ID.
     */
    public long requestId();

    /**
     * Sets request id for outgoing packets.
     *
     * @param reqId request ID.
     */
    public void requestId(long reqId);

    /**
     * Gets client identifier from which this request comes.
     *
     * @return Client identifier.
     */
    public UUID clientId();

    /**
     * Sets client identifier from which this request comes.
     *
     * @param id Client identifier.
     */
    public void clientId(UUID id);

    /**
     * Sets client session token.
     *
     * @return Session token.
     */
    public byte[] sessionToken();

    /**
     * Gets client session token.
     *
     * @param sesTok Session token.
     */
    public void sessionToken(byte[] sesTok);
}
