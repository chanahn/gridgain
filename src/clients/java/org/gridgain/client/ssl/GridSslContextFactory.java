// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.ssl;

import javax.net.ssl.*;

/**
 * This interface provides creation of SSL context both for server and client use.
 * <p>
 * Usually, it is enough to configure context from a particular key and trust stores, this functionality is provided
 * in {@link GridSslBasicContextFactory}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public interface GridSslContextFactory {
    /**
     * Creates SSL context based on factory settings.
     *
     * @return Initialized SSL context.
     * @throws GridSslException If SSL context could not be created.
     */
    public SSLContext createSslContext() throws GridSslException;
}
