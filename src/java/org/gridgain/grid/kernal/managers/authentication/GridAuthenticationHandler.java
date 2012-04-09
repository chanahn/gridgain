// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.authentication;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

/**
 * Interface to hide differences between enterprise and community editions security logic.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
interface GridAuthenticationHandler {
    /**
     * Depending on implementation will either delegate to corresponding authentication spi,
     * or always return {@code True} (in community edition).
     *
     * @param subjType Subject type.
     * @param subjId Subject id.
     * @param creds Authentication credentials.
     * @return Whether subject was authenticated or not.
     * @throws GridException If SPI for requested subject type was not found.
     */
    public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable Object creds)
        throws GridException;

    /**
     * @return {@code True} if authentication check will be delegated to SPI.
     */
    public boolean securityEnabled();
}
