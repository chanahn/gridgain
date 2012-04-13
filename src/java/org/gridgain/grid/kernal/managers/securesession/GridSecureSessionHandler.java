// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.securesession;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

/**
 * Interface to hide differences between enterprise and community editions security logic.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.2c.12042012
 */
interface GridSecureSessionHandler {
    /**
     * Depending on implementation will either delegate to corresponding secure session spi or always succeed.
     *
     * @param subjType Subject type.
     * @param subjId Subject id.
     * @param tok Token to authenticate.
     * @param params Parameters.
     * @return Generated token or {@code null} if authentication failed.
     * @throws GridException If SPI supporting given subject type was not found.
     */
    @Nullable public byte[] validate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridException;

    /**
     * @return {@code True} if session will be validated by SPI.
     */
    public boolean securityEnabled();
}
