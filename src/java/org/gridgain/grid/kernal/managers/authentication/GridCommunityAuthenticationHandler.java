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
 * Community authentication handler.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.2c.12042012
 */
public class GridCommunityAuthenticationHandler implements GridAuthenticationHandler {
    /** {@inheritDoc} */
    @Override public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable Object creds)
        throws GridException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean securityEnabled() {
        return false;
    }
}
