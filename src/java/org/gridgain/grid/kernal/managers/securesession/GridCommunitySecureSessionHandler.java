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
 * Community secure session handler. Will always authenticate subject.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridCommunitySecureSessionHandler implements GridSecureSessionHandler {
    /** Empty token to return. */
    private static final byte[] EMPTY_TOK = new byte[0];

    /** {@inheritDoc} */
    @Override public byte[] validate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridException {
        return EMPTY_TOK;
    }

    /** {@inheritDoc} */
    @Override public boolean securityEnabled() {
        return false;
    }
}
