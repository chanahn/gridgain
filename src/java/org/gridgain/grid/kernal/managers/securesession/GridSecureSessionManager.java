// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.securesession;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.securesession.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;

/**
 * This class defines a grid authentication manager.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridSecureSessionManager extends GridManagerAdapter<GridSecureSessionSpi> {
    /** Secure session handler. */
    private GridSecureSessionHandler sesHnd;

    /** @param ctx Grid kernal context. */
    public GridSecureSessionManager(GridKernalContext ctx) {
        super(GridSecureSessionSpi.class, ctx, ctx.config().getSecureSessionSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.isEnterprise()) {
            Package pkg = getClass().getPackage();

            if (pkg == null)
                throw new GridException("Internal error (package object was not found) for: " + getClass().getName());

            if (ctx.isEnterprise()) {
                try {
                    Class<?> cls = Class.forName(pkg.getName() + ".GridEnterpriseSecureSessionHandler");

                    sesHnd = (GridSecureSessionHandler)cls.getConstructor(GridSecureSessionSpi[].class)
                        .newInstance(new Object[]{getProxies()});
                }
                catch (ClassNotFoundException e) {
                    throw new GridException("Failed to create enterprise secure session handler (implementing class " +
                        "was not found)", e);
                }
                catch (InvocationTargetException e) {
                    throw new GridException("Failed to create enterprise secure session handler (target constructor " +
                        "has thrown an exception", e.getCause());
                }
                catch (InstantiationException e) {
                    throw new GridException("Failed to create enterprise secure session handler (object cannot be " +
                        "instantiated)", e);
                }
                catch (NoSuchMethodException e) {
                    throw new GridException("Failed to create enterprise secure session handler (target constructor " +
                        "could not be found)", e);
                }
                catch (IllegalAccessException e) {
                    throw new GridException("Failed to create enterprise secure session handler (object access is not" +
                        " allowed)", e);
                }
            }
        }
        else
            sesHnd = new GridCommunitySecureSessionHandler();

        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel, boolean wait) throws GridException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Checks if security check is enabled.
     *
     * @return {@code True} if secure session check is enabled.
     */
    public boolean securityEnabled() {
        return sesHnd.securityEnabled();
    }

    /**
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param tok Token.
     * @param params Parameters.
     * @return Next token.
     * @throws GridException If error occurred.
     */
    @Nullable public byte[] validate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridException {
        assert sesHnd != null;

        return sesHnd.validate(subjType, subjId, tok, params);
    }
}
