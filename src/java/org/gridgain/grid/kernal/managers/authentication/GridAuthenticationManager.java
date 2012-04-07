// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.authentication;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.spi.GridSecuritySubjectType.*;

/**
 * This class defines a grid authentication manager.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridAuthenticationManager extends GridManagerAdapter<GridAuthenticationSpi> {
    /** Authentication handler. */
    private GridAuthenticationHandler authHnd;

    /** @param ctx Grid kernal context. */
    public GridAuthenticationManager(GridKernalContext ctx) {
        super(GridAuthenticationSpi.class, ctx, ctx.config().getAuthenticationSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.isEnterprise()) {
            Package pkg = getClass().getPackage();

            if (pkg == null)
                throw new GridException("Internal error (package object was not found) for: " + getClass().getName());

            if (ctx.isEnterprise()) {
                try {
                    Class<?> cls = Class.forName(pkg.getName() + ".GridEnterpriseAuthenticationHandler");

                    authHnd = (GridAuthenticationHandler)cls.getConstructor(GridAuthenticationSpi[].class)
                        .newInstance(new Object[]{getProxies()});
                }
                catch (ClassNotFoundException e) {
                    throw new GridException("Failed to create enterprise authentication handler (implementing class " +
                        "was not found)", e);
                }
                catch (InvocationTargetException e) {
                    throw new GridException("Failed to create enterprise authentication handler (target constructor " +
                        "has thrown an exception", e.getCause());
                }
                catch (InstantiationException e) {
                    throw new GridException("Failed to create enterprise authentication handler (object cannot be " +
                        "instantiated)", e);
                }
                catch (NoSuchMethodException e) {
                    throw new GridException("Failed to create enterprise authentication handler (target constructor " +
                        "could not be found)", e);
                }
                catch (IllegalAccessException e) {
                    throw new GridException("Failed to create enterprise authentication handler (object access is not" +
                        " allowed)", e);
                }
            }
        }
        else
            authHnd = new GridCommunityAuthenticationHandler();

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
     * @return {@code True} if authentication check is enabled.
     */
    public boolean securityEnabled() {
        return authHnd.securityEnabled();
    }

    /**
     * Authenticates subject via underlying {@link GridAuthenticationSpi}s.
     *
     * @param subjType Subject type.
     * @param subjId Subject ID.
     * @param creds Credentials.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws GridException If error occurred.
     */
    public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId,
        @Nullable Object creds) throws GridException {
        assert authHnd != null;

        return authHnd.authenticate(subjType, subjId, creds);
    }

    /**
     * Authenticates grid node with it's attributes via underlying {@link GridAuthenticationSpi}s.
     *
     * @param nodeId Node id to authenticate.
     * @param attrs Node attributes.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws GridException If error occurred.
     */
    public boolean authenticateNode(UUID nodeId, Map<String, Object> attrs) throws GridException {
        // Interpret the node attributes as credentials.
        return authenticate(REMOTE_NODE, U.getBytes(nodeId), attrs);
    }
}
