// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.kernal.processors.rest.handlers.log.*;
import org.gridgain.grid.kernal.processors.rest.handlers.task.*;
import org.gridgain.grid.kernal.processors.rest.handlers.top.*;
import org.gridgain.grid.kernal.processors.rest.handlers.version.*;
import org.gridgain.grid.kernal.processors.rest.protocols.http.jetty.*;
import org.gridgain.grid.kernal.processors.rest.protocols.tcp.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestResponse.*;
import static org.gridgain.grid.spi.GridSecuritySubjectType.REMOTE_CLIENT;

/**
 * Rest processor implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridRestProcessor extends GridProcessorAdapter {
    /** Protocols. */
    private final Collection<GridRestProtocol> protos = new ArrayList<GridRestProtocol>();

    /**  Command handlers. */
    private final Collection<GridRestCommandHandler> handlers = new ArrayList<GridRestCommandHandler>();

    /** Protocol handler. */
    private final GridRestProtocolHandler protoHnd = new GridRestProtocolHandler() {
        @Override public GridRestResponse handle(GridRestRequest req) throws GridException {
            return handleAsync(req).get();
        }

        @Override public GridFuture<GridRestResponse> handleAsync(final GridRestRequest req) {
            try {
                authenticate(req);
            }
            catch (GridException e) {
                return new GridFinishedFuture<GridRestResponse>(ctx,
                    new GridRestResponse(STATUS_AUTH_FAILED, e.getMessage()));
            }

            GridFuture<GridRestResponse> res = null;

            for (GridRestCommandHandler handler : handlers)
                if (handler.supported(req.getCommand())) {
                    res = handler.handleAsync(req);

                    break;
                }

            if (res == null)
                return new GridFinishedFuture<GridRestResponse>(ctx, new GridException(
                    "Failed to find registered handler for command: " + req.getCommand()));

            return new GridEmbeddedFuture<GridRestResponse, GridRestResponse>(ctx, res,
                new C2<GridRestResponse, Exception, GridRestResponse>() {
                    @Override public GridRestResponse apply(GridRestResponse res, Exception e) {
                        if (e != null)
                            res = new GridRestResponse(STATUS_FAILED, e.getMessage());

                        assert res != null;

                        try {
                            res.sessionTokenBytes(updateSessionToken(req));
                        } catch (GridException x) {
                            U.warn(log, "Cannot update response session token: " + x.getMessage());
                        }

                        return res;
                    }
                });
        }
    };

    /**
     * @param ctx Context.
     */
    public GridRestProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Authenticates remote client.
     *
     * @param req Request to authenticate.
     * @throws GridException If authentication failed.
     */
    private void authenticate(GridRestRequest req) throws GridException {
        UUID clientId = req.getClientId();

        if (clientId == null)
            throw new GridException("Failed to authenticate remote client (client id was not specified): " + req);

        byte[] clientIdBytes = U.getBytes(clientId);

        byte[] sesTok = req.getSessionToken();

        // Validate session.
        if (sesTok != null && ctx.secureSession().validate(REMOTE_CLIENT, clientIdBytes, sesTok, null) != null)
            // Session is still valid.
            return;

        // Authenticate client if invalid session.
        if (!ctx.auth().authenticate(REMOTE_CLIENT, clientIdBytes, req.getCredentials()))
            if (req.getCredentials() == null)
                throw new GridException("Failed to authenticate remote client (secure session SPI not set?): " + req);
            else
                throw new GridException("Failed to authenticate remote client (invalid credentials?): " + req);
    }

    /**
     * Update session token to actual state.
     *
     * @param req Grid est request.
     * @return Valid session token.
     * @throws GridException If session token update process failed.
     */
    private byte[] updateSessionToken(GridRestRequest req) throws GridException {
        byte[] subjId = U.getBytes(req.getClientId());

        byte[] sesTok = req.getSessionToken();

        // Update token from request to actual state.
        if (sesTok != null)
            sesTok = ctx.secureSession().validate(REMOTE_CLIENT, subjId, req.getSessionToken(), null);

        // Create new session token, if request doesn't valid session token.
        if (sesTok == null)
            sesTok = ctx.secureSession().validate(REMOTE_CLIENT, subjId, null, null);

        // Validate token has been created.
        if (sesTok == null)
            throw new GridException("Cannot create session token (is secure session SPI set?).");

        return sesTok;
    }

    /**
     *
     * @return Whether or not REST is enabled.
     */
    private boolean isRestEnabled() {
        return ctx != null && ctx.config().isRestEnabled();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        if (isRestEnabled()) {
            // Register handlers.
            addHandler(new GridCacheCommandHandler(ctx));
            addHandler(new GridTaskCommandHandler(ctx));
            addHandler(new GridTopologyCommandHandler(ctx));
            addHandler(new GridVersionCommandHandler(ctx));
            addHandler(new GridLogCommandHandler(ctx));

            // Start protocol.
            startProtocol(new GridJettyRestProtocol(ctx));
            startProtocol(new GridTcpRestProtocol(ctx));

            if (log.isDebugEnabled())
                log.debug("REST processor started.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel, boolean wait) {
        if (isRestEnabled()) {
            for (GridRestProtocol proto : protos)
                proto.stop();

            if (log.isDebugEnabled())
                log.debug("REST processor stopped.");
        }
    }

    /**
     * @param hnd Command handler.
     * @return {@code True} if class for handler was found.
     */
    private boolean addHandler(GridRestCommandHandler hnd) {
        assert !handlers.contains(hnd);

        if (log.isDebugEnabled())
            log.debug("Added REST command handler: " + hnd);

        return handlers.add(hnd);
    }

    /**
     * @param proto Protocol.
     * @throws GridException If protocol initialization failed.
     */
    private void startProtocol(GridRestProtocol proto) throws GridException {
        assert !protos.contains(proto);

        if (log.isDebugEnabled())
            log.debug("Added REST protocol: " + proto);

        proto.start(protoHnd);

        protos.add(proto);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> REST processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   protosSize: " + protos.size());
        X.println(">>>   handlersSize: " + handlers.size());
    }
}
