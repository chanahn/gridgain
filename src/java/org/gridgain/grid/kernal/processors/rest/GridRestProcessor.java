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
 * @version 4.0.0c.22032012
 */
public class GridRestProcessor extends GridProcessorAdapter {
    /** Empty session token. */
    private static final byte[] EMPTY_TOK = new byte[0];

    /** Protocols. */
    private final Collection<GridRestProtocol> protos = new ArrayList<GridRestProtocol>();

    /**  Command handlers. */
    private final Collection<GridRestCommandHandler> handlers = new ArrayList<GridRestCommandHandler>();

    /** Protocol handler. */
    private final GridRestProtocolHandler protoHnd = new GridRestProtocolHandler() {
        @Override public GridRestResponse handle(GridRestRequest req) throws GridException {
            return handleAsync(req).get();
        }

        @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
            try {
                final byte[] sesTok = authenticate(req);

                GridFuture<GridRestResponse> res = null;

                for (GridRestCommandHandler handler : handlers) {
                    if (handler.supported(req.getCommand())) {
                        res = handler.handleAsync(req);

                        break;
                    }
                }

                if (res != null) {
                    return new GridEmbeddedFuture<GridRestResponse, GridRestResponse>(ctx, res,
                        new C2<GridRestResponse, Exception, GridRestResponse>() {
                        @Override public GridRestResponse apply(GridRestResponse res, Exception e) {
                            res.sessionTokenBytes(sesTok);

                            return res;
                        }
                    });
                }
                else {
                    // No handler found for command.
                    GridRestResponse resp = new GridRestResponse(STATUS_FAILED, null,
                        "Failed to find registered handler for command: " + req.getCommand());

                    resp.sessionTokenBytes(sesTok);

                    return new GridFinishedFuture<GridRestResponse>(ctx, resp);
                }
            }
            catch (GridException e) {
                return new GridFinishedFuture<GridRestResponse>(ctx, new GridRestResponse(STATUS_AUTH_FAILED, null,
                    e.getMessage()));
            }
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
     * @return Secure session token.
     *
     * @throws GridException If authentication failed.
     */
    private byte[] authenticate(GridRestRequest req) throws GridException {
        UUID clientId = req.getClientId();

        if (clientId == null)
            throw new GridException("Failed to authenticate remote client (client id was not specified): " + req);

        byte[] sesTok = req.getSessionToken();

        if (sesTok == null)
            sesTok = EMPTY_TOK;

        byte[] clientIdBytes = U.getBytes(clientId);

        sesTok = ctx.secureSession().validate(REMOTE_CLIENT, clientIdBytes, sesTok, null);

        if (sesTok != null)
            return sesTok;
        else {
            if (ctx.auth().authenticate(REMOTE_CLIENT, clientIdBytes, req.getCredentials())) {
                // Generate new session id.
                return ctx.secureSession().validate(REMOTE_CLIENT, clientIdBytes, null, null);
            }
            else
                // Return response with flag indicating if it is a expired session or authentication failed.
                throw new GridException("Failed to authenticate remote client [clientId=" + clientId + ", req=" + req +
                    ']');
        }
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
