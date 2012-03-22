// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.gridgain.client.message.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.protocols.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.nio.ssl.*;
import org.gridgain.grid.util.worker.*;

import javax.net.ssl.*;
import java.net.*;

/**
 * TCP binary protocol implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridTcpRestProtocol extends GridRestProtocolAdapter {
    /** TCP port rebind attempt frequency. */
    private static final int REBIND_FREQ = 3000;

    /** Server. */
    private GridNioServer<GridClientMessage> srv;

    /** Binder thread. */
    private Thread binder;

    /** @param ctx Context. */
    public GridTcpRestProtocol(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "TCP binary";
    }

    /** {@inheritDoc} */
    @Override public void start(final GridRestProtocolHandler hnd) throws GridException {
        assert hnd != null;

        final GridConfiguration cfg = ctx.config();

        String host = cfg.getRestTcpHost();

        if (host == null)
            host = cfg.getLocalHost();

        final int port = cfg.getRestTcpPort();

        final GridNioServerListener<GridClientMessage> lsnr = new GridTcpRestNioListener(log, hnd);

        final GridNioParser<GridClientMessage> parser = new GridTcpRestParser();

        try {
            final InetAddress hostAddr = InetAddress.getByName(host);

            SSLContext sslCtx = null;

            if (cfg.isRestTcpSslEnabled()) {
                GridSslContextFactory factory = cfg.getRestTcpSslContextFactory();

                if (factory == null)
                    throw new GridSslException("SSL is enabled, but SSL context factory is not specified.");

                sslCtx = factory.createSslContext();
            }

            final boolean tcpNoDelay = cfg.isRestTcpNoDelay();

            if (!startTcpServer(hostAddr, port, lsnr, parser, tcpNoDelay, sslCtx, cfg.isRestTcpSslClientAuth(),
                cfg.isRestTcpSslClientAuth())) {
                U.warn(log, "TCP binary REST server failed to start (retrying every " + REBIND_FREQ + " ms). " +
                    "Another node on this host?");

                final SSLContext sslCtx0 = sslCtx;

                binder = new GridThread(new GridWorker(ctx.gridName(), "grid-connector-jetty-binder", log) {
                    @SuppressWarnings({"BusyWait"})
                    @Override public void body() {
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                Thread.sleep(REBIND_FREQ);
                            }
                            catch (InterruptedException ignore) {
                                if (log.isDebugEnabled())
                                    log.debug("Jetty binder thread was interrupted.");

                                break;
                            }

                            if (startTcpServer(hostAddr, port, lsnr, parser, tcpNoDelay, sslCtx0,
                                cfg.isRestTcpSslClientAuth(), cfg.isRestTcpSslClientAuth()))
                                break;
                        }
                    }
                });
            }
            else if (log.isInfoEnabled())
                log.info(startInfo());
        }
        catch (UnknownHostException e) {
            U.warn(log, "Failed to start " + name() + " protocol on port " + port + ": " + e.getMessage(),
                "Failed to start " + name() + " protocol on port " + port + ". Check tcpHost configuration property.");
        }
        catch (GridSslException e) {
            U.warn(log, "Failed to start " + name() + " protocol on port " + port + ": " + e.getMessage(),
                "Failed to start " + name() + " protocol on port " + port + ". Check if SSL context factory is " +
                    "properly configured.");
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        U.interrupt(binder);

        U.join(binder, log);

        if (srv != null) {
            ctx.ports().deregisterPorts(getClass());

            srv.stop();
        }

        if (log.isInfoEnabled())
            log.info(stopInfo());
    }

    /**
     * Tries to start server with given parameters.
     *
     * @param hostAddr Host on which server should be bound.
     * @param port Port on which server should be bound.
     * @param lsnr Server message listener.
     * @param parser Server message parser.
     * @param tcpNoDelay Flag indicating whether TCP_NODELAY flag should be set for accepted connections.
     * @param sslCtx SSL context in case if SSL is enabled.
     * @param wantClientAuth Whether client will be requested for authentication.
     * @param needClientAuth Whether client is required to be authenticated.
     * @return {@code True} if server successfully started, {@code false} if port is used and
     *      server was unable to start.
     */
    private boolean startTcpServer(InetAddress hostAddr, int port, GridNioServerListener<GridClientMessage> lsnr,
        GridNioParser<GridClientMessage> parser, boolean tcpNoDelay, SSLContext sslCtx, boolean wantClientAuth,
        boolean needClientAuth) {
        try {
            GridNioFilterAdapter codec = new GridNioCodecFilter<GridClientMessage>(parser, log);

            if (sslCtx == null)
                srv = GridNioServerFactory.createServer(hostAddr, port, lsnr, log,
                    Runtime.getRuntime().availableProcessors(), ctx.gridName(), tcpNoDelay, false, codec);
            else {
                GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtx, log);

                sslFilter.wantClientAuth(wantClientAuth);

                sslFilter.needClientAuth(needClientAuth);

                srv = GridNioServerFactory.createServer(hostAddr, port, lsnr, log,
                    Runtime.getRuntime().availableProcessors(), ctx.gridName(), tcpNoDelay, false, codec, sslFilter);
            }

            srv.start();

            ctx.ports().registerPort(port, GridPortProtocol.TCP, getClass());

            return true;
        }
        catch (GridException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to start " + name() + " protocol on port " + port + " (will retry in " +
                    REBIND_FREQ + " ms): " + e.getMessage());

            return false;
        }
    }
}
