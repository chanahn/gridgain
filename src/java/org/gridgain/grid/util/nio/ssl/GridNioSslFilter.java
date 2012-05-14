// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio.ssl;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.nio.impl.*;

import javax.net.ssl.*;
import java.io.*;
import java.nio.*;

/**
 * Implementation of SSL filter using {@link SSLEngine}
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridNioSslFilter extends GridNioFilterAdapter {
    /** Handler meta name. */
    private static final String SSL_HANDLER_META_NAME = GridNioSslFilter.class.getName() + ".handler";

    /** Logger to use. */
    private GridLogger log;

    /** Set to true if engine should request client authentication. */
    private boolean wantClientAuth;

    /** Set to true if engine should require client authentication. */
    private boolean needClientAuth;

    /** Array of enabled cipher suites, optional. */
    private String[] enabledCipherSuites;

    /** Array of enabled protocols. */
    private String[] enabledProtos;

    /** SSL context to use. */
    private SSLContext sslCtx;

    /**
     * Creates SSL filter.
     *
     * @param sslCtx SSL context.
     * @param log Logger to use.
     */
    public GridNioSslFilter(SSLContext sslCtx, GridLogger log) {
        super("SSL filter");

        this.log = log;
        this.sslCtx = sslCtx;
    }

    /**
     * Sets flag indicating whether client authentication will be requested during handshake.
     *
     * @param wantClientAuth {@code True} if client authentication should be requested.
     */
    public void wantClientAuth(boolean wantClientAuth) {
        this.wantClientAuth = wantClientAuth;
    }

    /**
     * Sets flag indicating whether client authentication will be required.
     *
     * @param needClientAuth {@code True} if client authentication is required.
     */
    public void needClientAuth(boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    /**
     * Sets a set of cipher suites that will be enabled for this filter.
     *
     * @param enabledCipherSuites Enabled cipher suites.
     */
    public void enabledCipherSuites(String... enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites;
    }

    /**
     * Sets enabled secure protocols for this filter.
     *
     * @param enabledProtos Enabled protocols.
     */
    public void enabledProtocols(String... enabledProtos) {
        this.enabledProtos = enabledProtos;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Remote client connected, creating SSL handler and performing initial handshake: " + ses);

        SSLEngine engine = sslCtx.createSSLEngine();

        engine.setUseClientMode(false);

        engine.setWantClientAuth(wantClientAuth);

        engine.setNeedClientAuth(needClientAuth);

        if (enabledCipherSuites != null)
            engine.setEnabledCipherSuites(enabledCipherSuites);

        if (enabledProtos != null)
            engine.setEnabledProtocols(enabledProtos);

        try {
            GridNioSslHandler hnd = new GridNioSslHandler(this, ses, engine, log);

            ses.addMeta(SSL_HANDLER_META_NAME, hnd);

            hnd.handshake();
        }
        catch (SSLException e) {
            U.error(log, "Failed to start SSL handshake (will close inbound connection): " + ses, e);

            ses.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws GridException {
        GridNioSslHandler hnd = sslHandler(ses);

        try {
            hnd.shutdown();
        }
        finally {
            proceedSessionClosed(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
        ByteBuffer input = checkMessage(ses, msg);

        if (!input.hasRemaining())
            return new GridNioFinishedFuture<Object>(null);

        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            if (hnd.isOutboundDone())
                return new GridNioFinishedFuture<Object>(new IOException("Failed to send data (secure session was " +
                    "already closed): " + ses));

            if (hnd.isHandshakeFinished()) {
                hnd.encrypt(input);

                return hnd.writeNetBuffer();
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Write request received during handshake, scheduling deferred write: " + ses);

                return hnd.deferredWrite(input);
            }
        }
        catch (SSLException e) {
            throw new GridNioException("Failed to encode SSL data: " + ses, e);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
        ByteBuffer input = checkMessage(ses, msg);

        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            hnd.messageReceived(input);

            // Handshake may become finished on incoming message, flush writes, if any.
            if (hnd.isHandshakeFinished())
                hnd.flushDeferredWrites();

            ByteBuffer appBuf = hnd.getApplicationBuffer();

            appBuf.flip();

            if (appBuf.hasRemaining())
                proceedMessageReceived(ses, appBuf);

            appBuf.clear();

            if (hnd.isInboundDone() && !hnd.isOutboundDone()) {
                if (log.isDebugEnabled())
                    log.debug("Remote peer closed secure session (will close connection): " + ses);

                shutdownSession(ses, hnd);
            }
        }
        catch (SSLException e) {
            throw new GridNioException("Failed to decode SSL data: " + ses, e);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
        GridNioSslHandler hnd = sslHandler(ses);

        hnd.lock();

        try {
            return shutdownSession(ses, hnd);
        }
        finally {
            hnd.unlock();
        }
    }

    /**
     * Sends SSL <tt>close_notify</tt> message and closes underlying TCP connection.
     *
     * @param ses Session to shutdown.
     * @param hnd SSL handler.
     * @throws GridNioException If failed to forward requests to filter chain.
     * @return Close future.
     */
    private GridNioFuture<Boolean> shutdownSession(GridNioSession ses, GridNioSslHandler hnd) throws GridException {
        try {
            hnd.closeOutbound();

            hnd.writeNetBuffer();
        }
        catch (SSLException e) {
            U.warn(log, "Failed to shutdown SSL session gracefully (will force close) [ex=" + e + ", ses=" + ses + ']');
        }

        return proceedSessionClose(ses);
    }

    /**
     * Gets ssl handler from the session.
     *
     * @param ses Session instance.
     * @return SSL handler.
     * @throws GridNioException If no handlers were associated with the session.
     */
    private GridNioSslHandler sslHandler(GridNioSession ses) throws GridNioException {
        GridNioSslHandler hnd = ses.meta(SSL_HANDLER_META_NAME);

        if (hnd == null)
            throw new GridNioException("Failed to process incoming message (received message before SSL handler " +
                "was created): " + ses);

        return hnd;
    }

    /**
     * Checks type of the message passed to the filter and converts it to a byte buffer (since SSL filter
     * operates only on binary data).
     *
     * @param ses Session instance.
     * @param msg Message passed in.
     * @return Message that was cast to a byte buffer.
     * @throws GridNioException If msg is not a byte buffer.
     */
    private ByteBuffer checkMessage(GridNioSession ses, Object msg) throws GridNioException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Invalid object type received (is SSL filter correctly placed in filter " +
                "chain?) [ses=" + ses + ", msgClass=" + msg.getClass().getName() +  ']');

        return (ByteBuffer)msg;
    }

    /**
     * Expands the given byte buffer to the requested capacity.
     *
     * @param original Original byte buffer.
     * @param cap Requested capacity.
     * @return Expanded byte buffer.
     */
    public static ByteBuffer expandBuffer(ByteBuffer original, int cap) {
        ByteBuffer res = ByteBuffer.allocate(cap);

        original.flip();

        res.put(original);

        return res;
    }

    /**
     * Copies the given byte buffer.
     *
     * @param original Byte buffer to copy.
     * @return Copy of the original byte buffer.
     */
    public static ByteBuffer copy(ByteBuffer original) {
        ByteBuffer cp = ByteBuffer.allocate(original.remaining());

        cp.put(original);

        cp.flip();

        return cp;
    }
}
