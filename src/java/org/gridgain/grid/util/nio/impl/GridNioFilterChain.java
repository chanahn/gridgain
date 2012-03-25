// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;

import java.nio.*;

/**
 * Filter chain implementation for nio server filters.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridNioFilterChain<T> extends GridNioFilterAdapter {
    /** Grid logger. */
    private GridLogger log;

    /** Listener that will be used to notify on server events. */
    private GridNioServerListener<T> lsnr;

    /** Server implementation that will send outgoing messages over network. */
    private GridNioServerImpl<T> srv;

    /** Head of filter list. */
    private GridNioFilterAdapter head;

    /** Tail of filter list. */
    private GridNioFilter tail;

    /** Cached value for toString method. */
    private volatile String str;

    /**
     * Constructor.
     *
     * @param log Logger instance.
     * @param lsnr Message listener.
     * @param srv Server implementation.
     * @param filters Filters that will be in chain.
     */
    public GridNioFilterChain(GridLogger log, GridNioServerListener<T> lsnr, GridNioServerImpl<T> srv,
        GridNioFilter... filters) {
        super("FilterChain");

        this.log = log;
        this.lsnr = lsnr;
        this.srv = srv;

        GridNioFilter prev;

        tail = prev = new TailFilter();

        for (GridNioFilter filter : filters) {
            prev.nextFilter(filter);
            
            filter.previousFilter(prev);
            
            prev = filter;
        }

        head = new HeadFilter();

        prev.nextFilter(head);

        head.previousFilter(prev);
    }

    /** {@inheritDoc} */
    public String toString() {
        if (str == null) {
            StringBuilder res = new StringBuilder("FilterChain[filters=[");

            GridNioFilter ref = tail.nextFilter();

            while (ref != head) {
                res.append(ref);

                ref = ref.nextFilter();

                if (ref != head)
                    res.append(", ");
            }

            // It is OK if this variable will be rewritten concurrently.
            str = res.toString();
        }

        return str;
    }

    /**
     * Starts all filters in order from application layer to the network layer.
     */
    @Override public void start() {
        GridNioFilter ref = tail.nextFilter();

        // Walk through the linked list and start all the filters.
        while (ref != head) {
            ref.start();

            ref = ref.nextFilter();
        }
    }

    /**
     * Stops all filters in order from network layer to the application layer.
     */
    @Override public void stop() {
        GridNioFilter ref = head.previousFilter();

        // Walk through the linked list and stop all the filters.
        while (ref != tail) {
            ref.stop();

            ref = ref.previousFilter();
        }
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session that was created.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public void onSessionOpened(GridNioSession ses) throws GridException {
        head.onSessionOpened(ses);
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session that was closed.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public void onSessionClosed(GridNioSession ses) throws GridException {
        head.onSessionClosed(ses);
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session in which GridNioException was caught.
     * @param e GridException instance.
     */
    @Override public void onExceptionCaught(GridNioSession ses, GridException e) {
        try {
            head.onExceptionCaught(ses, e);
        }
        catch (Exception ex) {
            LT.warn(log, ex, "Failed to forward GridNioException to filter chain [ses=" + ses + ", e=" + e + ']');
        }
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session in which message was received.
     * @param msg Received message.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
        head.onMessageReceived(ses, msg);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to which message should be written.
     * @param msg Message to write.
     * @return Send future.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
        return tail.onSessionWrite(ses, msg);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to close.
     * @return Close future.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
        return tail.onSessionClose(ses);
    }

    /**
     * Tail filter that handles all incoming events.
     */
    private class TailFilter extends GridNioFilterAdapter {
        /**
         * Constructs tail filter.
         */
        private TailFilter() {
            super("TailFilter");
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) {
            lsnr.onConnected(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) {
            lsnr.onDisconnected(ses, null);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, GridException ex) {
            lsnr.onDisconnected(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg)
            throws GridException {
            return proceedSessionWrite(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
            return proceedSessionClose(ses);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) {
            lsnr.onMessage(ses, (T) msg);
        }
    }

    /**
     * Head filter that handles write and close sesssion requests.
     */
    private class HeadFilter extends GridNioFilterAdapter {
        /**
         * Constructs head filter.
         */
        private HeadFilter() {
            super("HeadFilter");
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) throws GridException {
            proceedSessionOpened(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) throws GridException {
            proceedSessionClosed(ses);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException {
            proceedExceptionCaught(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) {
            return srv.send(ses, (ByteBuffer)msg);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
            proceedMessageReceived(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) {
            return srv.close(ses);
        }
    }
}
