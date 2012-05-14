// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;

/**
 * Class that defines the piece for application-to-network and vice-versa data conversions
 * (protocol transformations, encryption, etc.)
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public abstract class GridNioFilterAdapter implements GridNioFilter {
    /** Filter name. */
    private String name;

    /** Next filter in filter chain. */
    protected GridNioFilter nextFilter;

    /** Previous filter in filter chain. */
    protected GridNioFilter prevFilter;

    /**
     * Assigns filter name to a filter.
     *
     * @param name Filter name. Used in filter chain.
     */
    protected GridNioFilterAdapter(String name) {
        assert name != null;

        this.name = name;
    }

    /** {@inheritDoc} */
    public String toString() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridNioFilter nextFilter() {
        return nextFilter;
    }

    /** {@inheritDoc} */
    @Override public GridNioFilter previousFilter() {
        return prevFilter;
    }

    /**
     * Sets next filter in filter chain. In filter chain next filter would be more close
     * to the network layer.
     *
     * @param filter Next filter in filter chain.
     */
    @Override public void nextFilter(GridNioFilter filter) {
        nextFilter = filter;
    }

    /**
     * Sets previous filter in filter chain. In filter chain previous filter would be more close
     * to application layer.
     *
     * @param filter Previous filter in filter chain.
     */
    @Override public void previousFilter(GridNioFilter filter) {
        prevFilter = filter;
    }

    /**
     * Forwards session opened event to the next logical filter in filter chain.
     *
     * @param ses Opened session.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    @Override public void proceedSessionOpened(GridNioSession ses) throws GridException {
        checkPrevious();

        prevFilter.onSessionOpened(ses);
    }

    /**
     * Forwards session closed event to the next logical filter in filter chain.
     *
     * @param ses Closed session.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    @Override public void proceedSessionClosed(GridNioSession ses) throws GridException {
        checkPrevious();

        prevFilter.onSessionClosed(ses);
    }

    /**
     * Forwards GridNioException event to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param e GridNioException instance.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    @Override public void proceedExceptionCaught(GridNioSession ses, GridException e) throws GridException {
        checkPrevious();

        prevFilter.onExceptionCaught(ses, e);
    }

    /**
     * Forwards received message to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Received message.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    @Override public void proceedMessageReceived(GridNioSession ses, Object msg) throws GridException {
        checkPrevious();

        prevFilter.onMessageReceived(ses, msg);
    }

    /**
     * Forwards write request to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Message to send.
     * @return Write future.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    @Override public GridNioFuture<?> proceedSessionWrite(GridNioSession ses, Object msg) throws GridException {
        checkNext();

        return nextFilter.onSessionWrite(ses, msg);
    }

    /**
     * Forwards session close request to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @return Close future.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    @Override public GridNioFuture<Boolean> proceedSessionClose(GridNioSession ses) throws GridException {
        checkNext();

        return nextFilter.onSessionClose(ses);
    }

    /**
     * Checks that previous filter is set.
     *
     * @throws GridNioException If previous filter is not set.
     */
    private void checkPrevious() throws GridNioException {
        if (prevFilter == null)
            throw new GridNioException("Failed to proceed with filter call since previous filter is not set " +
                "(do you use filter outside the filter chain?): " + getClass().getName());
    }

    /**
     * Checks that next filter is set.
     *
     * @throws GridNioException If next filter is not set.
     */
    private void checkNext() throws GridNioException {
        if (nextFilter == null)
            throw new GridNioException("Failed to proceed with filter call since previous filter is not set " +
                "(do you use filter outside the filter chain?): " + getClass().getName());
    }
}
