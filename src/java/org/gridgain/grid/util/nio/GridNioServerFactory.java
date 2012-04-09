// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.nio.impl.*;

import java.net.*;
import java.util.concurrent.*;

/**
 * Factory for the {@link GridNioServer} instances.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public final class GridNioServerFactory {
    /**
     * Factory method for creating instances of {@link GridNioServer}.
     *
     * @param addr Interface address on which this server should be bound. If server should be bound on all
     *      interfaces, then {@code InetAddress.getByName(0.0.0.0)} should be used.
     * @param port Port on which this server should be bound.
     * @param lsnr Listener for server.
     * @param parser Parser to use for decoding and encoding user messages from raw data.
     * @param log Logger to use.
     * @param exec Executor within which listener will be notified.
     * @param selectorCnt Count of selectors to use. It is recommended to have the same count of selectors as
     *      count of processors on computer.
     * @param gridName Name of grid.
     * @param directBuf Flag indicating whether direct buffers should be used.
     * @return Created instance of server.
     * @throws GridException If server could not be bound on specified port.
     */
    public static <T> GridNioServer<T> createServer(InetAddress addr, int port, GridNioServerListener<T> lsnr,
        GridNioParser<T> parser, GridLogger log, Executor exec, int selectorCnt, String gridName, boolean directBuf)
        throws GridException {
        if (addr == null || lsnr == null || parser == null || log == null || exec == null)
            throw new NullPointerException();

        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("Invalid port: " + port);

        if (selectorCnt <= 0)
            throw new IllegalArgumentException("Invalid selectors count: " + selectorCnt);

        GridNioAsyncNotifyFilter async = new GridNioAsyncNotifyFilter(gridName, exec, log);
        GridNioCodecFilter<T> codec = new GridNioCodecFilter<T>(parser, log);

        return new GridNioServerImpl<T>(addr, port, log, selectorCnt, gridName, directBuf, true, lsnr, async, codec);
    }

    /**
     * Factory method for creating instances of {@link GridNioServer}.
     *
     * @param addr Interface address on which this server should be bound. If server should be bound on all
     *      interfaces, then {@code InetAddress.getByName(0.0.0.0)} should be used.
     * @param port Port on which this server should be bound.
     * @param lsnr Listener for server.
     * @param log Logger to use.
     * @param selectorCnt Count of selectors to use. It is recommended to have the same count of selectors as
     *      count of processors on computer.
     * @param gridName Name of grid.
     * @param tcpNoDelay If tcp no delay flag should be set.
     * @param directBuf Flag indicating whether direct buffers should be used.
     * @param filters Filters to be used in server.
     * @return Created instance of server.
     * @throws GridException If server could not be bound on specified port.
     */
    public static <T> GridNioServer<T> createServer(InetAddress addr, int port, GridNioServerListener<T> lsnr,
        GridLogger log, int selectorCnt, String gridName, boolean tcpNoDelay, boolean directBuf,
        GridNioFilter... filters)
        throws GridException {
        if (addr == null || lsnr == null || log == null)
            throw new NullPointerException();

        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("Invalid port: " + port);

        if (selectorCnt <= 0)
            throw new IllegalArgumentException("Invalid selectors count: " + selectorCnt);

        return new GridNioServerImpl<T>(addr, port, log, selectorCnt, gridName, tcpNoDelay, directBuf, lsnr, filters);
    }

    /**
     * Ensure no instances are created.
     */
    private GridNioServerFactory() {

    }
}
