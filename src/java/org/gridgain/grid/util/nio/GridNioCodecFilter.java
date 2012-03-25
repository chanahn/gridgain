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
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;

/**
 * Filter that transforms byte buffers to user-defined objects and vice-versa
 * with specified {@link GridNioParser}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridNioCodecFilter<T> extends GridNioFilterAdapter {
    /** Grid logger. */
    @GridToStringExclude
    private GridLogger log;

    /** Parser used */
    private GridNioParser<T> parser;

    /**
     * Creates a codec filter.
     *
     * @param parser Parser to use.
     * @param log Log instance to use.
     */
    public GridNioCodecFilter(GridNioParser<T> parser, GridLogger log) {
        super("GridNioCodecFilter");

        this.parser = parser;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioCodecFilter.class, this);
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
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
        try {
            ByteBuffer result = parser.encode(ses, (T) msg);

            return proceedSessionWrite(ses, result);
        }
        catch (IOException e) {
            throw new GridNioException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Failed to decode incoming message (incoming message is not a byte buffer, is " +
                "filter properly placed?): " + msg.getClass());

        try {
            ByteBuffer input = (ByteBuffer)msg;

            while (input.hasRemaining()) {
                T res = parser.decode(ses, input);
    
                if (res != null)
                    proceedMessageReceived(ses, res);
                else {
                    if (input.hasRemaining()) {
                        LT.warn(log, null, "Parser returned null but there are still unread data in input buffer (bug in " +
                            "parser code?) [parser=" + parser + ", ses=" + ses + ']');
    
                        input.position(input.limit());
                    }
                }
            }
        }
        catch (IOException e) {
            throw new GridNioException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
        return proceedSessionClose(ses);
    }
}
