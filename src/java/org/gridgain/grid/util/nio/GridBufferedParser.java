// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * This class implements stream parser based on {@link GridNioServerBuffer}.
 * <p>
 * The rule for this parser is that every message sent over the stream is prepended with
 * 4-byte integer header containing message size. So, the stream structure is as follows:
 * <pre>
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 *     | MSG_SIZE  |   MESSAGE  | MSG_SIZE  |   MESSAGE  |
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 * <pre/>
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridBufferedParser implements GridNioParser<byte[]> {
    /** Buffer metadata name */
    private static final String BUF_META_NAME = UUID.randomUUID().toString();

    /** {@inheritDoc} */
    @Override public byte[] decode(GridNioSession ses, ByteBuffer buf) throws IOException, GridException {
        GridNioServerBuffer nioBuf = ses.meta(BUF_META_NAME);

        // Decode for a given session is called per one thread, so there should not be any concurrency issues.
        // However, we make some additional checks.
        if (nioBuf == null) {
            nioBuf = new GridNioServerBuffer();

            GridNioServerBuffer old = ses.putMetaIfAbsent(BUF_META_NAME, nioBuf);

            assert old == null : "Concurrent thread put server buffer to metadata";
        }

        while (buf.remaining() > 0) {
            nioBuf.read(buf);

            if (nioBuf.isFilled()) {
                // Copy array so we can continue to write data to the same buffer.
                byte[] data = nioBuf.getMessageBytes().array();

                nioBuf.reset();

                return data;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, byte[] msg) throws IOException, GridException {
        ByteBuffer res = ByteBuffer.allocate(msg.length + 4);

        res.putInt(msg.length);
        res.put(msg);

        res.flip();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridBufferedParser.class.getSimpleName();
    }
}
