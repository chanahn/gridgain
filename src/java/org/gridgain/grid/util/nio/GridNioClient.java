package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.*;

/**
 * Grid client for NIO server.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.1c.21112011
 */
public class GridNioClient {
    /** Socket. */
    private final Socket sock;

    /** Cached byte buffer. */
    private final GridByteArrayList bytes = new GridByteArrayList(512);

    /** Time when this client was last used. */
    private volatile long lastUsed = System.currentTimeMillis();

    /** Reservations. */
    private final AtomicInteger reserves = new AtomicInteger();

    /**
     * @param addr Address.
     * @param port Port.
     * @param localHost Local address.
     * @throws GridException If failed.
     */
    public GridNioClient(InetAddress addr, int port, InetAddress localHost) throws GridException {
        assert addr != null;
        assert port > 0 && port < 0xffff;
        assert localHost != null;

        try {
            sock = new Socket(addr, port, localHost, 0);
        }
        catch (IOException e) {
            throw new GridException("Failed to connect to remote host [addr=" + addr + ", port=" + port +
                ", localHost=" + localHost + ']', e);
        }
    }

    /**
     * @return {@code True} if client has been closed by this call,
     *      {@code false} if failed to close client (due to concurrent reservation or concurrent close).
     */
    public boolean close() {
        if (reserves.compareAndSet(0, -1)) {
            // Future reservation is not possible.
            U.closeQuiet(sock);

            return true;
        }

        return false;
    }

    /**
     * @return {@code True} if client is closed;
     */
    public boolean closed() {
        return reserves.get() == -1;
    }

    /**
     * @return {@code True} if client was reserved, {@code false} otherwise.
     */
    public boolean reserve() {
        while (true) {
            int r = reserves.get();

            if (r == -1)
                return false;

            if (reserves.compareAndSet(r, r + 1))
                return true;
        }
    }

    /**
     * Releases this client by decreasing reservations.
     */
    public void release() {
        int r = reserves.decrementAndGet();

        assert r >= 0;
    }

    /**
     * @return {@code True} if client was reserved.
     */
    public boolean reserved() {
        return reserves.get() > 0;
    }

    /**
     * Gets idle time of this client.
     *
     * @return Idle time of this client.
     */
    public long getIdleTime() {
        return System.currentTimeMillis() - lastUsed;
    }

    /**
     * @param data Data to send.
     * @param len Size of data in bytes.
     * @throws GridException If failed.
     */
    public synchronized void sendMessage(byte[] data, int len) throws GridException {
        lastUsed = System.currentTimeMillis();

        bytes.reset();

        // Allocate 4 bytes for size.
        bytes.add(len);

        bytes.add(data, 0, len);

        try {
            // We assume that this call does not return until the message
            // is fully sent.
            sock.getOutputStream().write(bytes.getInternalArray(), 0, bytes.getSize());
        }
        catch (IOException e) {
            throw new GridException("Failed to send message to remote node: " + sock.getRemoteSocketAddress(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioClient.class, this);
    }
}
