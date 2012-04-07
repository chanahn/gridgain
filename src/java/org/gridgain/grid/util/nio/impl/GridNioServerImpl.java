// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * NIO server with improved functionality. There can be several selectors and several reading threads.
 * Supports sending data to remote clients.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridNioServerImpl<T> implements GridNioServer<T> {
    /** Time, which server will wait before retry operation. */
    private static final long ERR_WAIT_TIME = 2000;

    /** Key for buffer metadata. */
    private static final String BUF_META_NAME = UUID.randomUUID().toString();

    /** Key for message metadata. */
    private static final String NIO_OPERATION_META_NAME = UUID.randomUUID().toString();

    /** Accept worker thread. */
    @GridToStringExclude
    private GridThread acceptThread;

    /** Read worker threads. */
    private GridThread[] clientThreads;

    /** Read workers. */
    private final List<GridNioClientWorker> clientWorkers;

    /** Filter chain to use. */
    private GridNioFilterChain<T> filterChain;

    /** Logger. */
    @GridToStringExclude
    private final GridLogger log;

    /** Closed flag. */
    private volatile boolean closed;

    /** Flag indicating if this server should use direct buffers. */
    private boolean directBuf;

    /** Address, to which this server is bound. */
    private InetAddress addr;

    /** Port, to which this server is bound. */
    private int port;

    /** Index to select which thread will serve next socket channel. Using round-robin balancing. */
    @GridToStringExclude
    private int balanceIdx;

    /** Currently opened sessions. */
    private ConcurrentMap<InetSocketAddress, GridNioSession> sessions =
        new ConcurrentHashMap<InetSocketAddress, GridNioSession>();

    /** Tcp no delay flag. */
    private boolean tcpNoDelay;

    /** Static initializer ensures single-threaded execution of workaround. */
    static {
        // This is a workaround for JDK bug (NPE in Selector.open()).
        // http://bugs.sun.com/view_bug.do?bug_id=6427854
        try {
            Selector.open().close();
        }
        catch (IOException ignored) {
        }
    }

    /**
     * Constructor for test-only purposes.
     */
    protected GridNioServerImpl() {
        clientWorkers = null;
        log = null;
    }

    /**
     *
     * @param addr Address.
     * @param port Port.
     * @param log Log.
     * @param selectorCnt Count of selectors and selecting threads.
     * @param gridName Grid name.
     * @param tcpNoDelay If TCP_NODELAY option should be set to accepted sockets.
     * @param directBuf Direct buffer flag.
     * @param lsnr Listener.
     * @param filters Filters for this server.
     * @throws GridException If failed.
     */
    public GridNioServerImpl(InetAddress addr, int port, GridLogger log, int selectorCnt, String gridName,
        boolean tcpNoDelay, boolean directBuf,  GridNioServerListener<T> lsnr, GridNioFilter... filters)
        throws GridException {
        assert addr != null;
        assert port > 0 && port < 0xffff;
        assert lsnr != null;
        assert log != null;
        assert selectorCnt > 0;

        this.log = log;
        this.directBuf = directBuf;
        this.tcpNoDelay = tcpNoDelay;

        filterChain = new GridNioFilterChain<T>(log, lsnr, this, filters);

        // This method will throw exception if address already in use.
        Selector acceptSelector = createSelector(addr, port);

        // Once bind, we will not change the port in future.
        this.addr = addr;
        this.port = port;

        acceptThread = new GridThread(new GridNioAcceptWorker(gridName, "nio-acceptor", log, acceptSelector));

        clientWorkers = new ArrayList<GridNioClientWorker>(selectorCnt);
        clientThreads = new GridThread[selectorCnt];

        for (int i = 0; i < selectorCnt; i++) {
            GridNioClientWorker worker = new GridNioClientWorker(i, gridName, "nio-reader-" + i, log);

            clientWorkers.add(worker);

            clientThreads[i] = new GridThread(worker);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        filterChain.start();

        acceptThread.start();

        for (GridThread thread : clientThreads)
            thread.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (!closed) {
            closed = true;

            acceptThread.interrupt();

            for (GridThread thread : clientThreads)
                thread.interrupt();

            U.join(acceptThread, log);

            U.joinThreads(Arrays.asList(clientThreads), log);

            filterChain.stop();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> close(GridNioSession ses) {
        assert ses instanceof GridNioSessionImpl;

        GridNioSessionImpl impl = (GridNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture<Boolean>(false);

        NioOperationFuture<Boolean> fut = new NioOperationFuture<Boolean>(impl, NioOperation.CLOSE, null);

        clientWorkers.get(impl.selectorIndex()).offer(fut);

        return fut;
    }

    /** {@inheritDoc} */
    public GridNioFuture<?> send(GridNioSession ses, ByteBuffer msg) {
        assert ses instanceof GridNioSessionImpl;

        GridNioSessionImpl impl = (GridNioSessionImpl)ses;

        if (impl.closed())
            return new GridNioFinishedFuture(new IOException("Failed to send message (connection was closed): " + ses));

        NioOperationFuture<?> fut = new NioOperationFuture<Void>(impl, NioOperation.REQUIRE_WRITE, msg);

        int msgCnt = impl.offerFuture(fut);

        if (impl.closed())
            fut.connectionClosed();

        // Change from 0 to 1 means that worker thread should be waken up.
        if (msgCnt == 1)
            clientWorkers.get(impl.selectorIndex()).offer(fut);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridNioSession session(InetSocketAddress rmtAddr) {
        return sessions.get(rmtAddr);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNioSession> sessions() {
        return sessions.values();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioServerImpl.class, this);
    }

    /**
     * Creates selector and binds server socket to a given address and port. If address is null
     * then will not bind any address and just creates a selector.
     *
     * @param addr Local address to listen on.
     * @param port Local port to listen on.
     * @return Created selector.
     * @throws GridException If selector could not be created or port is already in use.
     */
    private Selector createSelector(@Nullable InetAddress addr, int port) throws GridException {
        Selector selector = null;

        ServerSocketChannel srvrCh = null;

        try {
            // Create a new selector
            selector = SelectorProvider.provider().openSelector();

            if (addr != null) {
                // Create a new non-blocking server socket channel
                srvrCh = ServerSocketChannel.open();

                srvrCh.configureBlocking(false);

                // Bind the server socket to the specified address and port
                srvrCh.socket().bind(new InetSocketAddress(addr, port));

                // Register the server socket channel, indicating an interest in
                // accepting new connections
                srvrCh.register(selector, SelectionKey.OP_ACCEPT);
            }

            return selector;
        }
        catch (IOException e) {
            U.close(srvrCh, log);
            U.close(selector, log);

            throw new GridException("Failed to initialize NIO selector.", e);
        }
    }

    /**
     * Adds registration request for a given socket channel to the next selector. Next selector
     * is selected according to a round-robin algorithm.
     *
     * @param sockCh Socket channel to be registered on one of the selectors.
     */
    private void addRegistrationReq(SocketChannel sockCh) {
        NioOperationFuture req = new NioOperationFuture(sockCh);

        clientWorkers.get(balanceIdx).offer(req);

        balanceIdx++;

        if (balanceIdx == clientWorkers.size())
            balanceIdx = 0;
    }

    /**
     * Thread performing only read operations from the channel.
     */
    private class GridNioClientWorker extends GridWorker {
        /** Queue of change requests on this selector. */
        private GridConcurrentLinkedDeque<NioOperationFuture> changeReqs = new GridConcurrentLinkedDeque<NioOperationFuture>();

        /** Buffer for reading. */
        private final ByteBuffer readBuf;

        /** Selector to select read events. */
        private Selector selector;

        /** Worker index. */
        private int idx;

        /**
         * @param idx Index of this worker in server's array.
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Logger.
         * @throws org.gridgain.grid.GridException If selector could not be created.
         */
        protected GridNioClientWorker(int idx, String gridName, String name, GridLogger log) throws GridException {
            super(gridName, name, log);

            selector = createSelector(null, 0);

            readBuf = directBuf ? ByteBuffer.allocateDirect(8 << 10) : ByteBuffer.allocate(8 << 10);

            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            boolean reset = false;

            while (!closed) {
                try {
                    if (reset)
                        selector = createSelector(null, 0);

                    bodyInternal();
                }
                catch (GridException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        U.error(log, "Failed to read data from remote connection (will wait for " +
                            ERR_WAIT_TIME + "ms).", e);

                        U.sleep(ERR_WAIT_TIME);

                        reset = true;
                    }
                }
            }
        }

        /**
         * Adds socket channel to the registration queue and wakes up reading thread.
         *
         * @param req Change request.
         */
        private void offer(NioOperationFuture req) {
            changeReqs.offer(req);

            selector.wakeup();
        }

        /**
         * Processes read and write events and registration requests.
         *
         * @throws GridException If IOException occurred or thread was unable to add worker to workers pool.
         */
        @SuppressWarnings("unchecked")
        private void bodyInternal() throws GridException {
            try {
                while (!closed && selector.isOpen()) {
                    NioOperationFuture req;

                    while ((req = changeReqs.poll()) != null) {
                        switch (req.operation()) {
                            case REGISTER: {
                                register(req.socketChannel());

                                req.onDone();

                                break;
                            }

                            case REQUIRE_WRITE: {
                                //Just register write key.
                                SelectionKey key = req.session().key();

                                if (key.isValid())
                                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                                break;
                            }

                            case CLOSE: {
                                if (close(req.session(), null))
                                    req.onDone(true);
                                else
                                    req.onDone(false);

                                break;
                            }
                        }
                    }

                    // Wake up every 2 seconds to check if closed.
                    if (selector.select(2000) > 0)
                        // Walk through the ready keys collection and process network events.
                        processSelectedKeys(selector.selectedKeys());
                }
            }
            // Ignore this exception as thread interruption is equal to 'close' call.
            catch (ClosedByInterruptException e) {
                if (log.isDebugEnabled())
                    log.debug("Closing selector due to thread interruption: " + e.getMessage());
            }
            catch (ClosedSelectorException e) {
                throw new GridException("Selector got closed while active.", e);
            }
            catch (IOException e) {
                throw new GridException("Failed to select events on selector.", e);
            }
            finally {
                if (selector.isOpen()) {
                    if (log.isDebugEnabled())
                        log.debug("Closing all connected client sockets.");

                    // Close all channels registered with selector.
                    for (SelectionKey key : selector.keys())
                        close((GridNioSessionImpl)key.attachment(), null);

                    if (log.isDebugEnabled())
                        log.debug("Closing NIO selector.");

                    U.close(selector, log);
                }
            }
        }

        /**
         * Processes keys selected by a selector.
         *
         * @param keys Selected keys.
         * @throws ClosedByInterruptException If this thread was interrupted while reading data.
         */
        private void processSelectedKeys(Set<SelectionKey> keys) throws ClosedByInterruptException {
            for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext(); ) {
                SelectionKey key = iter.next();

                iter.remove();

                // Was key closed?
                if (!key.isValid())
                    continue;

                GridNioSessionImpl ses = (GridNioSessionImpl) key.attachment();

                assert ses != null;

                try {
                    if (key.isReadable())
                        processRead(key);
                    else if (key.isWritable())
                        processWrite(key);
                }
                catch (ClosedByInterruptException e) {
                    // This exception will be handled in bodyInternal() method.
                    throw e;
                }
                catch (IOException e) {
                    if (!closed)
                        U.warn(log, "Failed to process selector key (will close): " + ses, e);

                    close(ses, new GridNioException(e));
                }
            }
        }

        /**
         * Registers given socket channel to the selector, creates a session and notifies the listener.
         *
         * @param sockCh Socket channel to register.
         */
        private void register(SocketChannel sockCh) {
            assert sockCh != null;

            Socket sock = sockCh.socket();

            try {
                final GridNioSessionImpl ses = new GridNioSessionImpl(idx, filterChain,
                    (InetSocketAddress)sock.getLocalSocketAddress(), (InetSocketAddress)sock.getRemoteSocketAddress(),
                    log);

                SelectionKey key = sockCh.register(selector, SelectionKey.OP_READ, ses);

                ses.key(key);

                GridNioSession old = sessions.put(ses.remoteAddress(), ses);

                assert old == null : "Session remote address collision";

                try {
                    filterChain.onSessionOpened(ses);
                }
                catch (GridException e) {
                    close(ses, e);
                }
            }
            catch (ClosedChannelException e) {
                U.warn(log, "Failed to register accepted socket channel to selector (channel was closed): "
                    + sock.getRemoteSocketAddress(), e);
            }
        }

        /**
         * Closes the ses and all associated resources, then notifies the listener.
         *
         * @param ses Session to be closed.
         * @param e Exception to be passed to the listener, if any.
         * @return {@code True} if this call closed the ses.
         */
        private boolean close(final GridNioSessionImpl ses,  @Nullable final GridException e) {
            SelectionKey key = ses.key();

            // Shutdown input and output so that remote client will see correct socket close.
            Socket sock = ((SocketChannel)key.channel()).socket();

            if (ses.setClosed()) {
                try {
                    try {
                        sock.shutdownInput();
                    }
                    catch (IOException ignored) {
                        // No-op.
                    }

                    try {
                        sock.shutdownOutput();
                    }
                    catch (IOException ignored) {
                        // No-op.
                    }
                }
                finally {
                    U.close(key, log);
                    U.close(sock, log);
                }

                GridNioSession old = sessions.remove(ses.remoteAddress());

                assert old == ses : "Invalid session removed on close";

                if (e != null)
                    filterChain.onExceptionCaught(ses, e);

                try {
                    filterChain.onSessionClosed(ses);
                }
                catch (GridException e1) {
                    filterChain.onExceptionCaught(ses, e1);
                }

                ses.removeMeta(BUF_META_NAME);

                // Since ses is in closed state, no write requests will be added.
                NioOperationFuture<?> fut = ses.removeMeta(NIO_OPERATION_META_NAME);

                if (fut != null)
                    fut.connectionClosed();

                while ((fut = (NioOperationFuture<?>)ses.pollFuture()) != null)
                    fut.connectionClosed();

                return true;
            }

            return false;
        }

        /**
         * Processes read-available event on the key.
         *
         * @param key Key that is ready to be read.
         * @throws IOException If key read failed.
         */
        private void processRead(SelectionKey key) throws IOException {
            SocketChannel sockCh = (SocketChannel)key.channel();

            final GridNioSessionImpl ses = (GridNioSessionImpl)key.attachment();

            // Reset buffer to read bytes up to its capacity.
            readBuf.clear();

            // Attempt to read off the channel
            int cnt = sockCh.read(readBuf);

            if (cnt == -1) {
                if (log.isDebugEnabled())
                    log.debug("Remote client closed connection: " + ses);

                close(ses, null);

                return;
            }
            else if (cnt == 0)
                return;

            ses.bytesReceived(cnt);

            // Sets limit to current position and
            // resets position to 0.
            readBuf.flip();

            try {
                if (readBuf.remaining() > 0) {
                    filterChain.onMessageReceived(ses, readBuf);

                    if (readBuf.remaining() > 0) {
                        LT.warn(log, null, "Read buffer contains data after filter chain processing (will flush " +
                            "remaining bytes) [ses=" + ses + ", remainingCnt=" + readBuf.remaining() + ']');

                        readBuf.clear();
                    }
                }
            }
            catch (GridException e) {
                close(ses, e);
            }
        }

        /**
         * Processes write-ready event on the key.
         *
         * @param key Key that is ready to be written.
         * @throws IOException If write failed.
         */
        @SuppressWarnings("unchecked")
        private void processWrite(SelectionKey key) throws IOException {
            SocketChannel sockCh = (SocketChannel)key.channel();

            final GridNioSessionImpl ses = (GridNioSessionImpl)key.attachment();

            while (true) {
                ByteBuffer buf = ses.removeMeta(BUF_META_NAME);

                NioOperationFuture<?> req = ses.removeMeta(NIO_OPERATION_META_NAME);

                // Check if there were any pending data from previous writes.
                if (buf == null) {
                    assert req == null;

                    req = (NioOperationFuture<?>)ses.pollFuture();

                    if (req == null) {
                        key.interestOps(SelectionKey.OP_READ);

                        break;
                    }

                    buf = req.message();
                }

                int cnt = sockCh.write(buf);

                ses.bytesSent(cnt);

                if (buf.remaining() > 0) {
                    //Not all data was written.
                    ses.addMeta(BUF_META_NAME, buf);
                    ses.addMeta(NIO_OPERATION_META_NAME, req);

                    break;
                }
                else {
                    // Message was successfully written.
                    assert req != null;

                    req.onDone();
                }
            }
        }
    }

    /**
     * A separate thread that will accept incoming connections and schedule read to some worker.
     */
    private class GridNioAcceptWorker extends GridWorker {
        /** Selector for this thread. */
        private Selector selector;

        /**
         * @param gridName Grid name.
         * @param name Thread name.
         * @param log Log.
         * @param selector Which will accept incoming connections.
         */
        protected GridNioAcceptWorker(String gridName, String name, GridLogger log, Selector selector) {
            super(gridName, name, log);

            this.selector = selector;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            boolean reset = false;

            while (!closed && !Thread.currentThread().isInterrupted()) {
                try {
                    if (reset)
                        selector = createSelector(addr, port);

                    accept();
                }
                catch (GridException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        U.error(log, "Failed to accept remote connection (will wait for " + ERR_WAIT_TIME + "ms).", e);

                        U.sleep(ERR_WAIT_TIME);

                        reset = true;
                    }
                }
            }
        }

        /**
         * Accepts connections and schedules them for processing by one of read workers.
         *
         * @throws GridException If failed.
         */
        private void accept() throws GridException {
            try {
                while (!closed && selector.isOpen() && !Thread.currentThread().isInterrupted()) {
                    // Wake up every 2 seconds to check if closed.
                    if (selector.select(2000) > 0)
                        // Walk through the ready keys collection and process date requests.
                        processSelectedKeys(selector.selectedKeys());
                }
            }
            // Ignore this exception as thread interruption is equal to 'close' call.
            catch (ClosedByInterruptException e) {
                if (log.isDebugEnabled())
                    log.debug("Closing selector due to thread interruption [srvr=" + this +
                        ", err=" + e.getMessage() + ']');
            }
            catch (ClosedSelectorException e) {
                throw new GridException("Selector got closed while active: " + this, e);
            }
            catch (IOException e) {
                throw new GridException("Failed to accept connection: " + this, e);
            }
            finally {
                if (selector.isOpen()) {
                    if (log.isDebugEnabled())
                        log.debug("Closing all listening sockets.");

                    // Close all channels registered with selector.
                    for (SelectionKey key : selector.keys())
                        U.close(key.channel(), log);

                    if (log.isDebugEnabled())
                        log.debug("Closing NIO selector.");

                    U.close(selector, log);
                }
            }
        }

        /**
         * Processes selected accept requests for server socket.
         *
         * @param keys Selected keys from acceptor.
         * @throws IOException If accept failed or IOException occurred while configuring channel.
         */
        private void processSelectedKeys(Set<SelectionKey> keys) throws IOException {
            for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext();) {
                SelectionKey key = iter.next();

                iter.remove();

                // Was key closed?
                if (!key.isValid())
                    continue;

                if (key.isAcceptable()) {
                    // The key indexes into the selector so we
                    // can retrieve the socket that's ready for I/O
                    ServerSocketChannel srvrCh = (ServerSocketChannel)key.channel();

                    SocketChannel sockCh = srvrCh.accept();

                    sockCh.configureBlocking(false);
                    sockCh.socket().setTcpNoDelay(tcpNoDelay);

                    if (log.isDebugEnabled())
                        log.debug("Accepted new client connection: " + sockCh.socket().getRemoteSocketAddress());

                    addRegistrationReq(sockCh);
                }
            }
        }
    }

    /**
     * Asynchronous operation that may be requested on selector.
     */
    private enum NioOperation {
        /** Register read key selection. */
        REGISTER,

        /** Register write key selection. */
        REQUIRE_WRITE,

        /** Close key. */
        CLOSE
    }

    /**
     * Class for requesting write and session close operations.
     */
    private class NioOperationFuture<R> extends GridNioFutureImpl<R> {
        /** Socket channel in register request. */
        @GridToStringExclude
        private SocketChannel sockCh;

        /** Session to perform operation on. */
        private GridNioSessionImpl ses;

        /** Is it a close request or a write request. */
        private NioOperation op;

        /** User message to send. */
        private ByteBuffer msg;

        /**
         * Creates registration request for a given socket channel.
         *
         * @param sockCh Socket channel to register on selector.
         */
        private NioOperationFuture(SocketChannel sockCh) {
            this.sockCh = sockCh;

            op = NioOperation.REGISTER;
        }

        /**
         * Creates change request.
         *
         * @param ses Session to change.
         * @param op Requested operation.
         * @param msg Object to write.
         */
        private NioOperationFuture(GridNioSessionImpl ses, NioOperation op, ByteBuffer msg) {
            assert ses != null;
            assert op != null;
            assert op != NioOperation.REGISTER;

            this.ses = ses;
            this.op = op;
            this.msg = msg;
        }

        /**
         * @return Requested change operation.
         */
        private NioOperation operation() {
            return op;
        }

        /**
         * @return Socket channel for register request.
         */
        private SocketChannel socketChannel() {
            return sockCh;
        }

        /**
         * @return Session for this change request.
         */
        private GridNioSessionImpl session() {
            return ses;
        }

        /**
         * @return Message for write request.
         */
        private ByteBuffer message() {
            return msg;
        }

        /**
         * Applicable to write futures only. Fails future with corresponding IOException.
         */
        private void connectionClosed() {
            assert op == NioOperation.REQUIRE_WRITE;
            assert ses != null;

            onDone(new IOException("Failed to send message (connection was closed): " + ses));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NioOperationFuture.class, this);
        }
    }
}
