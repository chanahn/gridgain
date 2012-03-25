// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Distributed query manager.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridCacheDistributedQueryManager<K, V> extends GridCacheQueryManager<K, V> {
    /** */
    private static final int MAX_CANCEL_IDS = 1000;

    /** Query response frequency. */
    private static final long RESEND_FREQ = 3000;

    /** Query response attempts. */
    private static final int RESEND_ATTEMPTS = 5;

    /** Prefix for communication topic. */
    private static final String TOPIC_PREFIX = "QUERY";

    /** Timeout for waiting ordered messages. */
    private static final int ORDERED_MSG_TIMEOUT = 30 * 1000;

    /** {request id -> thread} */
    private ConcurrentHashMap<Long, Thread> threads = new ConcurrentHashMap<Long, Thread>();

    /** {request id -> future} */
    private ConcurrentHashMap<Long, GridCacheDistributedQueryFuture<K, V, ?>> futs =
        new ConcurrentHashMap<Long, GridCacheDistributedQueryFuture<K, V, ?>>();

    /** Received requests to cancel. */
    private Collection<CancelMessageId> cancelIds =
        new GridBoundedConcurrentOrderedSet<CancelMessageId>(MAX_CANCEL_IDS);

    /** Canceled queries. */
    private Collection<Long> cancelled = new GridBoundedConcurrentOrderedSet<Long>(MAX_CANCEL_IDS);

    /** Query response handler. */
    private CI2<UUID, GridCacheQueryResponse<K, V>> resHandler = new CI2<UUID, GridCacheQueryResponse<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheQueryResponse<K, V> res) {
            processQueryResponse(nodeId, res);
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        super.start0();

        assert cctx.config().getCacheMode() != LOCAL;

        cctx.io().addHandler(GridCacheQueryRequest.class, new CI2<UUID, GridCacheQueryRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheQueryRequest<K, V> req) {
                processQueryRequest(nodeId, req);
            }
        });

        cctx.events().addListener(new GridLocalEventListener() {
                @Override public void onEvent(GridEvent evt) {
                    GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

                    for (GridCacheDistributedQueryFuture fut : futs.values())
                        fut.onNodeLeft(discoEvt.eventNodeId());
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        super.printMemoryStats();

        X.println(">>>   threadsSize: " + threads.size());
        X.println(">>>   futsSize: " + futs.size());
    }

    /**
     * Removes query future from futures map.
     *
     * @param reqId Request id.
     * @param fut Query future.
     */
    protected void addQueryFuture(long reqId, GridCacheDistributedQueryFuture<K, V, ?> fut) {
        futs.put(reqId, fut);
    }

    /**
     * Removes query future from futures map.
     *
     * @param reqId Request id.
     */
    protected void removeQueryFuture(long reqId) {
        futs.remove(reqId);
    }

    /**
     * Gets query future from futures map.
     *
     * @param reqId Request id.
     * @return Found future or null.
     */
    protected GridCacheDistributedQueryFuture<K, V, ?> getQueryFuture(long reqId) {
        return futs.get(reqId);
    }

    /**
     * Processes cache query request.
     *
     * @param senderId Sender node id.
     * @param req Query request.
     */
    @SuppressWarnings({"unchecked"})
    private void processQueryRequest(UUID senderId, GridCacheQueryRequest<K, V> req) {
        if (!busyLock.enterBusy()) {
            if (log.isDebugEnabled())
                log.debug("Received query request while stopping or after shutdown (will ignore).");

            return;
        }

        try {
            if (req.cancel()) {
                Thread thread = threads.remove(req.id());

                if (thread != null)
                    thread.interrupt();
                else
                    cancelIds.add(new CancelMessageId(req.id(), senderId));
            }
            else {
                if (!cancelIds.contains(new CancelMessageId(req.id(), senderId))) {
                    GridCacheQueryResponse res = null;

                    if (!F.eq(req.cacheName(), cctx.name()))
                        res = new GridCacheQueryResponse(req.id(), req.queryId(), true);

                    if (res == null) {
                        threads.put(req.id(), Thread.currentThread());

                        try {
                            runQuery(distributedQueryInfo(senderId, req));
                        }
                        catch (Throwable e) {
                            log().error("Failed to run query.", e);

                            sendQueryResponse(senderId, new GridCacheQueryResponse(req.id(), req.queryId(),
                                e.getCause()));
                        }
                        finally {
                            threads.remove(req.id());
                        }
                    }
                    else
                        sendQueryResponse(senderId, res);
                }
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param senderId Sender node id.
     * @param req Query request.
     * @return Query info.
     */
    @SuppressWarnings({"unchecked"})
    private GridCacheQueryInfo<K, V> distributedQueryInfo(UUID senderId, GridCacheQueryRequest req) {
        GridPredicate<K> keyFilter =
            req.keyFilter() == null ? null : (GridPredicate<K>)req.keyFilter().apply(req.closureArguments());

        GridPredicate<V> valFilter =
            req.valueFilter() == null ? null : (GridPredicate<V>)req.valueFilter().apply(req.closureArguments());

        GridPredicate<GridCacheEntry<K, V>> prjPred =
            req.projectionFilter() == null ? F.<GridCacheEntry<K, V>>alwaysTrue() : req.projectionFilter();

        GridClosure<V, Object> trans =
            req.transformer() == null ? null : (GridClosure<V, Object>)req.transformer().apply(req.closureArguments());

        GridReducer<Map.Entry<K, V>, Object> rdc = req.reducer() == null ? null :
            (GridReducer<Map.Entry<K, V>, Object>)req.reducer().apply(req.closureArguments());

        GridCacheQueryAdapter<K, V> query = new GridCacheQueryAdapter<K, V>(
            cctx,
            req.queryId(),
            req.type(),
            req.clause(),
            req.className(),
            prjPred,
            req.cloneValues() ? EnumSet.of(GridCacheFlag.CLONE) : EnumSet.noneOf(GridCacheFlag.class));

        query.arguments(req.arguments());

        query.readThrough(req.readThrough());

        return new GridCacheQueryInfo<K, V>(
            false,
            req.single(),
            keyFilter,
            valFilter,
            prjPred,
            trans,
            rdc,
            query,
            req.pageSize(),
            req.readThrough(),
            req.cloneValues(),
            req.includeBackups(),
            null,
            senderId,
            req.id()
        );
    }

    /**
     * Sends cache query response.
     *
     * @param nodeId Node to send response.
     * @param res Cache query response.
     * @return {@code true} if response was sent, {@code false} otherwise.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean sendQueryResponse(UUID nodeId, GridCacheQueryResponse<K, V> res) {
        GridNode node = cctx.node(nodeId);

        if (node == null)
            return false;

        int attempt = 1;

        GridException err = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                String topic = orderedTopic(res.requestId(), nodeId);

                if (log().isDebugEnabled())
                    log().debug("Send query response: " + res);

                cctx.io().sendOrderedMessage(
                    node,
                    topic,
                    cctx.io().messageId(topic, nodeId),
                    res,
                    ORDERED_MSG_TIMEOUT
                );

                return true;
            }
            catch (GridTopologyException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send query response since node left grid [nodeId=" + nodeId +
                        ", res=" + res + "]");

                return false;
            }
            catch (GridException e) {
                if (err == null)
                    err = e;

                if (Thread.currentThread().isInterrupted())
                    break;

                if (attempt < RESEND_ATTEMPTS) {
                    if (log().isDebugEnabled())
                        log().debug("Failed to send queries response (will try again) [nodeId=" + nodeId + ", res=" +
                            res + ", attempt=" + attempt + ", err=" + e + "]");

                    if (!Thread.currentThread().isInterrupted())
                        try {
                            Thread.sleep(RESEND_FREQ);
                        }
                        catch (InterruptedException e1) {
                            log().error("Waiting for queries response resending was interrupted (response will not be sent) " +
                                "[nodeId=" + nodeId + ", response=" + res + "]", e1);

                            return false;
                        }
                }
                else {
                    log().error("Failed to send cache response [nodeId=" + nodeId + ", response=" + res + "]", err);

                    return false;
                }
            }

            attempt++;
        }

        return false;
    }

    /**
     * Processes cache query response.
     *
     * @param senderId Sender node id.
     * @param res Query response.
     */
    private void processQueryResponse(UUID senderId, GridCacheQueryResponse res) {
        if (log().isDebugEnabled())
            log().debug("Received query response: " + res);

        if (!busyLock.enterBusy()) {
            if (log().isDebugEnabled())
                log().debug("Received query response while stopping or after shutdown (will ignore).");

            return;
        }

        try {
            GridCacheQueryFutureAdapter fut = getQueryFuture(res.requestId());

            if (fut != null)
                fut.onPage(senderId, res.data(), res.error(), res.isFinished());
            else if (!cancelled.contains(res.requestId()))
                U.warn(log(), "Received response for finished or unknown query [remoteNodeId=" + senderId +
                    ", response=" + res + ']');
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override void onQueryFutureCanceled(long reqId) {
        cancelled.add(reqId);
    }

    /** {@inheritDoc} */
    @Override void onCancelAtStop() {
        super.onCancelAtStop();

        for (GridCacheQueryFutureAdapter fut : futs.values())
            try {
                fut.cancel();
            }
            catch (GridException e) {
                U.error(log, "Failed to cancel running query future: " + fut, e);
            }

        U.interrupt(threads.values());
    }

    /** {@inheritDoc} */
    @Override void onWaitAtStop() {
        super.onWaitAtStop();

        // Wait till all requests will be finished.
        for (GridCacheQueryFutureAdapter fut : futs.values())
            try {
                fut.get();
            }
            catch (GridException e) {
                if (log.isDebugEnabled())
                    log.debug("Received query error while waiting for query to finish [queryFuture= " + fut +
                        ", error= " + e + ']');
            }
    }

    /** {@inheritDoc} */
    @Override protected boolean onPageReady(boolean loc, GridCacheQueryInfo<K, V> qryInfo, Collection<?> data,
        boolean finished, Throwable e) {
        GridCacheQueryFutureAdapter fut = qryInfo.localQueryFuture();

        if (loc)
            assert fut != null;

        if (e != null) {
            if (loc)
                fut.onPage(null, null, e, true);
            else
                sendQueryResponse(qryInfo.senderId(),
                    new GridCacheQueryResponse<K, V>(qryInfo.requestId(), qryInfo.query().id(), e));

            return true;
        }

        assert data != null;

        if (loc)
            fut.onPage(null, data, null, finished);
        else {
            GridCacheQueryResponse<K, V> res = new GridCacheQueryResponse<K, V>(qryInfo.requestId(), qryInfo.query().id());

            res.data(data);

            res.finished(finished);

            if (!sendQueryResponse(qryInfo.senderId(), res))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public <R> GridCacheQueryFuture<R> queryLocal(GridCacheQueryBaseAdapter<K, V> qry, boolean single,
        boolean rmtRdcOnly, @Nullable GridInClosure2<UUID, Collection<R>> pageLsnr) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheDistributedQueryFuture<K, V, R> fut =
            new GridCacheDistributedQueryFuture<K, V, R>(cctx, qry, null, true, single, rmtRdcOnly, pageLsnr);

        try {
            validateQuery(qry);
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <R> GridCacheQueryFuture<R> queryDistributed(GridCacheQueryBaseAdapter<K, V> qry,
        Collection<GridRichNode> nodes, boolean single, boolean rmtOnly,
        @Nullable GridInClosure2<UUID, Collection<R>> pageLsnr) {
        assert cctx.config().getCacheMode() != LOCAL;

        final GridCacheDistributedQueryFuture<K, V, R> fut =
            new GridCacheDistributedQueryFuture<K, V, R>(cctx, qry, nodes, false, single, rmtOnly, pageLsnr);

        try {
            validateQuery(qry);

            GridCacheQueryRequest<K, V> req = new GridCacheQueryRequest<K, V>(
                cctx.io().nextIoId(),
                cctx.name(),
                qry.id(),
                qry.type(),
                qry.clause(),
                qry.className(),
                qry.remoteKeyFilter(),
                qry.remoteValueFilter(),
                qry.projectionFilter(),
                qry instanceof GridCacheReduceQueryAdapter ?
                    ((GridCacheReduceQueryAdapter)qry).remoteReducer() : null,
                qry instanceof GridCacheTransformQueryAdapter ?
                    ((GridCacheTransformQueryAdapter)qry).remoteTransformer() : null,
                qry.pageSize(),
                qry.readThrough(),
                qry.cloneValues(),
                qry.includeBackups(),
                qry.arguments(),
                qry.getClosureArguments(),
                single);

            fut.requestId(req.id());

            addQueryFuture(req.id(), fut);

            final String topic = orderedTopic(req.id(), cctx.nodeId());

            cctx.io().addOrderedHandler(topic, resHandler);

            fut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> fut) {
                    // Remove handler no matter if future has completed successfully.
                    cctx.io().removeOrderedHandler(topic);
                }
            });

            cctx.io().safeSend(nodes, req, new P1<GridNode>() {
                @Override public boolean apply(GridNode node) {
                    fut.onNodeLeft(node.id());

                    return !fut.isDone();
                }
            });
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param reqId Query request id.
     * @param nodeId Request sender node id.
     * @return Ordered topic.
     */
    private String orderedTopic(long reqId, UUID nodeId) {
        return TOPIC_CACHE.name(TOPIC_PREFIX, String.valueOf(reqId), nodeId.toString());
    }

    /**
     * Cancel message ID.
     */
    private class CancelMessageId implements Comparable<CancelMessageId> {
        /** Message ID. */
        private long reqId;

        /** Node ID. */
        private UUID nodeId;

        /**
         * @param reqId Message ID.
         * @param nodeId Node ID.
         */
        private CancelMessageId(long reqId, UUID nodeId) {
            this.reqId = reqId;
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(CancelMessageId m) {
            if (m.reqId == reqId)
                return m.nodeId.compareTo(nodeId);

            return reqId < m.reqId ? -1 : 1;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            CancelMessageId other = (CancelMessageId)obj;

            return reqId == other.reqId && nodeId.equals(other.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * ((int)(reqId ^ (reqId >>> 32))) + nodeId.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CancelMessageId.class, this);
        }
    }
}
