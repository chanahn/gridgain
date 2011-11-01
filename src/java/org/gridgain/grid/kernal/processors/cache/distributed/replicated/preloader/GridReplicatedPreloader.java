// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.worker.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Class that takes care about entries preloading in replicated cache.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.01112011
 */
public class GridReplicatedPreloader<K, V> extends GridCachePreloaderAdapter<K, V> {
    /** Busy lock to control activeness of threads (loader, sender). */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Future to wait for the end of preloading on. */
    private final GridFutureAdapter<?> syncPreloadFut = new GridFutureAdapter(cctx.kernalContext());

    /** Lock to prevent preloading for the time of eviction. */
    private final Lock lock = new ReentrantLock();

    /** Supply pool. */
    private GridReplicatedPreloadSupplyPool<K, V> supplyPool;

    /** Demand pool. */
    private GridReplicatedPreloadDemandPool<K, V> demandPool;

    /** */
    private final PreloadSegmentationWorker preloadWrk = new PreloadSegmentationWorker();

    /** */
    private final CountDownLatch initSesCreated = new CountDownLatch(1);

    /**
     * @param cctx Cache context.
     */
    public GridReplicatedPreloader(GridCacheContext<K, V> cctx) {
        super(cctx);
    }

    /**
     * @throws GridException In case of error.
     */
    @Override public void start() throws GridException {
        demandPool = new GridReplicatedPreloadDemandPool<K, V>(cctx, busyLock, lock);

        supplyPool = new GridReplicatedPreloadSupplyPool<K, V>(cctx,
            new PA() {
                @Override public boolean apply() {
                    return demandPool.currentSession() == null;
                }
            }, busyLock);
    }

    /**
     * @throws GridException In case of error.
     */
    @Override public void onKernalStart() throws GridException {
        if (cctx.accountForReconnect())
            new GridThread(preloadWrk).start();

        if (cctx.preloadEnabled()) {
            if (log.isDebugEnabled())
                log.debug("Creating initial session.");

            createSession(EVT_NODE_JOINED, syncPreloadFut);
        }

        initSesCreated.countDown();

        supplyPool.start();
        demandPool.start();

        if (cctx.config().getPreloadMode() == SYNC)
            U.log(log, "Starting preloading in SYNC mode...");

        long start = System.currentTimeMillis();

        if (cctx.config().getPreloadMode() == SYNC) {
            syncPreloadFut.get();

            U.log(log, "Completed preloading in SYNC mode in " + (System.currentTimeMillis() - start) + " ms.");
        }
    }

    /**
     * @param discoEvtType Corresponding discovery event.
     * @param finishFut Finish future for session.
     */
    void createSession(final int discoEvtType, GridFutureAdapter<?> finishFut) {
        assert cctx.preloadEnabled();
        assert finishFut != null;

        long maxOrder0 = cctx.localNode().order() - 1; // Preload only from elder nodes.

        GridReplicatedPreloadSession ses = new GridReplicatedPreloadSession(maxOrder0, finishFut);

        Collection<GridRichNode> rmts = CU.allNodes(cctx, maxOrder0);

        if (!rmts.isEmpty()) {
            for (int part : partitions(cctx.localNode())) {
                Collection<GridRichNode> partNodes = cctx.config().getAffinity().nodes(part, rmts);

                int cnt = partNodes.size();

                if (cnt == 0)
                    continue;

                for (int mod = 0; mod < cnt; mod++) {
                    GridReplicatedPreloadAssignment assign =
                        new GridReplicatedPreloadAssignment(ses, part, mod, cnt);

                    ses.addAssignment(assign);

                    if (log.isDebugEnabled())
                        log.debug("Created assignment: " + assign);
                }
            }
        }

        cctx.events().addPreloadEvent(-1, EVT_CACHE_PRELOAD_STARTED, cctx.discovery().shadow(cctx.localNode()),
            discoEvtType, cctx.localNode().metrics().getNodeStartTime());

        if (!ses.assigns().isEmpty())
            demandPool.currentSession(ses);
        else
            finishFut.onDone();

        // Preloading stopped event notification.
        finishFut.listenAsync(
            new CIX1<GridFuture<?>>() {
                @Override public void applyx(GridFuture<?> gridFuture) {
                    cctx.events().addPreloadEvent(-1, EVT_CACHE_PRELOAD_STOPPED,
                        cctx.discovery().shadow(cctx.localNode()),
                        discoEvtType, cctx.localNode().metrics().getNodeStartTime());
                }
            }
        );
    }

    /**
     * @param node Node.
     * @return Collection of partition numbers for the node.
     */
    Set<Integer> partitions(GridRichNode node) {
        assert node != null;

        GridCacheAffinity<Object> aff = cctx.config().getAffinity();

        Collection<GridRichNode> nodes = CU.allNodes(cctx);

        Set<Integer> parts = new HashSet<Integer>();

        int partCnt = aff.partitions();

        for (int i = 0; i < partCnt; i++) {
            Collection<GridRichNode> affNodes = aff.nodes(i, nodes);

            if (affNodes.contains(node))
                parts.add(i);
        }

        return parts;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public void onKernalStop() {
        if (log.isDebugEnabled())
            log.debug("Replicated preloader onKernalStop callback.");

        // Acquire write lock.
        busyLock.writeLock().lock();

        U.cancel(preloadWrk);
        U.join(preloadWrk, log);

        supplyPool.stop();
        demandPool.stop();

        if (log.isDebugEnabled())
            log.debug("Replicated preloader has been stopped.");
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> startFuture() {
        return cctx.config().getPreloadMode() != SYNC ? new GridFinishedFuture() : syncPreloadFut;
    }

    /**
     * @return {@code True} if lock was acquired (this makes preloading impossible).
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public boolean lock() {
        lock.lock();

        return true;
    }

    /**
     * Makes preloading possible.
     */
    public void unlock() {
        lock.unlock();
    }

    /** {@inheritDoc} */
    @Override protected void onSegmentationEvent(GridDiscoveryEvent evt) {
        assert evt != null;

        if (busyLock.readLock().tryLock()) {
            try {
                preloadWrk.addEvent(evt);
            }
            finally {
                busyLock.readLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedPreloader.class, this);
    }

    /**
     *
     */
    private class PreloadSegmentationWorker extends GridWorker {
        /** */
        private final BlockingQueue<GridDiscoveryEvent> evts = new LinkedBlockingQueue<GridDiscoveryEvent>();

        /**
         *
         */
        private PreloadSegmentationWorker() {
            super(cctx.gridName(), "replicated-preldr-seg-wrk", log);
        }

        /**
         * @param evt Event.
         */
        void addEvent(GridDiscoveryEvent evt) {
            assert evt != null;

            evts.add(evt);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            // First wait for initial preloading session to create.
            initSesCreated.await();

            int lastEvt = -1;

            while (!isCancelled()) {
                GridDiscoveryEvent evt = evts.take();

                int evtType = evt.type();

                assert evtType != lastEvt : "Unexpected event type [last=" + lastEvt + ", new=" + evtType + ']';

                if (log.isDebugEnabled())
                    log.debug("Processing event: " + evt);

                if (evtType == EVT_NODE_SEGMENTED) {
                    try {
                        demandPool.cancelCurrentSession();

                        for (GridCacheTxEx<K, V> tx : cctx.tm().txs()) {
                            cctx.tm().salvageTx(tx);

                            tx.finishFuture().get();
                        }
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to cancel current preload session.", e);
                    }
                }
                else {
                    assert evtType == EVT_NODE_RECONNECTED :
                        "Unexpected event type [last=" + lastEvt + ", new=" + evtType + ']';

                    createSession(EVT_NODE_RECONNECTED, new GridFutureAdapter<Object>());
                }

                lastEvt = evtType;
            }
        }
    }
}
