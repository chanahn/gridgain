// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Class that takes care about entries preloading in replicated cache.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.30092011
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
        demandPool = new GridReplicatedPreloadDemandPool<K, V>(cctx, syncPreloadFut, busyLock, lock);

        supplyPool = new GridReplicatedPreloadSupplyPool<K, V>(cctx,
            new PA() {
                @Override public boolean apply() {
                    return demandPool.finished();
                }
            }, busyLock);
    }

    /**
     * @throws GridException In case of error.
     */
    @Override public void onKernalStart() throws GridException {
        if (cctx.preloadEnabled()) {
            cctx.events().addPreloadEvent(-1, EVT_CACHE_PRELOAD_STARTED, cctx.discovery().shadow(cctx.localNode()),
                EVT_NODE_JOINED, cctx.localNode().metrics().getNodeStartTime());

            // Preloading stopped event notification.
            syncPreloadFut.listenAsync(
                new CIX1<GridFuture<?>>() {
                    @Override public void applyx(GridFuture<?> gridFuture) {
                        cctx.events().addPreloadEvent(-1, EVT_CACHE_PRELOAD_STOPPED,
                            cctx.discovery().shadow(cctx.localNode()),
                            EVT_NODE_JOINED, cctx.localNode().metrics().getNodeStartTime());
                    }
                }
            );
        }

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
     * Stops response thread.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public void stop() {
        if (log.isDebugEnabled())
            log.debug("Stopping replicated preloader...");

        // Acquire write lock so that any new thread could not be started.
        busyLock.writeLock().lock();

        supplyPool.stop();
        demandPool.stop();

        if (log.isDebugEnabled())
            log.debug("Replicated preloader has stopped.");
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
        if (demandPool.finished())
            return false;

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
    @Override public String toString() {
        return S.toString(GridReplicatedPreloader.class, this);
    }
}
