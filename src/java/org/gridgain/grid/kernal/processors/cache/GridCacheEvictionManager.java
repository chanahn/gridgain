// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.lang.utils.GridConcurrentLinkedDeque.*;

/**
 * Cache eviction manager.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.6.0c.30112011
 */
public class GridCacheEvictionManager<K, V> extends GridCacheManager<K, V> {
    /** Number of entries in the queue before unwinding happens. */
    private static final int ENTRY_UNWIND_THRESHOLD = Integer.getInteger(GG_EVICT_UNWIND_THRESHOLD, 100);

    /** How much are queues allowed to outgrow their maximums before they are forced to downsize. */
    private static final int QUEUE_OUTGROW_RATIO = 3;

    /** Number of entries to store in the eviction cache (for handling simultaneous preloading and evictions). */
    private static final int EVICT_HIST_SIZE = Integer.getInteger(GG_EVICTION_HISTORY_SIZE, 100000);

    /** Eviction policy. */
    private GridCacheEvictionPolicy<K, V> policy;

    /** Entries queue. */
    private final GridConcurrentLinkedDeque<GridCacheEntryEx<K, V>> entries =
            new GridConcurrentLinkedDeque<GridCacheEntryEx<K, V>>();

    /** Transactions queue. */
    private final GridConcurrentLinkedDeque<GridCacheTxEx<K, V>> txs =
            new GridConcurrentLinkedDeque<GridCacheTxEx<K, V>>();

    /** Entries queue size (including transaction entries). */
    private final AtomicInteger entryCnt = new AtomicInteger();

    /** Controlling lock for unwinding entries. */
    private final ReadWriteLock unwindLock = new ReentrantReadWriteLock();

    /** Unwinding flag. */
    private final AtomicBoolean unwinding = new AtomicBoolean(false);

    /** */
    private final GridConcurrentLinkedDeque<EvictionInfo> bufEvictQ = new GridConcurrentLinkedDeque<EvictionInfo>();

    /** Attribute name used to queue node in entry metadata. */
    private final String meta = UUID.randomUUID().toString();

    /** Evicting flag to make sure that only one thread processes eviction queue. */
    private final AtomicBoolean buffEvicting = new AtomicBoolean(false);

    /** Active eviction futures. */
    private final Map<Long, EvictionFuture> futs = new ConcurrentHashMap<Long, EvictionFuture>();

    /** Generator of future IDs. */
    private final AtomicLong idGen = new AtomicLong();

    /** Entry unwind threshold. */
    private int entryUnwindThreshold = ENTRY_UNWIND_THRESHOLD;

    /** Evict backup synchronized flag. */
    private boolean evictSync;

    /** Evict near synchronized flag. */
    private boolean nearSync;

    /** Evicted keys buffer (for handling simultaneous preloading and evictions). */
    private final GridConcurrentLinkedDeque<GridTuple2<K, GridCacheVersion>> evictHistBuff =
        new GridConcurrentLinkedDeque<GridTuple2<K, GridCacheVersion>>();

    /** Eviction history (for handling simultaneous preloading and evictions). */
    private final ConcurrentMap<K, Node<GridTuple2<K, GridCacheVersion>>> evictHist =
        new ConcurrentHashMap<K, Node<GridTuple2<K, GridCacheVersion>>>();

    /** Busy lock. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        GridCacheConfigurationAdapter cfg = cctx.config();

        policy = cctx.isNear() ? cfg.<K, V>getNearEvictionPolicy() : cfg.<K, V>getEvictionPolicy();

        assert policy != null;

        if (cfg.getMaxEvictionOverflowRatio() < 0)
            throw new GridException("Configuration parameter 'maxEvictionOverflowRatio' cannot be negative.");

        if (cfg.getEvictionKeyBufferSize() < 0)
            throw new GridException("Configuration parameter 'evictionKeyBufferSize' cannot be negative.");

        evictSync = cfg.isEvictSynchronized() && cfg.getCacheMode() != LOCAL &&
            !cctx.isNear() && !cctx.isSwapEnabled();

        nearSync = cfg.isEvictNearSynchronized() && cfg.getCacheMode() == PARTITIONED && !cctx.isNear();

        reportConfigurationProblems();

        if (evictSync || nearSync) {
            cctx.io().addHandler(GridCacheEvictionRequest.class, new CI2<UUID, GridCacheEvictionRequest<K, V>>() {
                @Override public void apply(UUID nodeId, GridCacheEvictionRequest<K, V> msg) {
                    processEvictionRequest(nodeId, msg);
                }
            });

            cctx.io().addHandler(GridCacheEvictionResponse.class, new CI2<UUID, GridCacheEvictionResponse<K, V>>() {
                @Override public void apply(UUID nodeId, GridCacheEvictionResponse<K, V> msg) {
                    processEvictionResponse(nodeId, msg);
                }
            });

            cctx.events().addListener(
                new GridLocalEventListener() {
                    @Override public void onEvent(GridEvent evt) {
                        assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                        GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

                        for (EvictionFuture fut : futs.values())
                            fut.onNodeLeft(discoEvt.eventNodeId());
                    }
                },
                EVT_NODE_FAILED, EVT_NODE_LEFT);
        }

        if (log.isDebugEnabled())
            log.debug("Eviction manager started on node: " + cctx.nodeId());
    }

    /**
     * @return {@code True} if indexing is enabled.
     */
    private boolean enabled() {
        return cctx.isNear() && cctx.config().isNearEvictionEnabled() ||
            !cctx.isNear() && cctx.config().isEvictionEnabled();
    }

    /**
     * Outputs warnings if potential configuration problems are detected.
     */
    private void reportConfigurationProblems() {
        GridCacheMode mode = cctx.config().getCacheMode();

        if (!cctx.isNear()) {
            if ((mode == REPLICATED || mode == PARTITIONED) && !evictSync) {
                U.warn(log, "Evictions are not synchronized with other nodes in topology " +
                    "which may cause data inconsistency (consider changing 'evictSynchronized' " +
                    "configuration property).",
                    "Evictions are not synchronized for cache: " + cctx.namexx());
            }

            if (mode == PARTITIONED && !nearSync) {
                U.warn(log, "Evictions on primary node are not synchronized with near nodes " +
                    "which which may cause some entries not to be evicted (consider changing " +
                    "'nearEvictSynchronized' configuration property).",
                    "Evictions on primary node are not synchronized with near nodes for cache: " + cctx.namexx());
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel, boolean wait) {
        super.stop0(cancel, wait);

        busyLock.block();

        if (log.isDebugEnabled())
            log.debug("Eviction manager stopped on node: " + cctx.nodeId());
    }

    /**
     * This method is meant to be used for testing and potentially for management as well.
     *
     * @param entryUnwindThreshold Entry unwind threshold.
     */
    public void setEntryUnwindThreshold(int entryUnwindThreshold) {
        assert entryUnwindThreshold > 0;

        this.entryUnwindThreshold = entryUnwindThreshold;
    }

    /**
     * Resets unwind thresholds back to default values.
     */
    public void resetEntryUnwindThreshold() {
        entryUnwindThreshold = ENTRY_UNWIND_THRESHOLD;
    }

    /**
     * @return Current size of evict queue.
     */
    public int evictQueueSize() {
        return bufEvictQ.sizex();
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Response.
     */
    private void processEvictionResponse(UUID nodeId, GridCacheEvictionResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;

        if (log.isDebugEnabled())
            log.debug("Processing eviction response [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                ", res=" + res + ']');

        EvictionFuture fut = futs.get(res.futureId());

        if (fut != null)
            fut.onResponse(nodeId, res);
        else {
            if (log.isDebugEnabled())
                log.debug("Eviction future for response is not found [res=" + res + ", node=" + nodeId +
                    ", localNode=" + cctx.nodeId() + ']');
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    private void processEvictionRequest(UUID nodeId, GridCacheEvictionRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        // Check to prevent runnable creation.
        if (!busyLock.enterBusy())
            return;

        try {
            if (req.classError() != null) {
                if (log.isDebugEnabled())
                    log.debug("Class got undeployed during eviction: " + req.classError());

                sendEvictionResponse(nodeId, new GridCacheEvictionResponse<K, V>(req.futureId(), true));

                return;
            }

            processEvictionRequest0(nodeId, req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    private void processEvictionRequest0(UUID nodeId, GridCacheEvictionRequest<K, V> req) {
        if (log.isDebugEnabled())
            log.debug("Processing eviction request [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                ", reqSize=" + req.entries().size() + ']');

        if (!busyLock.enterBusy())
            return;

        try {
            GridCacheEvictionResponse<K, V> res = new GridCacheEvictionResponse<K, V>(req.futureId());

            GridCacheVersion obsoleteVer = cctx.versions().next();

            // Partition -> {{Key, Version}, ...}.
            // Group DHT and replicated cache entries by their partitions.
            Map<Integer, Collection<GridTuple3<K, GridCacheVersion, Boolean>>> dhtEntries =
                new HashMap<Integer, Collection<GridTuple3<K, GridCacheVersion, Boolean>>>();

            Collection<GridTuple3<K, GridCacheVersion, Boolean>> nearEntries =
                new LinkedList<GridTuple3<K, GridCacheVersion, Boolean>>();

            for (GridTuple3<K, GridCacheVersion, Boolean> t : req.entries()) {
                Boolean near = t.get3();

                if (!near) {
                    // Lock is required.
                    Collection<GridTuple3<K, GridCacheVersion, Boolean>> col =
                        F.addIfAbsent(dhtEntries, cctx.partition(t.get1()),
                            new LinkedList<GridTuple3<K, GridCacheVersion, Boolean>>());

                    assert col != null;

                    col.add(t);
                }
                else
                    nearEntries.add(t);
            }

            // DHT and replicated cache entires.
            for (Map.Entry<Integer, Collection<GridTuple3<K, GridCacheVersion, Boolean>>> e :
                dhtEntries.entrySet()) {
                int part = e.getKey();

                boolean locked = lockPartition(part);

                try {
                    for (GridTuple3<K, GridCacheVersion, Boolean> t : e.getValue()) {
                        K key = t.get1();
                        GridCacheVersion ver = t.get2();
                        Boolean near = t.get3();

                        assert !near;

                        boolean evicted = evictLocally(key, ver, near, obsoleteVer);

                        if (log.isDebugEnabled())
                            log.debug("Evicted key [key=" + key + ", ver=" + ver + ", near=" + near +
                                ", evicted=" + evicted +']');

                        if (locked && evicted)
                            // Preloading is in progress, we need to save eviction info.
                            saveEvictionInfo(key, ver);

                        if (!evicted)
                            res.addRejected(key);
                    }
                }
                finally {
                    if (locked)
                        unlockPartition(part);
                }
            }

            // Near entries.
            for (GridTuple3<K, GridCacheVersion, Boolean> t : nearEntries) {
                K key = t.get1();
                GridCacheVersion ver = t.get2();
                Boolean near = t.get3();

                assert near;

                boolean evicted = evictLocally(key, ver, near, obsoleteVer);

                if (log.isDebugEnabled())
                    log.debug("Evicted key [key=" + key + ", ver=" + ver + ", near=" + near +
                        ", evicted=" + evicted +']');

                if (!evicted)
                    res.addRejected(key);
            }

            sendEvictionResponse(nodeId, res);

            unwind();
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void sendEvictionResponse(UUID nodeId, GridCacheEvictionResponse<K, V> res) {
        try {
            cctx.io().send(nodeId, res);

            if (log.isDebugEnabled())
                log.debug("Sent eviction response [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                    ", res" + res + ']');
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send eviction response since initiating node left grid " +
                    "[node=" + nodeId + ", localNode=" + cctx.nodeId() + ']');
        }
        catch (GridException e) {
            U.error(log, "Failed to send eviction response to node [node=" + nodeId +
                ", localNode=" + cctx.nodeId() + ", res" + res + ']', e);
        }
    }

    /**
     * Cache preloader should call this method from a synchronized context
     * (synchronized against partition being loaded or against preloader in
     * case of replicated cache).
     *
     * @param key Key.
     * @param ver Version.
     * @return {@code True} if preloading is permitted.
     */
    public boolean preloadingPermitted(K key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;

        if (enabled()) {
            Node<GridTuple2<K, GridCacheVersion>> node = evictHist.get(key);

            if (node != null) {
                GridTuple2<K, GridCacheVersion> t = node.item();

                if (t != null && ver.isLessEqual(t.get2())) {
                    if (log.isDebugEnabled())
                        log.debug("Preloading is not permitted for entry [key=" + key + ", ver=" + ver + ']');

                    return false;
                }
            }

            if (log.isDebugEnabled())
                log.debug("Preloading is permitted for entry [key=" + key + ", ver=" + ver + ']');
        }

        return true;
    }

    /**
     * Note: this method is synchronized on partition/preloader, so only one thread can
     * call this method with particular key value.
     *
     * @param key Key.
     * @param ver Version.
     */
    private void saveEvictionInfo(K key, GridCacheVersion ver) {
        Node<GridTuple2<K, GridCacheVersion>> node = new Node<GridTuple2<K, GridCacheVersion>>(F.t(key, ver));

        Node<GridTuple2<K, GridCacheVersion>> existingNode = evictHist.putIfAbsent(key, node);

        if (existingNode == null) {
            evictHistBuff.add(node);

            if (evictHistBuff.sizex() >= EVICT_HIST_SIZE) {
                // Remove the first element from queue and corresponding mapping from map.
                GridTuple2<GridTuple2<K, GridCacheVersion>, Node<GridTuple2<K, GridCacheVersion>>> t =
                    evictHistBuff.pollx();

                if (t != null)
                    evictHist.remove(t.get1().get1(), t.get2());
            }
        }
        else {
            GridTuple2<K, GridCacheVersion> t = existingNode.item();

            if (t == null || ver.isGreater(t.get2())) {
                // Node is unlinked or version to save is greater.
                if (!evictHist.replace(key, existingNode, node)) {
                    // This may happen only if node has been removed from map in the block above
                    Node<GridTuple2<K, GridCacheVersion>> n = evictHist.put(key, node);

                    assert n == null : "Node was concurrently added: " + n;
                }

                evictHistBuff.unlinkx(existingNode);

                evictHistBuff.add(node);
            }
        }
    }

    /**
     * @param p Partition ID.
     * @return {@code True} if partition has been actually locked,
     *      {@code false} if preloading is finished or disabled and no lock is needed.
     */
    private boolean lockPartition(int p) {
        if (!cctx.preloadEnabled())
            return false;

        if (cctx.isReplicated()) {
            GridReplicatedPreloader<K, V> preldr = (GridReplicatedPreloader<K, V>)cctx.cache().preloader();

            return preldr.lock();
        }
        else if (cctx.isDht()) {
            try {
                GridDhtLocalPartition<K, V> part = cctx.dht().topology().localPartition(p, -1, false);

                if (part != null && part.reserve()) {
                    part.lock();

                    return true;
                }
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition does not belong to local node [part=" + p +
                        ", nodeId" + cctx.localNode().id() + ']');
            }
        }

        // No lock is needed.
        return false;
    }

    /**
     * @param p Partition ID.
     */
    private void unlockPartition(int p) {
        if (!cctx.preloadEnabled())
            return;

        if (cctx.isReplicated()) {
            GridReplicatedPreloader<K, V> preldr = (GridReplicatedPreloader<K, V>)cctx.cache().preloader();

            preldr.unlock();
        }
        else if (cctx.isDht()) {
            try {
                GridDhtLocalPartition<K, V> part = cctx.dht().topology().localPartition(p, -1, false);

                if (part != null) {
                    part.unlock();

                    part.release();
                }
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition does not belong to local node [part=" + p +
                        ", nodeId" + cctx.localNode().id() + ']');
            }
        }
    }

    /**
     * @param key Key to evict.
     * @param ver Entry version on initial node.
     * @param near {@code true} if entry should be evicted from near cache.
     * @param obsoleteVer Obsolete version.
     * @return {@code true} if evicted successfully, {@code false} if could not be evicted.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    private boolean evictLocally(K key, final GridCacheVersion ver, boolean near, GridCacheVersion obsoleteVer) {
        assert key != null;
        assert ver != null;
        assert obsoleteVer != null;

        if (log.isDebugEnabled())
            log.debug("Evicting key locally [key=" + key + ", ver=" + ver + ", obsoleteVer=" + obsoleteVer +
                ", localNode=" + cctx.localNode() + ']');

        GridCacheAdapter<K, V> cache = near ? cctx.dht().near() : cctx.cache();

        GridCacheEntryEx<K, V> entry = cache.peekEx(key);

        if (entry == null)
            return true;

        try {
            // If entry should be evicted from near cache it can be done safely
            // without any consistency risks. We don't use filter in this case.
            if (near)
                return evict0(cache, entry, obsoleteVer, true, null);

            // Create filter that will not evict entry if its version changes after we get it.
            GridPredicate<? super GridCacheEntry<K, V>>[] filter =
                cctx.vararg(new P1<GridCacheEntry<K, V>>() {
                    @Override public boolean apply(GridCacheEntry<K, V> e) {
                        GridCacheVersion v = (GridCacheVersion)e.version();

                        return ver.equals(v);
                    }
                });

            GridCacheVersion v = entry.version();

            // If received version is less or greater than entry local version,
            // then don't evict.
            return ver.equals(v) && evict0(cache, entry, obsoleteVer, true, filter);
        }
        catch (GridCacheEntryRemovedException ignored) {
            // Entry was concurrently removed.
            return true;
        }
        catch (GridException e) {
            U.error(log, "Failed to evict entry on remote node [key=" + key + ", localNode=" + cctx.nodeId() + ']', e);

            return false;
        }
    }

    /**
     * @param cache Cache from which to evict entry.
     * @param entry Entry to evict.
     * @param obsoleteVer Obsolete version.
     * @param touch {@code true} to touch entry in case if it could not be evicted.
     * @param filter Filter.
     * @return {@code true} if entry has been evicted.
     * @throws GridException If failed to evict entry.
     */
    private boolean evict0(GridCacheAdapter<K, V> cache, GridCacheEntryEx<K, V> entry, GridCacheVersion obsoleteVer,
        boolean touch, @Nullable GridPredicate<? super GridCacheEntry<K, V>>[] filter) throws GridException {
        assert cache != null;
        assert entry != null;
        assert obsoleteVer != null;

        boolean evicted = entry.evictInternal(cctx.isSwapEnabled(), obsoleteVer, filter);

        if (evicted) {
            cache.removeEntry(entry);

            cctx.events().addEvent(entry.partition(), entry.key(), cctx.nodeId(), (GridUuid)null, null,
                EVT_CACHE_ENTRY_EVICTED, null, null);

            if (log.isDebugEnabled())
                log.debug("Entry was evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }
        else {
            if (touch)
                cache.context().evicts().touch(entry, true); // Make sure to go through cache context.

            if (log.isDebugEnabled())
                log.debug("Entry was not evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }

        return evicted;
    }

    /**
     * @param tx Transaction to register for eviction policy notifications.
     * @param explicit {@code True} if evict was called explicitly.
     */
    public void touch(GridCacheTxEx<K, V> tx, boolean explicit) {
        if (!enabled() && !explicit)
            return;

        if (log.isDebugEnabled())
            log.debug("Touching transaction [tx=" + CU.txString(tx) + ", localNode=" + cctx.nodeId() + ']');

        unwindLock.readLock().lock();

        try {
            txs.add(tx);

            entryCnt.addAndGet(tx.allEntries().size());
        }
        finally {
            unwindLock.readLock().unlock();
        }

        if (evictSync || nearSync) {
            for (GridCacheTxEntry<K, V> e : F.concat(false, tx.readEntries(), tx.writeEntries())) {
                Node<EvictionInfo> node = e.cached().removeMeta(meta);

                if (node != null)
                    bufEvictQ.unlinkx(node);

                for (EvictionFuture fut : futs.values())
                    fut.rejectEntry(e.cached());
            }
        }
    }

    /**
     * @param entry Entry for eviction policy notification.
     * @param explicit {@code True} if evict was called explicitly.
     */
    public void touch(GridCacheEntryEx<K, V> entry, boolean explicit) {
        if (!enabled() && !explicit)
            return;

        if (log.isDebugEnabled())
            log.debug("Touching entry [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');

        unwindLock.readLock().lock();

        try {
            entries.add(entry);

            entryCnt.incrementAndGet();
        }
        finally {
            unwindLock.readLock().unlock();
        }

        if (evictSync || nearSync) {
            Node<EvictionInfo> node = entry.removeMeta(meta);

            if (node != null)
                bufEvictQ.unlinkx(node);

            for (EvictionFuture fut : futs.values())
                fut.rejectEntry(entry);
        }
    }

    /**
     * @param entry Entry to attempt to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Optional entry filter.
     * @return {@code True} if entry was marked for eviction.
     * @throws GridException In case of error.
     */
    public boolean evict(@Nullable GridCacheEntryEx<K, V> entry, GridCacheVersion obsoleteVer,
        @Nullable GridPredicate<? super GridCacheEntry<K, V>>[] filter) throws GridException {
        if (entry == null)
            return true;

        // Do not evict internal entries.
        if (entry.key() instanceof GridCacheInternal)
            return false;

        if (evictSync || nearSync) {
            if (entry.wrap(false).backup())
                // Entry cannot be evicted on backup node.
                return false;

            try {
                if (!cctx.isAll(entry, filter))
                    return false;

                // Add entry to eviction queue.
                enqueue(entry, filter);
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Entry got removed while evicting [entry=" + entry +
                        ", localNode=" + cctx.nodeId() + ']');
            }
        }
        else
            return evict0(cctx.cache(), entry, obsoleteVer, false, filter);

        return true;
    }

    /**
     * @param entry Entry.
     * @param filter Filter.
     * @throws GridCacheEntryRemovedException If entry got removed.
     */
    private void enqueue(GridCacheEntryEx<K, V> entry, GridPredicate<? super GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException {
        Node<EvictionInfo> node = entry.meta(meta);

        if (node == null) {
            node = bufEvictQ.addLastx(new EvictionInfo(entry, entry.version(), filter));

            if (entry.putMetaIfAbsent(meta, node) != null)
                // Was concurrently added, need to clear it from queue.
                bufEvictQ.unlinkx(node);
            else if (log.isDebugEnabled())
                log.debug("Added entry to eviction queue: " + entry);
        }
    }

    /**
     * Checks eviction queue.
     */
    private void checkEvictionQueue() {
        if (bufEvictQ.sizex() >= maxQueueSize()) {
            if (buffEvicting.compareAndSet(false, true)) {
                Collection<EvictionInfo> evictionInfos;

                try {
                    int size = bufEvictQ.sizex();

                    if (size < maxQueueSize())
                        return;

                    if (log.isDebugEnabled())
                        log.debug("Processing eviction queue on node: " + cctx.nodeId());

                    evictionInfos = new ArrayList<EvictionInfo>(size);

                    for (int i = 0; i < size; i++) {
                        EvictionInfo info = bufEvictQ.poll();

                        if (info == null)
                            break;

                        evictionInfos.add(info);
                    }
                }
                finally {
                    buffEvicting.set(false);
                }

                if (!evictionInfos.isEmpty())
                    createEvictionFuture(evictionInfos);
            }
        }
    }

    /**
     * @return Max queue size.
     */
    private int maxQueueSize() {
        int size = Math.min((int)(cctx.cache().keySize() * cctx.config().getMaxEvictionOverflowRatio()) / 100,
            cctx.config().getEvictionKeyBufferSize());

        return size > 0 ? size : 500;
    }

    /**
     * Processes eviction queue (sends required requests, etc.).
     *
     * @param evictionInfos Eviction information to create future with.
     */
    private void createEvictionFuture(Collection<EvictionInfo> evictionInfos) {
        final EvictionFuture fut = new EvictionFuture(evictionInfos);

        // Listen to the future completion.
        fut.listenAsync(new CI1<GridFuture<?>>() {
            @Override public void apply(GridFuture<?> f) {
                // Prevent unwinding.
                unwindLock.readLock().lock();

                try {
                    GridTuple2<Collection<EvictionInfo>, Collection<EvictionInfo>> t;

                    try {
                        t = fut.get();
                    }
                    catch (GridException e) {
                        U.error(log, "Eviction future finished with error (all entries will be touched): " + fut, e);

                        for (EvictionInfo info : fut.entries())
                            touch(info.entry(), true);

                        return;
                    }

                    // Evict remotely evicted entries.
                    GridCacheVersion obsoleteVer = cctx.versions().next();

                    Collection<EvictionInfo> evictedEntries = t.get1();

                    for (EvictionInfo info : evictedEntries) {
                        GridCacheEntryEx<K, V> entry = info.entry();

                        try {
                            // Remove readers on which the entry was evicted.
                            for (GridTuple2<GridRichNode, Long> r : fut.evictedReaders(entry.key())) {
                                UUID readerId = r.get1().id();
                                Long msgId = r.get2();

                                ((GridDhtCacheEntry<K, V>)entry).removeReader(readerId, msgId);
                            }

                            evict0(cctx.cache(), entry, obsoleteVer, true, versionFilter(info));
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to evict entry [entry=" + entry +
                                ", localNode=" + cctx.nodeId() + ']', e);
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Entry was concurrently removed while evicting [entry=" + entry +
                                    ", localNode=" + cctx.nodeId() + ']');
                        }
                    }

                    // Touch rejected entries.
                    Collection<EvictionInfo> rejectedEntries = t.get2();

                    for (EvictionInfo info : rejectedEntries) {
                        if (log.isDebugEnabled())
                            log.debug("Touching rejected entry [entry=" + info.entry() + ", localNode="
                                + cctx.nodeId() + ']');

                        touch(info.entry(), true);
                    }
                }
                finally {
                    unwindLock.readLock().unlock();
                }
            }
        });
    }

    /**
     * @param info Eviction info.
     * @return Version aware filter.
     */
    private GridPredicate<? super GridCacheEntry<K, V>>[] versionFilter(final EvictionInfo info) {
        // If version has changed since we started the whole process
        // then we should not evict entry.
        return cctx.vararg(new P1<GridCacheEntry<K, V>>() {
            @Override public boolean apply(GridCacheEntry<K, V> e) {
                GridCacheVersion ver = (GridCacheVersion)e.version();

                return info.version().equals(ver) && F.isAll(info.filter());
            }
        });
    }

    /**
     * Gets a collection of nodes to send eviction requests to.
     *
     * @param entry Entry.
     * @return Tuple of two collections: dht (in case of partitioned cache) nodes
     *      and readers (empty for replicated cache).
     * @throws GridCacheEntryRemovedException If entry got removed during method
     *      execution.
     */
    @SuppressWarnings( {"IfMayBeConditional"})
    private GridTuple2<Collection<GridRichNode>, Collection<GridRichNode>> remoteNodes(GridCacheEntryEx<K, V> entry)
        throws GridCacheEntryRemovedException {
        assert entry != null;

        Collection<GridRichNode> backups;

        Collection<GridRichNode> readers;

        GridCacheAffinity<Object> aff = cctx.config().getAffinity();

        if (cctx.config().getCacheMode() == REPLICATED) {
            if (evictSync) {
                backups = new HashSet<GridRichNode>(
                    F.view(aff.nodes(entry.partition(), CU.allNodes(cctx)), F.notEqualTo(cctx.localNode())));
            }
            else
                backups = Collections.emptySet();

            readers = Collections.emptySet();
        }
        else {
            assert cctx.config().getCacheMode() == PARTITIONED;

            if (evictSync) {
                // TODO: What topology version to pass?
                backups = F.transform(cctx.dht().topology().nodes(entry.partition(), -1), cctx.rich().richNode(),
                    F.<GridNode>notEqualTo(cctx.localNode()));
            }
            else
                backups = Collections.emptySet();

            if (nearSync) {
                readers = F.transform(((GridDhtCacheEntry<K, V>)entry).readers(), new C1<UUID, GridRichNode>() {
                    @Override @Nullable public GridRichNode apply(UUID nodeId) {
                        return cctx.node(nodeId);
                    }
                });
            }
            else
                readers = Collections.emptySet();
        }

        return new GridPair<Collection<GridRichNode>>(backups, readers);
    }

    /**
     * Notifications.
     */
    public void unwind() {
        if (entryCnt.get() >= entryUnwindThreshold) {
            // Only one thread should unwind for efficiency.
            if (unwinding.compareAndSet(false, true)) {
                GridCacheFlag[] old = cctx.forceLocal();

                try {
                    if (!unwindSafe(true)) {
                        unwindLock.writeLock().lock();

                        try {
                            boolean ret = unwindSafe(false); // Unwind within lock.

                            assert ret;
                        }
                        finally {
                            unwindLock.writeLock().unlock();
                        }
                    }
                }
                finally {
                    cctx.forceFlags(old);

                    unwinding.set(false);
                }
            }
        }

        checkEvictionQueue();
    }

    /**
     * @param cap {@code True} if unwind process should cap after queue grows to certain limit.
     * @return {@code True} if unwind thread was able to succeed, {@code false} if queue
     *      kept growing during unwind.
     */
    private boolean unwindSafe(boolean cap) {
        int cnt = 0;

        int startCnt = entryCnt.get();

        // Touch first.
        for (GridCacheEntryEx<K, V> e = entries.poll(); e != null; e = entries.poll()) {
            cnt = entryCnt.decrementAndGet();

            // Internal entry can't be checked in policy.
            if (!(e.key() instanceof GridCacheInternal))
                policy.onEntryAccessed(e.obsolete(), e.evictWrap());

            if (cap && cnt >= startCnt * QUEUE_OUTGROW_RATIO)
                return false;
        }

        for (Iterator<GridCacheTxEx<K, V>> it = txs.iterator(); it.hasNext(); ) {
            GridCacheTxEx<K, V> tx = it.next();

            if (!tx.done())
                return true;

            it.remove();

            Collection<GridCacheTxEntry<K, V>> txEntries = tx.allEntries();

            cnt = entryCnt.addAndGet(-txEntries.size());

            if (!tx.internal())
                notify(txEntries);

            if (cap && cnt >= startCnt * QUEUE_OUTGROW_RATIO)
                return false;
        }

        assert cap || cnt == 0 : "Invalid entry count [cnt=" + cnt + ", size=" + txs.size() + ", txs=" + txs +
            ", entries=" + entries + ']';

        return true;
    }

    /**
     * Prints out eviction stats.
     */
    public void printStats() {
        X.println("Eviction stats [grid=" + cctx.gridName() + ", cache=" + cctx.cache().name() +
            ", txs=" + txs.size() + ", entries=" + entries.size() + ", buffEvictQ=" + bufEvictQ.sizex() + ']');
    }

    /**
     * @param entries Transaction entries for eviction notifications.
     */
    private void notify(Iterable<GridCacheTxEntry<K, V>> entries) {
        for (GridCacheTxEntry<K, V> txe : entries) {
            GridCacheEntryEx<K, V> e = txe.cached();

            // Internal entry can't be checked in policy.
            if (!(e.key() instanceof GridCacheInternal))
                policy.onEntryAccessed(e.obsolete(), e.evictWrap());
        }
    }

    /** {@inheritDoc} */
    @Override protected void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Eviction manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   buffEvictQ size: " + bufEvictQ.sizex());
        X.println(">>>   txsSize: " + txs.size());
        X.println(">>>   entriesSize: " + entries.size());
        X.println(">>>   futsSize: " + futs.size());
        X.println(">>>   futsCreated: " + idGen.get());
        X.println(">>>   evictionHistoryMapSize: " + evictHist.size());
        X.println(">>>   evictionHistoryBufferSize: " + evictHistBuff.sizex());
    }

    /**
     * Wrapper around an entry to be put into queue.
     */
    private class EvictionInfo {
        /** Cache entry. */
        private GridCacheEntryEx<K, V> entry;

        /** Start version. */
        private GridCacheVersion ver;

        /** Filter to pass before entry will be evicted. */
        private GridPredicate<? super GridCacheEntry<K, V>>[] filter;

        /**
         * @param entry Entry.
         * @param ver Version.
         * @param filter Filter.
         */
        EvictionInfo(GridCacheEntryEx<K, V> entry, GridCacheVersion ver,
            GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            assert entry != null;
            assert ver != null;

            this.entry = entry;
            this.ver = ver;
            this.filter = filter;
        }

        /**
         * @return Entry.
         */
        GridCacheEntryEx<K, V> entry() {
            return entry;
        }

        /**
         * @return Version.
         */
        GridCacheVersion version() {
            return ver;
        }

        /**
         * @return Filter.
         */
        GridPredicate<? super GridCacheEntry<K, V>>[] filter() {
            return filter;
        }
    }

    /**
     * Future for synchronized eviction. Result is a tuple: {evicted entries, rejected entries}.
     */
    private class EvictionFuture extends GridFutureAdapter<GridTuple2<Collection<EvictionInfo>,
        Collection<EvictionInfo>>> {
        /** */
        private final long id = idGen.incrementAndGet();

        /** */
        private final ConcurrentMap<K, EvictionInfo> entries = new ConcurrentHashMap<K, EvictionInfo>();

        /** */
        private final ConcurrentMap<K, Collection<GridRichNode>> readers =
            new ConcurrentHashMap<K, Collection<GridRichNode>>();

        /** */
        private final Collection<EvictionInfo> evictedEntries = new GridConcurrentHashSet<EvictionInfo>();

        /** */
        private final ConcurrentMap<K, EvictionInfo> rejectedEntries = new ConcurrentHashMap<K, EvictionInfo>();

        /** Request map. */
        private final ConcurrentMap<UUID, GridCacheEvictionRequest<K, V>> reqMap =
            new ConcurrentHashMap<UUID, GridCacheEvictionRequest<K, V>>();

        /** Response map. */
        private final ConcurrentMap<UUID, GridCacheEvictionResponse<K, V>> resMap =
            new ConcurrentHashMap<UUID, GridCacheEvictionResponse<K, V>>();

        /** To make sure that future is completing within a single thread. */
        private final AtomicBoolean completing = new AtomicBoolean(false);

        /** Lock. */
        @GridToStringExclude
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Object to force future completion on elapsing network timeout. */
        @GridToStringExclude
        private GridTimeoutObject timeoutObject;

        /**
         * @param evictionInfos Eviction information to create future with.
         */
        EvictionFuture(Collection<EvictionInfo> evictionInfos) {
            super(cctx.kernalContext());

            futs.put(id, this);

            prepare(evictionInfos);
        }

        /**
         * Required by {@code Externalizable}.
         */
        public EvictionFuture() {
            // No-op.
        }

        /**
         * Prepares future (sends all required requests).
         *
         * @param evictionInfos Eviction information to prepare with.
         */
        private void prepare(Collection<EvictionInfo> evictionInfos) {
            if (log.isDebugEnabled())
                log.debug("Preparing eviction future [futId=" + id + ", localNode=" + cctx.nodeId() + ']');

            Collection<EvictionInfo> locals = null;

            for (EvictionInfo info : evictionInfos) {
                // Queue node may have been stored in entry metadata concurrently, but we don't care
                // about it since we are currently processing this entry.
                Node<EvictionInfo> queueNode = info.entry().removeMeta(meta);

                if (queueNode != null)
                    bufEvictQ.unlinkx(queueNode);

                GridTuple2<Collection<GridRichNode>, Collection<GridRichNode>> tup;

                try {
                    tup = remoteNodes(info.entry());
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Entry got removed while preparing eviction future (will be ignored) [entry=" +
                            info.entry() + ", nodeId=" + cctx.nodeId() + ']');

                    continue;
                }

                Collection<GridRichNode> entryReaders =
                    F.addIfAbsent(readers, info.entry().key(), new GridConcurrentHashSet<GridRichNode>());

                assert entryReaders != null;

                // Add entry readers so that we could remove them right before local eviction.
                entryReaders.addAll(tup.get2());

                Collection<GridRichNode> nodes = F.concat(true, tup.get1(), tup.get2());

                if (!nodes.isEmpty()) {
                    entries.put(info.entry().key(), info);

                    // There are remote participants.
                    for (GridRichNode node : nodes) {
                        GridCacheEvictionRequest<K, V> req = F.addIfAbsent(reqMap, node.id(),
                            new GridCacheEvictionRequest<K, V>(id, evictionInfos.size()));

                        assert req != null;

                        req.addKey(info.entry().key(), info.version(), entryReaders.contains(node));
                    }
                }
                else {
                    if (locals == null)
                        locals = new HashSet<EvictionInfo>(evictionInfos.size(), 1.0f);

                    // There are no remote participants, need to keep the entry as local.
                    locals.add(info);
                }
            }

            if (locals != null) {
                // Evict entries without remote participant nodes immediately.
                GridCacheVersion obsoleteVer = cctx.versions().next();

                for (EvictionInfo info : locals) {
                    if (log.isDebugEnabled())
                        log.debug("Evicting key without remote participant nodes: " + info);

                    try {
                        evict0(cctx.cache(), info.entry(), obsoleteVer, true, versionFilter(info));
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to evict entry: " + info.entry(), e);
                    }
                }
            }

            // If there were only local entries.
            if (entries.isEmpty()) {
                complete(false);

                return;
            }

            // Send eviction requests.
            for (Map.Entry<UUID, GridCacheEvictionRequest<K, V>> e : reqMap.entrySet()) {
                UUID nodeId = e.getKey();

                GridCacheEvictionRequest<K, V> req = e.getValue();

                if (log.isDebugEnabled())
                    log.debug("Sending eviction request [node=" + nodeId + ", req=" + req + ']');

                try {
                    cctx.io().send(nodeId, req);
                }
                catch (GridTopologyException ignored) {
                    // Node left the topology.
                    onNodeLeft(nodeId);
                }
                catch (GridException ex) {
                    U.error(log, "Failed to send eviction request to node [node=" + nodeId + ", req=" + req + ']', ex);

                    rejectEntries(nodeId);
                }
            }

            registerTimeoutObject();
        }

        /**
         *
         */
        private void registerTimeoutObject() {
            // Check whether future has not been completed yet.
            if (lock.readLock().tryLock()) {
                try {
                    timeoutObject = new GridTimeoutObject() {
                        private final GridUuid id = GridUuid.randomUuid();
                        private final long endTime =
                            System.currentTimeMillis() + cctx.gridConfig().getNetworkTimeout();

                        @Override public GridUuid timeoutId() {
                            return id;
                        }

                        @Override public long endTime() {
                            return endTime;
                        }

                        @Override public void onTimeout() {
                            complete(true);
                        }
                    };

                    cctx.time().addTimeoutObject(timeoutObject);
                }
                finally {
                    lock.readLock().unlock();
                }
            }
        }

        /**
         * @return Keys to readers mapping.
         */
        Map<K, Collection<GridRichNode>> readers() {
            return readers;
        }

        /**
         * @return All entries associated with future that should be evicted (or rejected).
         */
        Collection<EvictionInfo> entries() {
            return entries.values();
        }

        /**
         * Reject all entries on behalf of specified node.
         *
         * @param nodeId Node ID.
         */
        private void rejectEntries(UUID nodeId) {
            assert nodeId != null;

            if (lock.readLock().tryLock()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Rejecting entries for node: " + nodeId);

                    GridCacheEvictionRequest<K, V> req = reqMap.remove(nodeId);

                    for (GridTuple3<K, GridCacheVersion, Boolean> t : req.entries()) {
                        EvictionInfo info = entries.get(t.get1());

                        assert info != null;

                        rejectedEntries.put(t.get1(), info);
                    }
                }
                finally {
                    lock.readLock().unlock();
                }
            }
            else
                assert false : "Failed to obtain read lock for future: " + this;

            checkDone();
        }

        /**
         * @param entry Entry to reject from being evicted.
         */
        public void rejectEntry(GridCacheEntryEx<K, V> entry) {
            assert entry != null;

            if (lock.readLock().tryLock()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Rejecting entry: " + entry);

                    EvictionInfo info = entries.get(entry.key());

                    if (info != null)
                        rejectedEntries.put(entry.key(), info);
                }
                finally {
                    lock.readLock().unlock();
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Ignored rejected entry: " + entry);
            }
        }

        /**
         * @param nodeId Node id that left the topology.
         */
        void onNodeLeft(UUID nodeId) {
            assert nodeId != null;

            if (lock.readLock().tryLock()) {
                try {
                    // Stop waiting response from this node.
                    reqMap.remove(nodeId);

                    resMap.remove(nodeId);
                }
                finally {
                    lock.readLock().unlock();
                }

                checkDone();
            }
        }

        /**
         * @param nodeId Sender node ID.
         * @param res Response.
         */
        void onResponse(UUID nodeId, GridCacheEvictionResponse<K, V> res) {
            assert nodeId != null;
            assert res != null;

            if (lock.readLock().tryLock()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Entered to eviction future onResponse() [fut=" + this + ", node=" + nodeId +
                            ", res=" + res + ']');

                    GridRichNode node = cctx.node(nodeId);

                    if (node != null)
                        resMap.put(nodeId, res);
                    else
                        // Sender node left grid.
                        reqMap.remove(nodeId);
                }
                finally {
                    lock.readLock().unlock();
                }

                if (res.error())
                    // Complete future, since there was a class loading error on at least one node.
                    complete(false);
                else
                    checkDone();
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Ignored eviction response [fut=" + this + ", node=" + nodeId + ", res=" + res + ']');
            }
        }

        /**
         *
         */
        private void checkDone() {
            if (reqMap.isEmpty() || resMap.keySet().containsAll(reqMap.keySet()))
                complete(false);
        }

        /**
         * Completes future.
         *
         * @param timedOut {@code True} if future is being forcibly completed on timeout.
         * @return {@code True} if this call has completed future.
         */
        @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
        boolean complete(boolean timedOut) {
            if (completing.compareAndSet(false, true)) {
                // Lock will never be released intentionally.
                lock.writeLock().lock();

                if (timeoutObject != null)
                    cctx.time().removeTimeoutObject(timeoutObject);

                futs.remove(id);

                if (log.isDebugEnabled())
                    log.debug("Building eviction future result [fut=" + this + ", timedOut=" + timedOut + ']');

                boolean err = F.forAny(resMap.values(), new P1<GridCacheEvictionResponse<K, V>>() {
                    @Override public boolean apply(GridCacheEvictionResponse<K, V> res) {
                        return res.error();
                    }
                });

                if (err) {
                    Collection<UUID> ids = F.view(resMap.keySet(), new P1<UUID>() {
                        @Override public boolean apply(UUID e) {
                            return resMap.get(e).error();
                        }
                    });

                    assert !ids.isEmpty();

                    U.warn(log, "Remote node(s) failed to process eviction request " +
                        "(some backup or remote values maybe lost): " + ids);
                }

                if (timedOut)
                    U.warn(log, "Timed out waiting for eviction future " +
                        "(consider increasing 'networkTimeout' configuration property).");

                if (err || timedOut) {
                    // Future has not been completed successfully, all entries should be rejected.
                    assert evictedEntries.isEmpty();

                    rejectedEntries.putAll(entries);
                }
                else {
                    // Future has been completed successfully - build result.
                    for (EvictionInfo info : entries.values()) {
                        K key = info.entry().key();

                        if (rejectedEntries.containsKey(key))
                            // Was already rejected.
                            continue;

                        boolean rejected = false;

                        for (GridCacheEvictionResponse<K, V> res : resMap.values())
                            if (res.rejectedKeys().contains(key)) {
                                rejectedEntries.put(key, info);

                                rejected = true;

                                break;
                            }

                        if (!rejected)
                            evictedEntries.add(info);
                    }
                }

                onDone(F.t(evictedEntries, rejectedEntries.values()));

                return true;
            }

            return false;
        }

        /**
         * @param key Key.
         * @return Reader nodes on which given key was evicted.
         */
        Collection<GridTuple2<GridRichNode, Long>> evictedReaders(K key) {
            Collection<GridRichNode> mappedReaders = readers.get(key);

            if (mappedReaders == null)
                return Collections.emptyList();

            Collection<GridTuple2<GridRichNode, Long>> col = new LinkedList<GridTuple2<GridRichNode, Long>>();

            for (Map.Entry<UUID, GridCacheEvictionResponse<K, V>> e : resMap.entrySet()) {
                GridRichNode node = cctx.node(e.getKey());

                // If node has left or response did not arrive from near node
                // then just skip it.
                if (node == null || !mappedReaders.contains(node))
                    continue;

                GridCacheEvictionResponse<K, V> res = e.getValue();

                if (!res.rejectedKeys().contains(key))
                    col.add(F.t(node, res.messageId()));
            }

            return col;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EvictionFuture.class, this);
        }
    }
}
