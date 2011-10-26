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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Thread pool for demanding entries.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.26102011
 */
class GridReplicatedPreloadDemandPool<K, V> {
    /** Dummy message to wake up demand worker. */
    private static final SupplyMessage DUMMY_MSG = new SupplyMessage();

    /** Cache context. */
    private final GridCacheContext<K, V> cctx;

    /** Logger. */
    private final GridLogger log;

    /** Future for preload finish. */
    private final GridFutureAdapter<?> syncPreloadFut;

    /** Busy lock. */
    private final ReadWriteLock busyLock;

    /** Demand workers. */
    private final Collection<DemandWorker> workers = new LinkedList<DemandWorker>();

    /** Assignments. */
    private BlockingQueue<Assignment> assigns = new LinkedBlockingQueue<Assignment>();

    /** Left assignments count. */
    private final AtomicInteger leftAssigns = new AtomicInteger();

    /** Timeout. */
    private final AtomicLong timeout = new AtomicLong();

    /** Max order of the elder nodes that completed preloading. */
    private final GridAtomicLong maxOrder = new GridAtomicLong();

    /** Lock to prevent preloading for the time of eviction. */
    private final Lock lock;

    /** Barrier for undeploys. */
    private final CyclicBarrier barrier;

    /**
     * @param cctx Cache context.
     * @param syncPreloadFut Preload future.
     * @param busyLock Shutdown lock.
     * @param lock Preloading lock.
     */
    GridReplicatedPreloadDemandPool(GridCacheContext<K, V> cctx, GridFutureAdapter<?> syncPreloadFut,
        ReadWriteLock busyLock, Lock lock) {
        assert cctx != null;
        assert syncPreloadFut != null;
        assert busyLock != null;

        this.cctx = cctx;
        this.syncPreloadFut = syncPreloadFut;
        this.busyLock = busyLock;
        this.lock = lock;

        log = cctx.logger(getClass());

        int poolSize = cctx.preloadEnabled() ? cctx.config().getPreloadThreadPoolSize() : 1;

        timeout.set(cctx.gridConfig().getNetworkTimeout());

        barrier = new CyclicBarrier(poolSize, new Runnable() {
            @Override public void run() {
                GridReplicatedPreloadDemandPool.this.cctx.deploy().unwind();
            }
        });

        for (int i = 0; i < poolSize; i++)
            workers.add(new DemandWorker(i));
    }

    /**
     *
     */
    void start() {
        if (cctx.preloadEnabled()) {
            long maxOrder0 = cctx.localNode().order() - 1; // Preload only from elder nodes.

            maxOrder.set(maxOrder0);

            Collection<GridRichNode> rmts = CU.allNodes(cctx, maxOrder0);

            if (!rmts.isEmpty()) {
                for (int part : partitions(cctx.localNode())) {
                    Collection<GridRichNode> partNodes = cctx.config().getAffinity().nodes(part, rmts);

                    int cnt = partNodes.size();

                    if (cnt == 0)
                        continue;

                    for (int mod = 0; mod < cnt; mod++) {
                        Assignment assignment = new Assignment(part, mod, cnt);

                        assigns.add(assignment);

                        if (log.isDebugEnabled())
                            log.debug("Created assignment: " + workers.size());

                        leftAssigns.incrementAndGet();
                    }
                }
            }
        }

        finish(); // Finish if preload is disabled or no assigns has been created.

        for (DemandWorker w : workers)
            new GridThread(cctx.gridName(), "preldr-demand-wrk", w).start();

        if (log.isDebugEnabled())
            log.debug("Started demand pool: " + workers.size());
    }

    /**
     *
     */
    private void finish() {
        // Act as a guard here.
        if (leftAssigns.compareAndSet(0, -1)) {
            boolean res = syncPreloadFut.onDone();

            assert res;
        }
    }

    /**
     * @return {@code True} if preload finished.
     */
    boolean finished() {
        return leftAssigns.get() == -1;
    }

    /**
     *
     */
    void stop() {
        U.cancel(workers);
        U.join(workers, log);
    }

    /**
     * @return {@code true} if entered busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (supplier is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * There is currently a case where {@code interrupted}
     * flag on a thread gets flipped during stop which causes the pool to hang.  This check
     * will always make sure that interrupted flag gets reset before going into wait conditions.
     * <p>
     * The true fix should actually make sure that interrupted flag does not get reset or that
     * interrupted exception gets propagated. Until we find a real fix, this method should
     * always work to make sure that there is no hanging during stop.
     */
    private void beforeWait() {
        GridWorker w = worker();

        if (w != null && w.isCancelled())
            Thread.currentThread().interrupt();
    }

    /**
     * @return Current worker.
     */
    private GridWorker worker() {
        GridWorker w = GridWorkerGroup.instance(cctx.gridName()).currentWorker();

        assert w != null;

        return w;
    }

    /**
     * @param queue Queue to poll from.
     * @param time Time to wait.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(BlockingQueue<T> queue, long time) throws InterruptedException {
        beforeWait();

        return queue.poll(time, MILLISECONDS);
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

    /**
     * Demand worker.
     */
    private class DemandWorker extends GridWorker {
        /** Worker ID. */
        private int id = -1;

        /** Message queue. */
        private final LinkedBlockingDeque<SupplyMessage<K, V>> msgQ =
            new LinkedBlockingDeque<SupplyMessage<K, V>>();

        /** Counter. */
        private long cntr;

        /**
         * @param id Worker ID.
         */
        private DemandWorker(int id) {
            super(cctx.gridName(), "preloader-demand-wrk", log);

            assert id >= 0;

            this.id = id;
        }

        /**
         * @param msg Message.
         */
        private void addMessage(SupplyMessage<K, V> msg) {
            if (!enterBusy())
                return;

            try {
                assert msg == DUMMY_MSG || msg.message().workerId() == id : "Invalid message: " + msg;

                msgQ.offer(msg);
            }
            finally {
                leaveBusy();
            }
        }

        /**
         * @param timeout Timed out value.
         */
        private void growTimeout(long timeout) {
            long newTimeout = (long)(timeout * 1.5D);

            // Account for overflow.
            if (newTimeout < 0)
                newTimeout = Long.MAX_VALUE;

            // Grow by 50% only if another thread didn't do it already.
            if (GridReplicatedPreloadDemandPool.this.timeout.compareAndSet(timeout, newTimeout))
                U.warn(log, "Increased preloading message timeout from " + timeout + "ms to " +
                    newTimeout + "ms.");
        }

        /**
         * @param idx Unique index for this topic.
         * @return Topic name for partition.
         */
        private String topic(long idx) {
            return TOPIC_CACHE.name(cctx.namexx(), "preloader#" + id, "idx#" + Long.toString(idx));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            while (!isCancelled()) {
                try {
                    barrier.await();
                }
                catch (BrokenBarrierException ignored) {
                    throw new InterruptedException("Demand worker stopped.");
                }

                Assignment assign = assigns.poll(cctx.gridConfig().getNetworkTimeout(), MILLISECONDS);

                if (assign == null)
                    continue;

                processAssignment(assign);

                leftAssigns.decrementAndGet();

                finish(); // Preloading finished with this assignment?
            }
        }

        /**
         * @param assign Assignment.
         * @throws GridInterruptedException If thread is interrupted.
         * @throws InterruptedException If thread is interrupted.
         */
        private void processAssignment(Assignment assign) throws GridInterruptedException, InterruptedException {
            assert assign != null;

            assert cctx.preloadEnabled();

            if (log.isDebugEnabled())
                log.debug("Processing assignment: " + assign);

            while (!isCancelled()) {
                Collection<GridRichNode> rmts = CU.allNodes(cctx, maxOrder.get());

                if (rmts.isEmpty())
                    return;

                List<GridRichNode> nodes = new ArrayList<GridRichNode>(cctx.affinity(assign.partition(), rmts));

                if (nodes.isEmpty())
                    return;

                Collections.sort(nodes);

                GridRichNode node = nodes.get(assign.mod() % nodes.size());

                try {
                    if (!demandFromNode(node, assign))
                        continue; // Retry to complete assignment with next node.

                    break; // Assignment has been processed.
                }
                catch (GridInterruptedException e) {
                    throw e;
                }
                catch (GridTopologyException e) {
                    if (log.isDebugEnabled())
                        log.debug("Node left during preloading (will retry) [node=" + node.id() +
                            ", msg=" + e.getMessage() + ']');
                }
                catch (GridException e) {
                    U.error(log, "Failed to receive entries from node (will retry): " + node.id(), e);
                }
            }
        }

        /**
         * @param node Node to demand from.
         * @param assign Assignment.
         * @return {@code True} if assignment has been fully processed.
         * @throws InterruptedException If thread is interrupted.
         * @throws GridException If failed.
         */
        private boolean demandFromNode(final GridNode node, Assignment assign) throws InterruptedException,
            GridException {

            GridLocalEventListener discoLsnr = new GridLocalEventListener() {
                @SuppressWarnings({"unchecked"})
                @Override public void onEvent(GridEvent evt) {
                    assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                    if (node.id().equals(((GridDiscoveryEvent)evt).eventNodeId()))
                        addMessage(DUMMY_MSG);
                }
            };

            cctx.events().addListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

            cntr++;

            GridReplicatedPreloadDemandMessage<K, V> d = new GridReplicatedPreloadDemandMessage<K, V>(
                assign.partition(), assign.mod(), assign.nodeCount(), topic(cntr), timeout.get(), id);

            // Drain queue before processing a new node.
            drainQueue();

            if (isCancelled())
                return true; // Pool is being stopped.

            cctx.io().addOrderedHandler(d.topic(), new CI2<UUID, GridReplicatedPreloadSupplyMessage<K, V>>() {
                @Override public void apply(UUID nodeId, GridReplicatedPreloadSupplyMessage<K, V> msg) {
                    addMessage(new SupplyMessage<K, V>(nodeId, msg));
                }
            });

            try {
                boolean retry;

                boolean stopOnDummy = false;

                // DoWhile.
                // =======
                do {
                    retry = false;

                    // Create copy.
                    d = new GridReplicatedPreloadDemandMessage<K, V>(d);

                    long timeout = GridReplicatedPreloadDemandPool.this.timeout.get();

                    d.timeout(timeout);

                    // Send demand message.
                    cctx.io().send(node, d);

                    if (log.isDebugEnabled())
                        log.debug("Sent demand message [node=" + node.id() + ", msg=" + d + ']');

                    // While.
                    // =====
                    while (!isCancelled()) {
                        SupplyMessage<K, V> s = poll(msgQ, timeout);

                        // If timed out.
                        if (s == null) {
                            if (msgQ.isEmpty()) { // Safety check.
                                U.warn(log, "Timed out waiting for preload response, will retry in " + timeout +
                                    " ms (you may need to increase 'networkTimeout' or 'preloadBatchSize'" +
                                    " configuration properties).");

                                growTimeout(timeout);

                                // Ordered listener was removed if timeout expired.
                                cctx.io().removeOrderedHandler(d.topic());

                                // Must create copy to be able to work with IO manager thread local caches.
                                d = new GridReplicatedPreloadDemandMessage<K, V>(d);

                                // Create new topic.
                                d.topic(topic(++cntr));

                                // Create new ordered listener.
                                cctx.io().addOrderedHandler(d.topic(),
                                    new CI2<UUID, GridReplicatedPreloadSupplyMessage<K, V>>() {
                                        @Override public void apply(UUID nodeId,
                                            GridReplicatedPreloadSupplyMessage<K, V> msg) {
                                            addMessage(new SupplyMessage<K, V>(nodeId, msg));
                                        }
                                    });

                                // Resend message with larger timeout.
                                retry = true;

                                break; // While.
                            }
                            else
                                continue; // While.
                        }
                        else if (s == DUMMY_MSG) {
                            if (!stopOnDummy) {
                                // Possibly event came prior to rest of messages from node.
                                stopOnDummy = true;

                                // Add dummy message to queue again.
                                addMessage(s);
                            }
                            else
                                // Quit preloading.
                                break;

                            continue;
                        }

                        // Check that message was received from expected node.
                        if (!s.senderId().equals(node.id())) {
                            U.warn(log, "Received supply message from unexpected node [expectedId=" + node.id() +
                                ", rcvdId=" + s.senderId() + ", msg=" + s + ']');

                            continue; // While.
                        }

                        GridReplicatedPreloadSupplyMessage<K, V> supply = s.message();

                        if (supply.failed()) {
                            // Node is preloading now and therefore cannot supply.
                            maxOrder.setIfLess(node.order() - 1); // Preload from nodes elder, than node.

                            // Quit preloading.
                            break;
                        }

                        // Check whether there were class loading errors on unmarshalling.
                        if (supply.classError() != null) {
                            if (log.isDebugEnabled())
                                log.debug("Class got undeployed during preloading: " + supply.classError());

                            retry = true;

                            // Retry preloading.
                            break;
                        }

                        preload(supply);

                        if (supply.last())
                            // Assignment is finished.
                            return true;
                    }
                }
                while (retry && !isCancelled());
            }
            finally {
                cctx.io().removeOrderedHandler(d.topic());

                cctx.events().removeListener(discoLsnr);
            }

            return false;
        }

        /**
         * @param supply Supply message.
         */
        private void preload(GridReplicatedPreloadSupplyMessage<K, V> supply) {
            lock.lock(); // Prevent evictions.

            try {
                for (GridCacheEntryInfo<K, V> info : supply.entries()) {
                    if (!cctx.evicts().preloadingPermitted(info.key(), info.version())) {
                        if (log.isDebugEnabled())
                            log.debug("Preloading is not permitted for entry due to evictions [key=" +
                                info.key() + ", ver=" + info.version() + ']');

                        continue;
                    }

                    GridCacheEntryEx<K, V> cached = null;

                    try {
                        cached = cctx.cache().entryEx(info.key());

                        if (!cached.initialValue(
                            info.value(),
                            info.valueBytes(),
                            info.version(),
                            info.ttl(),
                            info.expireTime(),
                            info.metrics())) {
                            if (log.isDebugEnabled())
                                log.debug("Preloading entry is already in cache (will ignore): " + cached);
                        }
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Entry has been concurrently removed while preloading: " + cached);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to put preloaded entry.", e);
                    }
                }
            }
            finally {
                lock.unlock(); // Let evicts run.
            }
        }

        /**
         * @throws InterruptedException If interrupted.
         */
        private void drainQueue() throws InterruptedException {
            while (msgQ.peek() != null) {
                SupplyMessage<K, V> msg = msgQ.take();

                if (log.isDebugEnabled())
                    log.debug("Drained supply message: " + msg);
            }
        }
    }

    /**
     * Supply message wrapper.
     */
    private static class SupplyMessage<K, V> extends GridTuple2<UUID, GridReplicatedPreloadSupplyMessage<K, V>> {
        /**
         * @param senderId Sender ID.
         * @param msg Message.
         */
        SupplyMessage(UUID senderId, GridReplicatedPreloadSupplyMessage<K, V> msg) {
            super(senderId, msg);
        }

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public SupplyMessage() {
            // No-op.
        }

        /**
         * @return Sender ID.
         */
        UUID senderId() {
            return get1();
        }

        /**
         * @return Message.
         */
        public GridReplicatedPreloadSupplyMessage<K, V> message() {
            return get2();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "SupplyMessage [senderId=" + senderId() + ", msg=" + message() + ']';
        }
    }

    /**
     * Assignment.
     */
    private static class Assignment {
        /** */
        private final int part;

        /** */
        private final int mod;

        /** */
        private final int cnt;

        /**
         * @param part Partition.
         * @param mod Mod.
         * @param cnt Node count.
         */
        private Assignment(int part, int mod, int cnt) {
            this.part = part;
            this.mod = mod;
            this.cnt = cnt;
        }

        /**
         * @return Partition.
         */
        public int partition() {
            return part;
        }

        /**
         * @return Mod.
         */
        public int mod() {
            return mod;
        }

        /**
         * @return Node count.
         */
        public int nodeCount() {
            return cnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Assignment.class, this);
        }
    }
}
