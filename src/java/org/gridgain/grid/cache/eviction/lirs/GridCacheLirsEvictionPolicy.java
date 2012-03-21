// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.lirs;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.eviction.lirs.GridCacheLirsEvictionPolicy.State.*;
import static org.gridgain.grid.lang.utils.GridConcurrentLinkedDeque.*;

/**
 * Very efficient implementation of {@code LIRS} cache eviction policy which often provides
 * better hit ratio than the {@code LRU} eviction policy. It in particular offers much better
 * performance for access patterns with weak locality, such as regular access over more
 * entries than the cache size. Instead of standard {@code LRU} eviction based on
 * access order, {@code LIRS} maintains a main {@code LRU} stack, called {@code LIRS Stack},
 * as primary {@code Low Inter-reference Recency} stack, and a secondary queue, called
 * {@code HIRS Queue}) for {@code High Inter-Reference} recency entries.
 * <p>
 * For more information see
 * <a href="http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.116.2184&rep=rep1&type=pdf">Low
 * Inter-Reference Recency Set (LIRS)</a>
 * algorithm by Sone Jiang and Xiaodong Zhang.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridCacheLirsEvictionPolicy<K, V> implements GridCacheEvictionPolicy<K, V>,
    GridCacheLirsEvictionPolicyMBean {
    /**
     * Default ratio of {@code HIRS} (High Inter-reference Recency Set). Default value is {@code 0.02},
     * which means that {@code HIRS} set size is {@code 2%} of {@code LIRS} set size.
     */
    public static final float DFLT_QUEUE_SIZE_RATIO = 0.02f;

    /** Default number of locks to synchronize operations. Default value is {@code 256}. */
    public static final int DFLT_LOCKS_CNT = 256;

    /** LIRS stack. */
    @GridToStringExclude
    private final LirsStack stack = new LirsStack();

    /** LIRS queue. */
    @GridToStringExclude
    private final HirsQueue queue = new HirsQueue();

    /** Maximum stack size. */
    private volatile int max = -1;

    /** Flag indicating whether to allow empty entries. */
    private volatile boolean allowEmptyEntries = true;

    /** Ratio of {@code HIRS} (High Inter-reference Recency Set). */
    private double queueRatio = DFLT_QUEUE_SIZE_RATIO;

    /** Meta tag. */
    @GridToStringExclude
    private final String meta = UUID.randomUUID().toString();

    /** Locks count. */
    private int locksCnt = DFLT_LOCKS_CNT;

    /** Locks array. */
    private Lock[] locks;

    /**
     * Constructs LIRS eviction policy with all defaults.
     */
    public GridCacheLirsEvictionPolicy() {
        initLocks();
    }

    /**
     * Constructs LIRS eviction policy with maximum size.
     *
     * @param max Maximum allowed size of entries in cache.
     */
    public GridCacheLirsEvictionPolicy(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;

        initLocks();
    }

    /**
     * Constructs LIRS eviction policy with maximum size and specified allow empty entries flag.
     *
     * @param max Maximum allowed size of entries in cache.
     * @param allowEmptyEntries If {@code false}, empty entries will be evicted immediately.
     */
    public GridCacheLirsEvictionPolicy(int max, boolean  allowEmptyEntries) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
        this.allowEmptyEntries = allowEmptyEntries;

        initLocks();
    }

    /**
     * Constructs LIRS eviction policy with maximum size and secondary queue ratio to compute
     * size of secondary queue.
     *
     * @param max Maximum allowed size of entries in cache.
     * @param queueRatio Ratio of {@code HIRS} queue size compared to maximum allowed size.
     */
    public GridCacheLirsEvictionPolicy(int max, float queueRatio) {
        A.ensure(max > 0, "max > 0");
        A.ensure(queueRatio > 0 && queueRatio <= 1, "queueRatio > 0 && queueRatio <= 1");

        this.max = max;
        this.queueRatio = queueRatio;

        initLocks();
    }

    /**
     * Constructs LIRS eviction policy with maximum size and secondary queue ratio to compute
     * size of secondary queue.
     *
     * @param max Maximum allowed size of entries in cache.
     * @param queueRatio Ratio of {@code HIRS} queue size compared to maximum allowed size.
     * @param allowEmptyEntries If {@code false}, empty entries will be evicted immediately.
     */
    public GridCacheLirsEvictionPolicy(int max, float queueRatio, boolean allowEmptyEntries) {
        A.ensure(max > 0, "max > 0");
        A.ensure(queueRatio > 0 && queueRatio <= 1, "queueRatio > 0 && queueRatio <= 1");

        this.max = max;
        this.queueRatio = queueRatio;
        this.allowEmptyEntries = allowEmptyEntries;

        initLocks();
    }

    /**
     * Inits locks.
     */
    private void initLocks() {
        locks = new Lock[locksCnt];

        for (int i = 0; i < locksCnt; i++)
            locks[i] = new ReentrantLock();
    }

    /** {@inheritDoc} */
    @Override public int getMaxSize() {
        return max;
    }

    /**
     * Sets maximum allowed size of cached entries.
     *
     * @param max Maximum allowed size of cached entries.
     */
    @Override public void setMaxSize(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /** {@inheritDoc} */
    @Override public boolean isAllowEmptyEntries() {
        return allowEmptyEntries;
    }

    /** {@inheritDoc} */
    @Override public void setAllowEmptyEntries(boolean allowEmptyEntries) {
        this.allowEmptyEntries = allowEmptyEntries;
    }

    /** {@inheritDoc} */
    @Override public double getQueueSizeRatio() {
        return queueRatio;
    }

    /**
     * Sets ratio of {@code HIRS} queue size compared to main stack size. Generally {@code HIRS}
     * size should be much smaller than main stack size. The default value is {@code 0.02}
     * defined by {@link #DFLT_QUEUE_SIZE_RATIO} constant.
     *
     * @param queueRatio Ratio of {@code HIRS} set size compared to main stack size.
     */
    public void setQueueSizeRatio(double queueRatio) {
        A.ensure(queueRatio > 0 && queueRatio <= 1, "queueRatio > 0 && queueRatio <= 1");

        this.queueRatio = queueRatio;
    }

    /** {@inheritDoc} */
    @Override public int getLocksCount() {
        return locksCnt;
    }

    /**
     * Sets locks count to synchronize operations.
     * <p>
     * This parameter may be interpreted as {@code concurrency level} parameter
     * of {@code java.util.concurrent.ConcurrentHashMap}.
     * <p>
     * If not provided, default value is {@link #DFLT_LOCKS_CNT}.
     *
     * @param locksCnt Locks count.
     */
    public void setLocksCount(int locksCnt) {
        A.ensure(locksCnt > 0, "locksCnt > 0");

        this.locksCnt = locksCnt;
    }

    /** {@inheritDoc} */
    @Override public String getMetaAttributeName() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public int getMaxQueueSize() {
        return (int)Math.ceil(max * queueRatio);
    }

    /** {@inheritDoc} */
    @Override public int getMaxStackSize() {
        return (int)Math.floor(max * (1 - queueRatio));
    }

    /**
     * @return Current main stack size.
     */
    @Override public int getCurrentStackSize() {
        return stack.sizex();
    }

    /**
     * @return Current HIRS set size.
     */
    @Override public int getCurrentQueueSize() {
        return queue.sizex();
    }

    /**
     * Gets read-only view on main internal stack for {@code LIRS} implementation.
     *
     * @return Read-only view on Main internal stack for {@code LIRS} implementation.
     */
    public Collection<GridCacheEntry<K, V>> stack() {
        return stack.entries();
    }

    /**
     * Gets read-only view on secondary {@code HIR} queue to hold {@code High Inter-reference Recency}
     * entries.
     *
     * @return Secondary read-only view on {@code HIR} queue.
     */
    public Collection<GridCacheEntry<K, V>> queue() {
        return queue.entries();
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<K, V> entry) {
        if (!rmv) {
            if (!entry.isCached())
                return;

            if (!allowEmptyEntries && empty(entry)) {
                Capsule c = entry.meta(meta);

                if (c != null) {
                    c.lock();

                    try {
                        c.clear();
                    }
                    finally {
                        c.unlock();
                    }
                }

                if (!entry.evict())
                    touch(entry);
            }
            else
                touch(entry);
        }
        else {
            Capsule c = entry.meta(meta);

            if (c != null) {
                c.lock();

                try {
                    c.clear();
                }
                finally {
                    c.unlock();
                }
            }
        }
    }

    /**
     * Checks entry for empty value.
     *
     * @param entry Entry to check.
     * @return {@code True} if entry is empty.
     */
    private boolean empty(GridCacheEntry<K, V> entry) {
        try {
            return !entry.hasValue(GLOBAL);
        }
        catch (GridException e) {
            U.error(null, e.getMessage(), e);

            assert false : "Should never happen: " + e;

            return false;
        }
    }

    /**
     * @param entry Entry to touch.
     */
    @SuppressWarnings("TooBroadScope")
    private void touch(GridCacheEntry<K, V> entry) {
        Capsule c = entry.meta(meta);

        Lock lock = c == null ? locks[Math.abs(entry.getKey().hashCode()) % locksCnt] : c.getLock();

        boolean prune = false;
        boolean demote = false;
        boolean evict = false;

        State initState = stack.sizex() < getMaxStackSize() ? LIR : HIR_R;

        lock.lock();

        try {
            boolean miss = false;

            if (c == null) {
                Capsule old = entry.putMetaIfAbsent(meta, c = new Capsule(entry, initState, lock));

                if (old != null)
                    c = old;
                else
                    miss = true;
            }

            // Replace removed capsule.
            if (c.cleared())
                entry.addMeta(meta, c = new Capsule(entry, initState, lock));

            if (!entry.isCached()) {
                c.clear();

                return;
            }

            switch (c.state()) {
                // Low inter-recency.
                case LIR:
                    if (stack.isFirst(c))
                        prune = true;

                    c.addStackNode(LIR);

                    break;

                // High inter-recency resident.
                case HIR_R:
                    if (c.inStack()) {
                        // Add to stack.
                        c.addStackNode(LIR);

                        // Remove from queue.
                        c.dequeue();

                        // Demote LIR from stack head to HIR_R.
                        demote = true;
                        prune = true;
                    }
                    else {
                        // Add to the top of the stack.
                        c.addStackNode(HIR_R);

                        // Move to the end of the queue.
                        c.addQueueNode(HIR_R);

                        if (miss && stack.full()) {
                            demote = true;
                            prune = true;
                        }
                    }

                    break;

                // High inter-recency non-resident.
                case HIR_NR:
                    // Dequeue from head and change to HIR_NR.
                    evict = true;

                    if (c.inStack()) {
                        c.addStackNode(LIR);

                        demote = true;
                        prune = true;
                    }
                    else {
                        c.addStackNode(HIR_R);
                        c.addQueueNode(HIR_R);
                    }

                    break;

                default:
                    assert false;
            }
        }
        finally {
            lock.unlock();
        }

        if (evict) {
            while (true) {
                Capsule cap = queue.poll();

                if (cap == null)
                    break;

                cap.lock();

                try {
                    if (cap.pollQueue())
                        break;
                }
                finally {
                    cap.unlock();
                }
            }
        }

        // Shrink the HIR queue.
        if (queue.shrink())
            prune = true;

        if (prune) {
            while (true) {
                Capsule cap = stack.prune();

                if (cap != null && demote) {
                    cap.lock();

                    try {
                        if (!cap.demote())
                            continue;
                    }
                    finally {
                        cap.unlock();
                    }

                    // Prune again.
                    stack.prune();

                    queue.shrink();
                }

                break;
            }
        }
    }

    /**
     * Gets string representation of all queue and stack contents.
     *
     * @return String representation of all queue and stack contents.
     */
    public String toFullString() {
        return S.toString(GridCacheLirsEvictionPolicy.class, this,
            "maxStack", getMaxStackSize(),
            "maxQueue", getMaxQueueSize(),
            "stack", stack,
            "queueSize", queue);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLirsEvictionPolicy.class, this,
            "maxStack", getMaxStackSize(),
            "maxQueue", getMaxQueueSize(),
            "stackSize", stack.sizex(),
            "queueSize", queue.sizex());
    }

    /**
     * Concurrent generic linked queue.
     */
    private abstract class LinkedQueue extends GridConcurrentLinkedDeque<Capsule> {
        /**
         * Collection view over cache entries (effectively skipping {@code nulls}.
         *
         * @return Collection view over cache entries.
         */
        public Collection<GridCacheEntry<K, V>> entries() {
            final GridTuple<GridCacheEntry<K, V>> t = F.t1();

            return F.viewReadOnly(this, new C1<Capsule, GridCacheEntry<K, V>>() {
                @Override public GridCacheEntry<K, V> apply(Capsule c) {
                    return t.get();
                }
            }, new P1<Capsule>() {
                @Override public boolean apply(Capsule c) {
                    GridCacheEntry<K, V> e = c.entry();

                    if (e != null) {
                        t.set(e);

                        return true;
                    }

                    return false;
                }
            });
        }

        /**
         * Checks if capsule is first.
         *
         * @param c Capsule.
         * @return {@code True} if capsule is first.
         */
        public boolean isFirst(Capsule c) {
            while (true) {
                Node<Capsule> n = peekx();

                if (n == null)
                    return false;

                Capsule cap = n.item();

                if (cap == null)
                    continue; // Try again.

                return cap == c;
            }
        }
    }

    /**
     * HIRS queue.
     */
    private class HirsQueue extends LinkedQueue {
        /**
         * @return {@code True} if queue changed.
         */
        boolean shrink() {
            while (sizex() > getMaxQueueSize()) {
                Capsule c = poll();

                if (c == null)
                    return false;

                c.lock();

                try {
                    if (c.state() == HIR_R) {
                        c.state(HIR_NR);

                        if (!c.inStack()) {
                            // It's OK to evict while holding lock on capsule.
                            if (!c.entry().evict())
                                // Add to the top again.
                                c.addStackNode(LIR);
                        }

                        return true;
                    }
                }
                finally {
                    c.unlock();
                }
            }

            return false;
        }

        /**
         * @return {@code True} if size is excessive.
         */
        boolean full() {
            return sizex() >= getMaxQueueSize();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(HirsQueue.class, this, "size", sizex(), "elements", super.toString());
        }
    }

    /**
     * Lirs stack.
     */
    private class LirsStack extends LinkedQueue {
        /**
         * Prunes stack.
         *
         * @return First LIR capsule.
         */
        @Nullable Capsule prune() {
            Capsule ret = null;

            while (true) {
                // This will clear obsolete nodes.
                Node<Capsule> first = peekx();

                if (first == null)
                    break;

                Capsule c = first.item();

                // If capsule is null, then another thread pruned or cleared.
                // In this case we simply try again.
                if (c != null) {
                    c.lock();

                    try {
                        if (c.cleared()) {
                            if (first.item() != null)
                                stack.unlinkx(first); // Strange.

                            // Try again.
                            continue;
                        }

                        if (c.state() == HIR_R)
                            c.unstack();
                        // If need to evict.
                        else if (c.state() == HIR_NR) {
                            if (c.unstack())
                                if (!c.entry().evict())
                                    // Add to the top again.
                                    c.addStackNode(LIR);
                        }
                        else {
                            ret = c;

                            break;
                        }
                    }
                    finally {
                        c.unlock();
                    }
                }
            }

            return ret;
        }

        /**
         * @return {@code True} if size is greater or equal to {@code 'max'} cache size.
         */
        boolean full() {
            // We specifically compare against maximum cache size, since we don't keep
            // explicit count for non-resident nodes. This way stack may get slightly
            // over-sized, but implementation is simpler.
            return sizex() >= max;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LirsStack.class, this, "size", sizex(), "elements", super.toString());
        }
    }

    /**
     * Capsule.
     */
    private class Capsule {
        /** Stack node. */
        @GridToStringExclude
        private Node<Capsule> stackNode;

        /** Queue node. */
        @GridToStringExclude
        private Node<Capsule> queueNode;

        /** State. */
        @GridToStringInclude
        private State state;

        /** Entry. */
        @GridToStringInclude
        private GridCacheEntry<K, V> entry;

        /** Lock. */
        private final Lock lock;

        /**
         * @param entry Entry.
         * @param state Initial state.
         * @param lock Lock.
         */
        Capsule(GridCacheEntry<K, V> entry, State state, Lock lock) {
            assert entry != null;
            assert state != null;
            assert lock != null;

            this.state = state;
            this.entry = entry;
            this.lock = lock;
        }

        /**
         *
         */
        void clear() {
            if (entry != null) {
                dequeue();
                unstack();

                entry = null;

                state = null;
            }
        }

        /**
         * @return {@code True} if capsule is cleared.
         */
        boolean cleared() {
            return entry == null;
        }

        /**
         * @return Entry.
         */
        GridCacheEntry<K, V> entry() {
            return entry;
        }

        /**
         * @return {@code True} if in stack.
         */
        boolean inStack() {
            return stackNode != null && stackNode.item() != null;
        }

        /**
         * @return {@code True} if in queue.
         */
        boolean inQueue() {
            return queueNode != null && queueNode.item() != null;
        }

        /**
         * @param state State.
         * @return Node.
         */
        Node<Capsule> addStackNode(State state) {
            if (stackNode != null && stackNode.item() != null)
                stack.unlinkx(stackNode);

            this.state = state;

            return stackNode = stack.offerx(this);
        }

        /**
         * @param state State.
         * @return Node.
         */
        Node<Capsule> addQueueNode(State state) {
            if (queueNode != null && queueNode.item() != null)
                queue.unlinkx(queueNode);

            this.state = state;

            return queueNode = queue.offerx(this);
        }

        /**
         * @return {@code True} if dequeued.
         */
        boolean dequeue() {
            if (cleared()) {
                assert queueNode == null || queueNode.item() == null;

                return false;
            }

            if (queueNode != null && queueNode.item() != null) {
                queue.unlinkx(queueNode);

                queueNode = null;

                return true;
            }

            return false;
        }

        /**
         * @return {@code True} if unstacked.
         */
        boolean unstack() {
            if (cleared())
                return false;

            if (stackNode != null && stackNode.item() != null) {
                stack.unlinkx(stackNode);

                stackNode = null;

                return true;
            }

            return false;
        }

        /**
         * @return {@code True} if demoted.
         */
        boolean demote() {
            if (cleared())
                return false;

            if (state == LIR) {
                unstack();

                addQueueNode(HIR_R);

                return true;
            }

            return false;
        }

        /**
         * @return {@code True} if changed to non-resident status.
         */
        boolean pollQueue() {
            if (cleared())
                return false;

            if (state == HIR_R) {
                state = HIR_NR;

                dequeue();

                if (!inStack())
                    if (!entry.evict())
                        // Add to the top again.
                        addStackNode(LIR);

                return true;
            }

            return false;
        }

        /**
         * @return Stack node.
         */
        Node<Capsule> stackNode() {
            return stackNode;
        }

        /**
         *  @return Queue node .
         */
        Node<Capsule> queueNode() {
            return queueNode;
        }

        /**
         * @return State.
         */
        State state() {
            return state;
        }

        /**
         * @param state State.
         */
        void state(State state) {
            this.state = state;
        }

        /**
         * @return Lock associated with capsule.
         */
        Lock getLock() {
            return lock;
        }

        /**
         *
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        void lock() {
            lock.lock();
        }

        /**
         *
         */
        void unlock() {
            lock.unlock();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Capsule.class, this);
        }
    }

    /**
     * LIRS state.
     */
    @SuppressWarnings({"PackageVisibleInnerClass"})
    enum State {
        LIR, HIR_R, HIR_NR
    }
}
