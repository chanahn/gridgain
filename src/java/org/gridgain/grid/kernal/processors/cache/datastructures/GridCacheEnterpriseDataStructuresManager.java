// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Manager of data structures.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public final class GridCacheEnterpriseDataStructuresManager<K, V> extends GridCacheDataStructuresManager<K, V> {
    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 10;

    /** Cache contains only {@code GridCacheInternal,GridCacheInternal}. */
    private GridCacheProjection<GridCacheInternal, GridCacheInternal> dsView;

    /** Internal storage of all dataStructures items (sequence, queue , atomic long etc.). */
    private final ConcurrentMap<GridCacheInternal, GridCacheRemovable> dsMap;

    /** Cache contains only {@code GridCacheAtomicValue}. */
    private GridCacheProjection<GridCacheInternalStorableKey, GridCacheAtomicLongValue> atomicLongView;

    /** Cache contains only {@code GridCacheCountDownLatchValue}. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheCountDownLatchValue> cntDownLatchView;

    /** Cache contains only {@code GridCacheAtomicReferenceValue}. */
    private GridCacheProjection<GridCacheInternalStorableKey, GridCacheAtomicReferenceValue> atomicRefView;

    /** Cache contains only {@code GridCacheAtomicStampedValue}. */
    private GridCacheProjection<GridCacheInternalKey, GridCacheAtomicStampedValue> atomicStampedView;

    /** Cache contains only entry {@code GridCacheSequenceValue}.  */
    private GridCacheProjection<GridCacheInternalStorableKey, GridCacheAtomicSequenceValue> seqView;

    /** Cache contains only entry {@code GridCacheQueueHeader}.  */
    private GridCacheProjection<GridCacheInternalKey, GridCacheQueueHeader> queueHdrView;

    /** Cache contains only entry {@code GridCacheQueueItem}  */
    private GridCacheProjection<GridCacheQueueItemKey, GridCacheQueueItem> queueItemView;

    /** Query factory. */
    private GridCacheQueueQueryFactory queueQryFactory;

    /** Local cache of annotated methods and fields by classes. */
    private final GridCacheAnnotationHelper<GridCacheQueuePriority> annHelper;

    /** Init latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Init flag. */
    private boolean initFlag;

    /**
     * Default constructor.
     */
    public GridCacheEnterpriseDataStructuresManager() {
        dsMap = new ConcurrentHashMap<GridCacheInternal, GridCacheRemovable>(INITIAL_CAPACITY);

        annHelper = new GridCacheAnnotationHelper<GridCacheQueuePriority>(GridCacheQueuePriority.class);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() {
        assert !cctx.isDht();

        try {
            dsView = cctx.cache().<GridCacheInternal, GridCacheInternal>projection
                (GridCacheInternal.class, GridCacheInternal.class).flagsOn(CLONE);

            cntDownLatchView = cctx.cache().<GridCacheInternalKey, GridCacheCountDownLatchValue>projection
                (GridCacheInternalKey.class, GridCacheCountDownLatchValue.class).flagsOn(CLONE);

            atomicLongView = cctx.cache().<GridCacheInternalStorableKey, GridCacheAtomicLongValue>projection
                (GridCacheInternalStorableKey.class, GridCacheAtomicLongValue.class).flagsOn(CLONE);

            atomicRefView = cctx.cache().<GridCacheInternalStorableKey, GridCacheAtomicReferenceValue>projection
                (GridCacheInternalStorableKey.class, GridCacheAtomicReferenceValue.class).flagsOn(CLONE);

            atomicStampedView = cctx.cache().<GridCacheInternalKey, GridCacheAtomicStampedValue>projection
                (GridCacheInternalKey.class, GridCacheAtomicStampedValue.class).flagsOn(CLONE);

            seqView = cctx.cache().<GridCacheInternalStorableKey, GridCacheAtomicSequenceValue>projection
                (GridCacheInternalStorableKey.class, GridCacheAtomicSequenceValue.class).flagsOn(CLONE);

            queueHdrView = cctx.cache().<GridCacheInternalKey, GridCacheQueueHeader>projection
                (GridCacheInternalKey.class, GridCacheQueueHeader.class).flagsOn(CLONE);

            queueItemView = cctx.cache().<GridCacheQueueItemKey, GridCacheQueueItem>projection
                (GridCacheQueueItemKey.class, GridCacheQueueItem.class).flagsOn(CLONE);

            queueQryFactory = new GridCacheQueueQueryFactory(cctx);

            initFlag = true;
        }
        finally {
            initLatch.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override public void onPartitionsChange() {
        for (Map.Entry<GridCacheInternal, GridCacheRemovable> e : dsMap.entrySet()) {
            GridCacheRemovable v = e.getValue();

            if (v instanceof GridCacheQueue) {
                GridCacheInternal key = e.getKey();

                try {
                    // Re-initialize near cache.
                    GridCacheQueueHeader hdr = cast(dsView.get(key), GridCacheQueueHeader.class);

                    if (hdr != null) {
                        GridCacheQueueEx queue = cast(v, GridCacheQueueEx.class);

                        queue.onHeaderChanged(hdr);
                    }
                    else if (v != null) {
                        GridCacheQueueEx queue = cast(v, GridCacheQueueEx.class);

                        queue.onRemoved();
                    }
                }
                catch (GridException ex) {
                    log.error("Failed to synchronize queue state (will invalidate the queue): " + v, ex);

                    v.onInvalid(ex);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public final GridCacheAtomicSequence sequence(final String name, final long initVal,
        final boolean persistent, final boolean create) throws GridException {
        waitInitialization();

        CX1<Object, GridCacheAtomicSequenceValue> c = null;

        if (persistent){
            c = new CX1<Object, GridCacheAtomicSequenceValue>() {
                @Nullable @Override public GridCacheAtomicSequenceValue applyx(Object val) throws GridException {
                    if (val == null)
                        return null;

                    if (!(val instanceof Long))
                        throw new GridException("Failed to cast object [expected=Long, actual=" + val.getClass()
                            + ", value=" + val + ']');

                    return new GridCacheAtomicSequenceValue((Long)val, persistent);
                }
            };
        }

        final GridCacheInternalStorableKey key =
            new GridCacheInternalStorableKeyImpl<Object, GridCacheAtomicSequenceValue>(name, c);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicSequence val = cast(dsMap.get(key), GridCacheAtomicSequence.class);

            if (val != null)
                return val;

            return CU.outTx(new Callable<GridCacheAtomicSequence>() {
                    @Override public GridCacheAtomicSequence call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheAtomicSequenceValue seqVal = cast(dsView.get(key), GridCacheAtomicSequenceValue.class);

                            // Check that sequence hasn't been created in other thread yet.
                            GridCacheAtomicSequenceEx seq = cast(dsMap.get(key), GridCacheAtomicSequenceEx.class);

                            if (seq != null) {
                                assert seqVal != null;

                                return seq;
                            }

                            if (seqVal == null && !create)
                                return null;

                            /* We should use offset because we already reserved left side of range.*/
                            long off = cctx.config().getAtomicSequenceReserveSize() > 1 ?
                                cctx.config().getAtomicSequenceReserveSize() - 1 : 1;

                            long upBound;
                            long locCntr;

                            if (seqVal == null) {
                                locCntr = initVal;

                                upBound = locCntr + off;

                                // Global counter must be more than reserved region.
                                seqVal = new GridCacheAtomicSequenceValue(upBound + 1, persistent);
                            }
                            else {
                                locCntr = seqVal.get();

                                upBound = locCntr + off;

                                // Global counter must be more than reserved region.
                                seqVal.set(upBound + 1);
                            }

                            // Update global counter.
                            dsView.putx(key, seqVal);

                            // Only one thread can be in the transaction scope and create sequence.
                            seq = new GridCacheAtomicSequenceImpl(name, key, seqView, cctx,
                                locCntr, upBound);

                            dsMap.put(key, seq);

                            tx.commit();

                            return seq;
                        }
                        catch (Error e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic sequence: " + name, e);

                            throw e;
                        }
                        catch (Exception e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic sequence: " + name, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get sequence by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeSequence(String name) throws GridException {
        waitInitialization();

        try {
            GridCacheInternal key =
                new GridCacheInternalStorableKeyImpl<Long, GridCacheAtomicSequenceValue>(name, null);

            return removeInternal(key, GridCacheAtomicSequenceValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove sequence by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final GridCacheAtomicLong atomicLong(final String name, final long initVal,
        final boolean persistent, final boolean create) throws GridException {
        waitInitialization();

        CX1<Object, GridCacheAtomicLongValue> c = null;

        if (persistent) {
            c = new CX1<Object, GridCacheAtomicLongValue>() {
                @Nullable @Override public GridCacheAtomicLongValue applyx(Object val) throws GridException {
                    if (val == null)
                        return null;

                    if (!(val instanceof Long))
                        throw new GridException("Failed to cast object [expected=Long, actual=" + val.getClass()
                            + ", value=" + val + ']');

                    return new GridCacheAtomicLongValue((Long)val, persistent);
                }
            };
        }

        final GridCacheInternalStorableKey key =
            new GridCacheInternalStorableKeyImpl<Object, GridCacheAtomicLongValue>(name, c);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicLong atomicLong = cast(dsMap.get(key), GridCacheAtomicLong.class);

            if (atomicLong != null)
                return atomicLong;

            return CU.outTx(new Callable<GridCacheAtomicLong>() {
                    @Override public GridCacheAtomicLong call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheAtomicLongValue val = cast(dsView.get(key),
                                GridCacheAtomicLongValue.class);

                            // Check that atomic long hasn't been created in other thread yet.
                            GridCacheAtomicLongEx a = cast(dsMap.get(key), GridCacheAtomicLongEx.class);

                            if (a != null) {
                                assert val != null;

                                return a;
                            }

                            if (val == null && !create)
                                return null;

                            if (val == null) {
                                val = new GridCacheAtomicLongValue(initVal, persistent);

                                dsView.putx(key, val);
                            }

                            a = new GridCacheAtomicLongImpl(name, key, atomicLongView, cctx);

                            dsMap.put(key, a);

                            tx.commit();

                            return a;
                        }
                        catch (Error e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic long: " + name, e);

                            throw e;
                        }
                        catch (Exception e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic long: " + name, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic long by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAtomicLong(String name) throws GridException {
        waitInitialization();

        try {
            GridCacheInternal key =
                new GridCacheInternalStorableKeyImpl<Long, GridCacheAtomicLongValue>(name, null);

            return removeInternal(key, GridCacheAtomicLongValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic long by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override @SuppressWarnings("unchecked")
    public final <T> GridCacheAtomicReference<T> atomicReference(final String name, final T initVal,
        final boolean persistent, final boolean create) throws GridException {
        waitInitialization();

        CX1<Object, GridCacheAtomicReferenceValue<T>> c = null;

        if (persistent) {
            c = new CX1<Object,
                GridCacheAtomicReferenceValue<T>>() {
                @Nullable @Override public GridCacheAtomicReferenceValue<T> applyx(Object val) throws GridException {
                    if (val == null)
                        return null;

                    T locVal;

                    try {
                        locVal = (T)val;
                    }
                    catch (ClassCastException e) {
                        throw new GridException("Failed to cast object: " + val.getClass(), e);
                    }

                    return new GridCacheAtomicReferenceValue(locVal, persistent);
                }
            };
        }

        final GridCacheInternalStorableKey<GridCacheAtomicReferenceValue<T>, T> key =
            new GridCacheInternalStorableKeyImpl(name, c);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicReference atomicRef = cast(dsMap.get(key), GridCacheAtomicReference.class);

            if (atomicRef != null)
                return atomicRef;

            return CU.outTx(new Callable<GridCacheAtomicReference<T>>() {
                    @Override public GridCacheAtomicReference<T> call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheAtomicReferenceValue val = cast(dsView.get(key),
                                GridCacheAtomicReferenceValue.class);

                            // Check that atomic reference hasn't been created in other thread yet.
                            GridCacheAtomicReferenceEx ref = cast(dsMap.get(key),
                                GridCacheAtomicReferenceEx.class);

                            if (ref != null) {
                                assert val != null;

                                return ref;
                            }

                            if (val == null && !create)
                                return null;

                            if (val == null) {
                                val = new GridCacheAtomicReferenceValue(initVal, persistent);

                                dsView.putx(key, val);
                            }

                            ref = new GridCacheAtomicReferenceImpl(name, key, atomicRefView, cctx);

                            dsMap.put(key, ref);

                            tx.commit();

                            return ref;
                        }
                        catch (Error e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic reference: " + name, e);

                            throw e;
                        }
                        catch (Exception e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic reference: " + name, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic reference by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAtomicReference(String name) throws GridException {
        waitInitialization();

        try {
            GridCacheInternal key =
                new GridCacheInternalStorableKeyImpl<Long, GridCacheAtomicReferenceValue>(name, null);

            return removeInternal(key, GridCacheAtomicReferenceValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic reference by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final <T, S> GridCacheAtomicStamped<T, S> atomicStamped(final String name, final T initVal,
        final S initStamp, final boolean create) throws GridException {
        waitInitialization();

        final GridCacheInternalKeyImpl key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheAtomicStamped atomicStamped = cast(dsMap.get(key), GridCacheAtomicStamped.class);

            if (atomicStamped != null)
                return atomicStamped;

            return CU.outTx(new Callable<GridCacheAtomicStamped<T, S>>() {
                    @Override public GridCacheAtomicStamped<T, S> call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheAtomicStampedValue val = cast(dsView.get(key),
                                GridCacheAtomicStampedValue.class);

                            // Check that atomic stamped hasn't been created in other thread yet.
                            GridCacheAtomicStampedEx stmp = cast(dsMap.get(key),
                                GridCacheAtomicStampedEx.class);

                            if (stmp != null) {
                                assert val != null;

                                return stmp;
                            }

                            if (val == null && !create)
                                return null;

                            if (val == null) {
                                val = new GridCacheAtomicStampedValue(initVal, initStamp);

                                dsView.putx(key, val);
                            }

                            stmp = new GridCacheAtomicStampedImpl(name, key, atomicStampedView, cctx);

                            dsMap.put(key, stmp);

                            tx.commit();

                            return stmp;
                        }
                        catch (Error e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic stamped: " + name, e);

                            throw e;
                        }
                        catch (Exception e) {
                            dsMap.remove(key);

                            log.error("Failed to make atomic stamped: " + name, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get atomic stamped by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeAtomicStamped(String name) throws GridException {
        waitInitialization();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            return removeInternal(key, GridCacheAtomicStampedValue.class);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove atomic stamped by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final <T> GridCacheQueue<T> queue(final String name, final GridCacheQueueType type,
        final int cap, boolean colloc, final boolean create) throws GridException {
        A.ensure(cap > 0, "cap > 0");

        waitInitialization();

        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean collocMode = cctx.cache().configuration().getCacheMode() != PARTITIONED
            || colloc;

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheQueue queue = cast(dsMap.get(key), GridCacheQueue.class);

            if (queue != null)
                return queue;

            return CU.outTx(new Callable<GridCacheQueue<T>>() {
                    @Override public GridCacheQueue<T> call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheQueueHeader hdr = cast(dsView.get(key), GridCacheQueueHeader.class);

                            GridCacheQueueEx queue = cast(dsMap.get(key), GridCacheQueueEx.class);

                            if (queue != null) {
                                assert hdr != null : "Failed to find queue header in cache: " + queue;

                                return queue;
                            }

                            if (hdr == null) {
                                if (!create)
                                    return null;

                                hdr = new GridCacheQueueHeader(name, type, cap, collocMode);

                                dsView.putx(key, hdr);
                            }

                            queue = new GridCacheQueueImpl(name, hdr, key, cctx, queueHdrView, queueItemView,
                                queueQryFactory);

                            GridCacheRemovable prev = dsMap.put(key, queue);

                            assert prev == null;

                            tx.commit();

                            return queue;
                        }
                        catch (Error e) {
                            dsMap.remove(key);

                            log.error("Failed to create queue: " + name, e);

                            throw e;
                        }
                        catch (Exception e) {
                            dsMap.remove(key);

                            log.error("Failed to create queue: " + name, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get queue by name :" + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean removeQueue(String name, int batchSize) throws GridException {
        waitInitialization();

        try {
            GridCacheInternal key = new GridCacheInternalKeyImpl(name);

            GridCacheQueueEx queue = cast(dsMap.get(key), GridCacheQueueEx.class);

            return queue != null && queue.removeQueue(batchSize);
        }
        catch (Exception e) {
            throw new GridException("Failed to remove queue by name :" + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheCountDownLatch countDownLatch(final String name, final int cnt, final boolean autoDel,
        final boolean create) throws GridException {
        A.ensure(cnt >= 0, "count can not be negative" );

        waitInitialization();

        final GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

        try {
            // Check type of structure received by key from local cache.
            GridCacheCountDownLatch latch = cast(dsMap.get(key), GridCacheCountDownLatch.class);

            if (latch != null)
                return latch;

            return CU.outTx(new Callable<GridCacheCountDownLatch>() {
                    @Override public GridCacheCountDownLatch call() throws Exception {
                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            GridCacheCountDownLatchValue val = cast(dsView.get(key),
                                GridCacheCountDownLatchValue.class);

                            // Check that count down hasn't been created in other thread yet.
                            GridCacheCountDownLatchEx latch = cast(dsMap.get(key), GridCacheCountDownLatchEx.class);

                            if (latch != null) {
                                assert val != null;

                                return latch;
                            }

                            if (val == null && !create)
                                return null;

                            if (val == null) {
                                val = new GridCacheCountDownLatchValue(cnt, autoDel);

                                dsView.putx(key, val);
                            }

                            latch = new GridCacheCountDownLatchImpl(name, val.get(), val.initialCount(),
                                val.autoDelete(), key, cntDownLatchView, cctx);

                            dsMap.put(key, latch);

                            tx.commit();

                            return latch;
                        }
                        catch (Error e) {
                            dsMap.remove(key);

                            log.error("Failed to create count down latch: " + name, e);

                            throw e;
                        }
                        catch (Exception e) {
                            dsMap.remove(key);

                            log.error("Failed to create count down latch: " + name, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                }, cctx);
        }
        catch (Exception e) {
            throw new GridException("Failed to get count down latch by name: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeCountDownLatch(final String name) throws GridException {
        waitInitialization();

        try {
            return CU.outTx(
                new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        GridCacheInternal key = new GridCacheInternalKeyImpl(name);

                        GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                        try {
                            // Check correctness type of removable object.
                            GridCacheCountDownLatchValue val =
                                cast(dsView.get(key), GridCacheCountDownLatchValue.class);

                            if (val != null) {
                                if (val.get() > 0) {
                                    throw new GridException("Failed to remove count down latch " +
                                        "with non-zero count: " + val.get());
                                }

                                dsView.removex(key);

                                tx.commit();
                            }
                            else
                                tx.setRollbackOnly();

                            return val != null;
                        }
                        catch (Error e) {
                            log.error("Failed to remove data structure: " + key, e);

                            throw e;
                        }
                        catch (Exception e) {
                            log.error("Failed to remove data structure: " + key, e);

                            throw e;
                        }
                        finally {
                            tx.end();
                        }
                    }
                },
                cctx
            );
        }
        catch (Exception e) {
            throw new GridException("Failed to remove count down latch by name: " + name, e);
        }
    }

    /**
     * Remove internal entry by key from cache.
     *
     * @param key Internal entry key.
     * @param cls Class of object which will be removed. If cached object has different type exception will be thrown.
     * @return Method returns true if sequence has been removed and false if it's not cached.
     * @throws GridException If removing failed or class of object is different to expected class.
     */
    private <R> boolean removeInternal(final GridCacheInternal key, final Class<R> cls) throws GridException {
        return CU.outTx(
            new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    GridCacheTx tx = CU.txStartInternal(cctx, dsView, PESSIMISTIC, REPEATABLE_READ);

                    try {
                        // Check correctness type of removable object.
                        R val = cast(dsView.get(key), cls);

                        if (val != null) {
                            dsView.removex(key);

                            tx.commit();
                        }
                        else
                            tx.setRollbackOnly();

                        return val != null;
                    }
                    catch (Error e) {
                        log.error("Failed to remove data structure: " + key, e);

                        throw e;
                    }
                    catch (Exception e) {
                        log.error("Failed to remove data structure: " + key, e);

                        throw e;
                    }
                    finally {
                        tx.end();
                    }
                }
            },
            cctx
        );
    }

    /** {@inheritDoc} */
    @Override public void onTxCommitted(GridCacheTxEx<K, V> tx) {
        if (!cctx.isDht() && tx.internal()) {
            try {
                waitInitialization();
            }
            catch (GridException e) {
                U.error(log, "Failed to wait for manager initialization.", e);

                return;
            }

            Collection<GridCacheTxEntry<K, V>> entries = tx.writeEntries();

            if (log.isDebugEnabled())
                log.debug("Committed entries: " + entries);

            for (GridCacheTxEntry<K, V> entry : entries) {
                // Check updated or created GridCacheInternalKey keys.
                if ((entry.op() == CREATE || entry.op() == UPDATE) && entry.key() instanceof GridCacheInternalKey) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    if (entry.value() instanceof GridCacheQueueHeader) {
                        // Notify queue on changes.
                        GridCacheRemovable queue = dsMap.get(key);

                        // If queue is used in current cache.
                        if (queue instanceof GridCacheQueueEx) {
                            GridCacheQueueHeader hdr = (GridCacheQueueHeader)entry.value();

                            ((GridCacheQueueEx)queue).onHeaderChanged(hdr);
                        }
                        else if (queue != null)
                            U.error(log, "Failed to cast object [expected=" + GridCacheQueue.class.getSimpleName() +
                                ", actual=" + queue.getClass() + ", value=" + queue + ']');
                    }
                    else if (entry.value() instanceof GridCacheCountDownLatchValue) {
                        // Notify latch on changes.
                        GridCacheRemovable latch = dsMap.get(key);

                        GridCacheCountDownLatchValue val = (GridCacheCountDownLatchValue)entry.value();

                        if (latch instanceof GridCacheCountDownLatchEx) {
                            GridCacheCountDownLatchEx latch0 = (GridCacheCountDownLatchEx)latch;

                            latch0.onUpdate(val.get());

                            if (val.get() == 0 && val.autoDelete()) {
                                entry.cached().markObsolete(cctx.versions().next());

                                dsMap.remove(key);

                                latch.onRemoved();
                            }
                        }
                        else if (latch != null) {
                            U.error(log, "Failed to cast object " +
                                "[expected=" + GridCacheCountDownLatch.class.getSimpleName() +
                                ", actual=" + latch.getClass() + ", value=" + latch + ']');
                        }
                    }
                }

                // Check deleted GridCacheInternal keys.
                if (entry.op() == DELETE && entry.key() instanceof GridCacheInternal) {
                    GridCacheInternal key = (GridCacheInternal)entry.key();

                    // Entry's val is null if entry deleted.
                    GridCacheRemovable obj = dsMap.remove(key);

                    if (obj != null)
                        obj.onRemoved();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheAnnotationHelper<GridCacheQueuePriority> priorityAnnotations() {
        return annHelper;
    }

    /**
     * Gets queue query factory.
     *
     * @return Queue query factory.
     */
    public GridCacheQueueQueryFactory queueQueryFactory() {
        return queueQryFactory;
    }

    /**
     * @throws GridException If thread is interrupted or manager
     *     was not successfully initialized.
     */
    private void waitInitialization() throws GridException {
        if (initLatch.getCount() > 0) {
            try {
                initLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridInterruptedException("Thread has been interrupted.", e);
            }
        }

        if (!initFlag)
            throw new GridException("DataStructures manager was not properly initialized for cache: " +
                cctx.cache().name());
    }

    /**
     * Tries to cast the object to expected type.
     *
     * @param obj Object which will be casted.
     * @param cls Class
     * @param <R> Type of expected result.
     * @return Object has casted to expected type.
     * @throws GridException If {@code obj} has different to {@code cls} type.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <R> R cast(@Nullable Object obj, Class<R> cls) throws GridException {
        if (obj == null)
            return null;

        if (cls.isInstance(obj))
            return (R)obj;
        else
            throw new GridException("Failed to cast object [expected=" + cls + ", actual=" + obj.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override protected void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Data structure manager memory stats [grid=" + cctx.gridName() +
            ", cache=" + cctx.name() + ']');
        X.println(">>>   dsMapSize: " + dsMap.size());
    }
}
