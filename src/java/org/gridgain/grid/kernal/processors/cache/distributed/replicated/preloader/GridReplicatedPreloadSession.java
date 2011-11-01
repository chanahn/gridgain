// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Session.
 */
class GridReplicatedPreloadSession {
    /** */
    private final Collection<GridReplicatedPreloadAssignment> assigns =
        new LinkedList<GridReplicatedPreloadAssignment>();

    /** */
    private final GridAtomicLong maxOrder;

    /** */
    private final AtomicInteger leftAssigns = new AtomicInteger();

    /** */
    private final AtomicBoolean cancel = new AtomicBoolean();

    /** */
    private final GridFutureAdapter<?> finishFut;

    /**
     * @param maxOrder Max order of the node to demand from.
     * @param finishFut Future for session.
     */
    GridReplicatedPreloadSession(long maxOrder, GridFutureAdapter<?> finishFut) {
        this.maxOrder = new GridAtomicLong(maxOrder);
        this.finishFut = finishFut;
    }

    /**
     * @param assign Assignment to add.
     */
    void addAssignment(GridReplicatedPreloadAssignment assign) {
        assert assign != null;

        assigns.add(assign);

        leftAssigns.incrementAndGet();
    }

    /**
     * @return {@code True} if that was the last assignment.
     */
    boolean onAssignmentProcessed() {
        if (leftAssigns.decrementAndGet() == 0) {
            boolean res = finishFut.onDone();

            assert res;

            return true;
        }

        return false;
    }

    /**
     * @return Assignments for current session.
     */
    Collection<GridReplicatedPreloadAssignment> assigns() {
        return assigns;
    }

    /**
     * @return Max order of the node to demand from.
     */
    long maxOrder() {
        return maxOrder.get();
    }

    /**
     * @param maxOrder Max order to demand from.
     */
    void maxOrder(long maxOrder) {
        this.maxOrder.setIfLess(maxOrder);
    }

    /**
     * @return Future for session.
     */
    GridFutureAdapter<?> finishFuture() {
        return finishFut;
    }

    /**
     * @return {@code True} if current call cancels session.
     */
    boolean cancel() {
        return cancel.compareAndSet(false, true);
    }

    /**
     * @return {@code True} if session was cancelled.
     */
    boolean cancelled() {
        return cancel.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedPreloadSession.class, this);
    }
}
