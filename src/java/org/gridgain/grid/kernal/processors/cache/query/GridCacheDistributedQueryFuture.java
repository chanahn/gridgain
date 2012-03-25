// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Distributed query future.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridCacheDistributedQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    protected volatile long requestId;

    /** */
    protected final Collection<UUID> subgrid = new HashSet<UUID>();

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheDistributedQueryFuture() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     * @param qry Query.
     * @param nodes Nodes.
     * @param loc Local query or not.
     * @param single Single result or not.
     * @param rmtRdcOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     */
    @SuppressWarnings({"unchecked"})
    protected GridCacheDistributedQueryFuture(GridCacheContext<K, V> ctx, GridCacheQueryBaseAdapter<K, V> qry,
        Iterable<GridRichNode> nodes, boolean loc, boolean single, boolean rmtRdcOnly,
        @Nullable GridInClosure2<UUID, Collection<R>> pageLsnr) {
        super(ctx, qry, loc, single, rmtRdcOnly, pageLsnr);

        GridCacheQueryManager<K, V> mgr = ctx.queries();

        assert mgr != null;

        if (loc)
            locFut = ctx.closures().runLocalSafe(new LocalQueryRunnable<K, V, R>(mgr, this, single), true);
        else {
            synchronized (mux) {
                for (GridRichNode node : nodes)
                    subgrid.add(node.id());
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery() throws GridException {
        if (loc)
            locFut.cancel();
        else {
            try {
                Collection<GridNode> nodes = F.retain(
                    cctx.discovery().allNodes(),
                    true,
                    new P1<GridNode>() {
                        @Override public boolean apply(GridNode node) {
                            synchronized (mux) {
                                return subgrid.contains(node.id());
                            }
                        }
                    });

                if (!nodes.isEmpty())
                    cctx.io().safeSend(nodes, new GridCacheQueryRequest<K, V>(requestId), new P1<GridNode>() {
                        @Override public boolean apply(GridNode node) {
                            onNodeLeft(node.id());

                            return !isDone();
                        }
                    });
            }
            catch (GridException e) {
                U.error(log, "Can not send cancel request (will cancel query in any case).", e);
            }

            GridCacheQueryManager<K, V> qryMgr = cctx.queries();

            assert qryMgr != null;

            qryMgr.onQueryFutureCanceled(requestId);

            clear();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onNodeLeft(UUID nodeId) {
        if (!loc)
            // We consider node departure as a reception of last empty
            // page from this node.
            onPage(nodeId, Collections.emptyList(), null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onLastPage(UUID nodeId) {
        assert Thread.holdsLock(mux);

        boolean futFinish;

        if (loc)
            futFinish = true;
        else {
            subgrid.remove(nodeId);

            futFinish = subgrid.isEmpty();
        }

        return futFinish;
    }

    /** {@inheritDoc} */
    @Override void clear() {
        GridCacheDistributedQueryManager<K, V> qryMgr = (GridCacheDistributedQueryManager<K, V>)cctx.queries();

        assert qryMgr != null;

        qryMgr.removeQueryFuture(requestId);
    }

    /**
     * @param requestId Request id.
     */
    void requestId(long requestId) {
        this.requestId = requestId;
    }
}
