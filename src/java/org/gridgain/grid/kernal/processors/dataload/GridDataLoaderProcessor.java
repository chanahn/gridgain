// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridDataLoaderProcessor extends GridProcessorAdapter {
    /** Loaders map (access is not supposed to be highly concurrent). */
    private Collection<GridDataLoader> ldrs = new GridConcurrentHashSet<GridDataLoader>(16, 0.75f, 1);

    /** Busy lock. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /**
     * @param ctx Kernal context.
     */
    public GridDataLoaderProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (log.isDebugEnabled())
            log.debug("Started data loader processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel, boolean wait) {
        busyLock.block();

        for (GridDataLoader<?, ?> ldr : ldrs) {
            if (log.isDebugEnabled())
                log.debug("Closing active data loader on grid stop [ldr=" + ldr + ", cancel=" + cancel + ']');

            try {
                ldr.close(cancel);
            }
            catch (GridInterruptedException e) {
                U.warn(log, "Interrupted while waiting for completion of the data loader: " + ldr, e);
            }
            catch (GridException e) {
                U.error(log, "Failed to close data loader: " + ldr, e);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Stopped data loader processor.");
    }

    /**
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Data loader.
     */
    public <K, V> GridDataLoader<K, V> dataLoader(@Nullable String cacheName) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to create data loader (grid is stopping).");

        try {
            final GridDataLoader<K, V> ldr = new GridDataLoaderImpl<K, V>(ctx, cacheName);

            ldrs.add(ldr);

            ldr.future().listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> f) {
                    boolean b = ldrs.remove(ldr);

                    assert b : "Loader has not been added to set: " + ldr;

                    if (log.isDebugEnabled())
                        log.debug("Loader has been completed: " + ldr);
                }
            });

            return ldr;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> Data loader processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   ldrsSize: " + ldrs.size());
    }
}
