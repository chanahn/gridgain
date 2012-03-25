// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.segmentation;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;

import java.util.*;

/**
 * Kernal processor responsible for checking network segmentation issues.
 * <p>
 * Segment checks are performed by segmentation resolvers
 * Each segmentation resolver checks segment for validity, using its inner logic.
 * Typically, resolver should run light-weight single check (i.e. one IP address or
 * one shared folder). Compound segment checks may be performed using several
 * resolvers.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 * @see GridConfiguration#getSegmentationResolvers()
 * @see GridConfiguration#getSegmentationPolicy()
 * @see GridConfiguration#getSegmentCheckFrequency()
 * @see GridConfiguration#isAllSegmentationResolversPassRequired()
 * @see GridConfiguration#isWaitForSegmentOnStart()
 */
public class GridSegmentationProcessor extends GridProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public GridSegmentationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Performs network segment check.
     * <p>
     * This method is called by discovery manager in the following cases:
     * <ol>
     *     <li>Before discovery SPI start.</li>
     *     <li>When other node leaves topology.</li>
     *     <li>When other node in topology fails.</li>
     *     <li>Periodically (see {@link GridConfiguration#getSegmentCheckFrequency()}).</li>
     * </ol>
     *
     * @return {@code True} if segment is correct.
     */
    public boolean isValidSegment() {
        GridSegmentationResolver[] resolvers = ctx.config().getSegmentationResolvers();

        if (resolvers == null || resolvers.length == 0) {
            if (log.isDebugEnabled())
                log.debug("Segmentation check is disabled (configured array of resolvers is empty).");

            return true;
        }

        if (log.isDebugEnabled())
            log.debug("Starting network segment check.");

        long start = System.currentTimeMillis();

        boolean allSegResolversPassReq = ctx.config().isAllSegmentationResolversPassRequired();

        // Init variable in this way, since further logic depends on this value.
        boolean segValid = allSegResolversPassReq;

        Collection<GridTuple2<GridSegmentationResolver, GridException>> errs =
                new LinkedList<GridTuple2<GridSegmentationResolver, GridException>>();

        for (GridSegmentationResolver resolver : resolvers) {
            boolean valid = false;

            GridException err = null;

            for (int i = 0; i < ctx.config().getSegmentationResolveAttempts() && !valid; i++) {
                try {
                    valid = resolver.isValidSegment();

                    if (log.isDebugEnabled())
                        log.debug("Checked segmentation resolver [resolver=" + resolver + ", valid=" + valid + ']');
                }
                catch (GridException e) {
                    if (err == null)
                        err = e;

                    if (log.isDebugEnabled())
                        log.debug("Failed to check segmentation resolver [resolver=" + resolver +
                                ", err=" + e.getMessage() + ']');
                }
            }

            // Store error.
            if (!valid && err != null)
                errs.add(F.t(resolver, err));

            if (valid && !allSegResolversPassReq) {
                // Segment is valid.
                segValid = true;

                break;
            }

            if (!valid && allSegResolversPassReq) {
                // Segment is not valid.
                segValid = false;

                break;
            }
        }

        if (log.isDebugEnabled())
            log.debug("Network segment check finished in " + (System.currentTimeMillis() - start) + " ms.");

        if (segValid)
            return true;

        if (!errs.isEmpty()) {
            // Output errors via log throttle.
            for (GridTuple2<GridSegmentationResolver, GridException> t : errs)
                LT.error(log, t.get2(), "Failed to check segmentation resolver: " + t.get1());
        }

        return false;
    }
}
