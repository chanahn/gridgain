// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.GridJobResultPolicy.*;

/**
 * Affinity utility methods.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
class GridAffinityUtils {
    /**
     * Creates a task that will look up {@link GridCacheAffinityMapper} and {@link GridCacheAffinity} on a cache with
     * given name. If they exist, this job will serialize and transfer them together with all deployment information
     * needed to unmarshall objects on remote node. Result is returned as a {@link GridTuple3}, where first object is
     * {@link GridAffinityMessage} for {@link GridCacheAffinity}, second object is {@link GridAffinityMessage} for
     * {@link GridCacheAffinityMapper} and third object is optional {@link GridException} representing deployment
     * exception. If exception field is not null, first two objects must be discarded. If cache with name {@code
     * cacheName} does not exist on a node, the job will return {@code null}.
     *
     * @param cacheName Cache name.
     * @return Affinity job.
     */
    static GridTask<GridNode, GridTuple3<GridAffinityMessage, GridAffinityMessage, GridException>> affinityTask(
        final String cacheName) {
        return new GridTaskAdapter<GridNode, GridTuple3<GridAffinityMessage, GridAffinityMessage, GridException>>() {
            @Override public Map<? extends GridJob, GridNode> map(List<GridNode> subgrid, GridNode node) {
                assert node != null;

                return F.asMap(new GridJobAdapterEx() {
                    @GridInstanceResource
                    private Grid grid;

                    @GridLoggerResource
                    private GridLogger log;

                    @Override public Object execute() {
                        assert grid != null;
                        assert log != null;

                        GridCache cache = grid.cache(cacheName);

                        GridKernalContext ctx = ((GridKernal)grid).context();

                        GridTuple3<GridAffinityMessage, GridAffinityMessage, GridException> res = null;

                        if (cache != null)
                            try {
                                res = new GridTuple3<GridAffinityMessage, GridAffinityMessage, GridException>();

                                res.set1(affinityMessage(ctx, cache.configuration().getAffinityMapper()));
                                res.set2(affinityMessage(ctx, cache.configuration().getAffinity()));
                            }
                            catch (GridException e) {
                                res.set3(e);

                                U.error(log, "Failed to transfer affinity:", e);
                            }

                        return res;
                    }
                }, node);
            }

            @Override public GridJobResultPolicy result(GridJobResult res, List<GridJobResult> rcvd) {
                return WAIT;
            }

            @Override public GridTuple3<GridAffinityMessage, GridAffinityMessage, GridException> reduce(
                List<GridJobResult> results) {
                assert results.size() == 1;

                return F.first(results).getData();
            }
        };
    }

    /**
     * @param ctx  {@code GridKernalContext} instance which provides deployment manager
     * @param o Object for which deployment should be obtained.
     * @return Deployment object for given instance,
     * @throws GridException If node cannot create deployment for given object.
     */
    private static GridAffinityMessage affinityMessage(GridKernalContext ctx, Object o) throws GridException {
        Class cls = o.getClass();

        GridDeployment dep = ctx.deploy().deploy(cls, cls.getClassLoader());

        if (dep == null)
            throw new GridDeploymentException("Failed to deploy affinity object with class: " + cls.getName());

        return new GridAffinityMessage(
            U.marshal(ctx.config().getMarshaller(), o),
            cls.getName(),
            dep.classLoaderId(),
            dep.deployMode(),
            dep.sequenceNumber(),
            dep.userVersion(),
            dep.participants());
    }

    /**
     * Unmarshalls transfer object from remote node within a given context.
     *
     * @param ctx Grid kernal context that provides deployment and marshalling services.
     * @param sndNodeId {@link UUID} of the sender node.
     * @param msg Transfer object that contains original serialized object and deployment information.
     * @return Unmarshalled object.
     * @throws GridException If node cannot obtain deployment.
     */
    static Object unmarshall(GridKernalContext ctx, UUID sndNodeId, GridAffinityMessage msg)
        throws GridException {
        GridDeployment dep = ctx.deploy().getGlobalDeployment(
            msg.deploymentMode(),
            msg.sourceClassName(),
            msg.sourceClassName(),
            msg.sequenceNumber(),
            msg.userVersion(),
            sndNodeId,
            msg.classLoaderId(),
            msg.loaderParticipants(),
            null);

        if (dep == null)
            throw new GridDeploymentException("Failed to obtain affinity object (is peer class loading turned on?): " +
                msg);

        Object src = U.unmarshal(ctx.config().getMarshaller(), msg.source(), dep.classLoader());

        // Resource injection.
        ctx.resource().inject(dep, dep.deployedClass(msg.sourceClassName()), src);

        return src;
    }

    /** Ensure singleton. */
    private GridAffinityUtils() {
        // No-op.
    }
}
