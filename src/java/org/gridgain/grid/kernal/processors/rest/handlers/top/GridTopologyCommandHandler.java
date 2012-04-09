// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.top;

import org.gridgain.client.message.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Command handler for API requests.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public class GridTopologyCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * @param ctx Context.
     */
    public GridTopologyCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridRestCommand cmd) {
        switch (cmd) {
            case TOPOLOGY:
            case NODE:
                return true;

            default:
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling topology REST request: " + req);

        GridRestResponse res = new GridRestResponse();

        Object mtrVal = value("mtr", req);
        Object attrVal = value("attr", req);

        boolean mtr = mtrVal != null && (mtrVal instanceof String ?
            Boolean.parseBoolean((String)mtrVal) : (Boolean)mtrVal);
        boolean attr = attrVal != null && (attrVal instanceof String ?
            Boolean.parseBoolean((String)attrVal) : (Boolean)attrVal);

        switch (req.getCommand()) {
            case TOPOLOGY:
                Collection<GridClientNodeBean> top =
                    new ArrayList<GridClientNodeBean>(ctx.discovery().allNodes().size());

                for (GridNode node : ctx.discovery().allNodes())
                    top.add(createNodeBean(node, mtr, attr));

                res.setResponse(top);

                break;

            case NODE:
                String idParam = value("id", req);

                try {
                    UUID id = idParam != null ? UUID.fromString(idParam) : null;

                    final String ip = value("ip", req);

                    if (id == null && ip == null)
                        return new GridFinishedFuture<GridRestResponse>(ctx, new GridException(
                            "Failed to handle request (either id or ip should be specified)."));

                    GridNode node;

                    if (id != null) {
                        // Always refresh topology so client see most up-to-date view.
                        ctx.discovery().alive(id);

                        node = ctx.discovery().node(id);

                        if (node != null && ip != null && !node.externalAddresses().contains(ip) &&
                            !node.internalAddresses().contains(ip))
                            node = null;
                    }
                    else
                        node = F.find(ctx.discovery().allNodes(), null, new P1<GridNode> () {
                            @Override public boolean apply(GridNode n) {
                                return n.internalAddresses().contains(ip) || n.externalAddresses().contains(ip);
                            }
                        });

                    if (node != null)
                        res.setResponse(createNodeBean(node, mtr, attr));
                    else
                        res.setResponse(null);
                }
                catch (IllegalArgumentException e) {
                    String msg = "Failed to parse id parameter [id=" + idParam + ", err=" + e.getMessage() + ']';

                    if (log.isDebugEnabled())
                        log.debug(msg);

                    return new GridFinishedFuture<GridRestResponse>(ctx, new GridException(msg));
                }

                break;

            default:
                assert false : "Invalid command for topology handler: " + req;
        }

        if (log.isDebugEnabled())
            log.debug("Handled topology REST request [res=" + res + ", req=" + req + ']');

        return new GridFinishedFuture<GridRestResponse>(ctx, res);
    }

    /**
     * Creates node bean out of grid node. Notice that cache attribute is handled separately.
     *
     * @param node Grid node.
     * @param mtr {@code true} to add metrics.
     * @param attr {@code true} to add attributes.
     * @return Grid Node bean.
     */
    private GridClientNodeBean createNodeBean(GridNode node, boolean mtr, boolean attr) {
        assert node != null;

        GridClientNodeBean nodeBean = new GridClientNodeBean();

        nodeBean.setNodeId(node.id().toString());
        nodeBean.setInternalAddresses(node.internalAddresses());
        nodeBean.setExternalAddresses(node.externalAddresses());
        nodeBean.setTcpPort(node.<Integer>attribute(ATTR_REST_TCP_PORT));

        int jettyPort = node.<Integer>attribute(ATTR_REST_JETTY_PORT);

        if (jettyPort == 0) {
            Integer port = Integer.getInteger(GG_JETTY_PORT);

            if (port != null)
                jettyPort = port;
        }

        nodeBean.setJettyPort(jettyPort);

        GridCacheAttributes[] caches = node.attribute(ATTR_CACHE);

        if (caches != null && caches.length > 0) {
            Map<String, String> cacheMap = new HashMap<String, String>(caches.length);

            for (GridCacheAttributes cacheAttr : caches) {
                if (cacheAttr.cacheName() != null)
                    cacheMap.put(cacheAttr.cacheName(), cacheAttr.cacheMode().toString());
                else
                    nodeBean.setDefaultCacheMode(cacheAttr.cacheMode().toString());
            }

            nodeBean.setCaches(cacheMap);
        }

        if (mtr) {
            GridNodeMetrics metrics = node.metrics();

            GridClientNodeMetricsBean metricsBean = new GridClientNodeMetricsBean();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setFileSystemFreeSpace(metrics.getFileSystemFreeSpace());
            metricsBean.setFileSystemTotalSpace(metrics.getFileSystemTotalSpace());
            metricsBean.setFileSystemUsableSpace(metrics.getFileSystemUsableSpace());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setUpTime(metrics.getUpTime());

            nodeBean.setMetrics(metricsBean);
        }

        if (attr) {
            Map<String, Object> attrs = new HashMap<String, Object>(node.attributes());

            attrs.remove(ATTR_CACHE);

            nodeBean.setAttributes(attrs);
        }

        return nodeBean;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTopologyCommandHandler.class, this);
    }
}
