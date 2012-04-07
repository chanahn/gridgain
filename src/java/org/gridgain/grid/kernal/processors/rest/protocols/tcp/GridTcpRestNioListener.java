// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.gridgain.client.message.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.cache.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Listener for nio server that handles incoming tcp rest packets.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridTcpRestNioListener implements GridNioServerListener<GridClientMessage> {
    /** Logger. */
    private GridLogger log;

    /** Protocol handler. */
    private GridRestProtocolHandler hnd;

    /** Handler for all memcache requests */
    private GridTcpMemcacheNioListener memcacheLsnr;

    /**
     * Creates listener which will convert incoming tcp packets to rest requests and forward them to
     * a given rest handler.
     *
     * @param log Logger to use.
     * @param hnd Rest handler.
     */
    public GridTcpRestNioListener(GridLogger log, GridRestProtocolHandler hnd) {
        memcacheLsnr = new GridTcpMemcacheNioListener(log, hnd);

        this.log = log;
        this.hnd = hnd;
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (e != null) {
            if (e instanceof RuntimeException)
                U.error(log, "Failed to process request from remote client:" + ses, e);
            else
                U.warn(log, "Closed client session due to exception: [ses=" + ses + ", msg=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void onMessage(final GridNioSession ses, final GridClientMessage msg) {
        if (msg instanceof GridTcpRestPacket)
            memcacheLsnr.onMessage(ses, (GridTcpRestPacket)msg);
        else {
            if (msg == GridTcpRestParser.PING_MESSAGE) {
                ses.send(GridTcpRestParser.PING_MESSAGE);

                return;
            }

            final GridRestRequest req = createRestRequest(msg);

            if (req != null)
                hnd.handleAsync(req).listenAsync(new CIX1<GridFuture<GridRestResponse>>() {
                    @Override
                    public void applyx(GridFuture<GridRestResponse> fut) throws GridException {
                        GridRestResponse restRes = fut.get();

                        GridClientResultBean res = new GridClientResultBean();

                        res.requestId(msg.requestId());
                        res.clientId(msg.clientId());

                        res.sessionToken(restRes.sessionTokenBytes());

                        res.successStatus(restRes.getSuccessStatus());
                        res.errorMessage(restRes.getError());

                        Object resp = restRes.getResponse();

                        // In case of metrics a little adjustment is needed.
                        if (resp instanceof GridCacheRestMetrics)
                            resp = ((GridCacheRestMetrics)resp).map();

                        res.result(resp);

                        ses.send(res);
                    }
                });
            else
                U.warn(log, "Failed to process client request (unknown packet type): [ses=" + ses + ", msg=" + msg +
                    ']');
        }
    }

    /**
     * Creates a REST request object from client TCP binary packet.
     *
     * @param msg Request message.
     * @return REST request object.
     */
    private GridRestRequest createRestRequest(GridClientMessage msg) {
        GridRestRequest res = null;

        if (msg instanceof GridClientAuthenticationRequest) {
            GridClientAuthenticationRequest req = (GridClientAuthenticationRequest)msg;

            res = new GridRestRequest();

            res.setCommand(NOOP);

            res.setCredentials(req.credentials());
        }
        else if (msg instanceof GridClientCacheRequest) {
            GridClientCacheRequest req = (GridClientCacheRequest)msg;

            Map<?, ?> vals = req.values();

            res = new GridRestRequest();

            Map<String, Object> params = new GridLeanMap<String, Object>(4);

            params.put("cacheName", req.cacheName());

            int i = 1;

            switch (req.operation()) {
                case PUT:
                    res.setCommand(CACHE_PUT);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
                case PUT_ALL:
                    res.setCommand(CACHE_PUT_ALL);

                    for (Map.Entry entry : vals.entrySet()) {
                        params.put("k" + i, entry.getKey());
                        params.put("v" + i, entry.getValue());

                        i++;
                    }

                    break;
                case GET:
                    res.setCommand(CACHE_GET);

                    params.put("key", req.key());

                    break;
                case GET_ALL:
                    res.setCommand(CACHE_GET_ALL);

                    for (Map.Entry entry : vals.entrySet()) {
                        params.put("k" + i, entry.getKey());

                        i++;
                    }

                    break;
                case RMV:
                    res.setCommand(CACHE_REMOVE);

                    params.put("key", req.key());

                    break;
                case RMV_ALL:
                    res.setCommand(CACHE_REMOVE_ALL);

                    for (Map.Entry entry : vals.entrySet()) {
                        params.put("k" + i, entry.getKey());

                        i++;
                    }

                    break;
                case ADD:
                    res.setCommand(CACHE_ADD);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
                case REPLACE:
                    res.setCommand(CACHE_REPLACE);

                    params.put("key", req.key());
                    params.put("val", req.value());

                    break;
                case CAS:
                    res.setCommand(CACHE_CAS);

                    params.put("key", req.key());
                    params.put("val1", req.value());
                    params.put("val2", req.value2());

                    break;
                case METRICS:
                    res.setCommand(CACHE_METRICS);

                    params.put("key", req.key());

                    break;
            }

            res.setParameters(params);
        }
        else if (msg instanceof GridClientTaskRequest) {
            GridClientTaskRequest req = (GridClientTaskRequest)msg;

            res = new GridRestRequest();

            res.setCommand(EXE);

            Map<String, Object> params = new GridLeanMap<String, Object>(1);

            params.put("name", req.taskName());

            params.put("p1", req.argument());

            res.setParameters(params);
        }
        else if (msg instanceof GridClientTopologyRequest) {
            GridClientTopologyRequest req = (GridClientTopologyRequest)msg;

            res = new GridRestRequest();

            res.setCommand(TOPOLOGY);

            Map<String, Object> params = new GridLeanMap<String, Object>(2);

            params.put("mtr", req.includeMetrics());
            params.put("attr", req.includeAttributes());

            if (req.nodeId() != null) {
                res.setCommand(NODE);

                params.put("id", req.nodeId());
            }
            else if (req.nodeIp() != null) {
                res.setCommand(NODE);

                params.put("ip", req.nodeIp());
            }
            else
                res.setCommand(TOPOLOGY);

            res.setParameters(params);
        }
        else if (msg instanceof GridClientLogRequest) {
            GridClientLogRequest req = (GridClientLogRequest)msg;

            res = new GridRestRequest();

            res.setCommand(LOG);

            Map<String, Object> params = new GridLeanMap<String, Object>(3);

            params.put("path", req.path());
            params.put("from", req.from());
            params.put("to", req.to());

            res.setParameters(params);
        }

        if (res != null) {
            res.setClientId(msg.clientId());
            res.setSessionToken(msg.sessionToken());
        }

        return res;
    }
}
