// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Command handler for API requests.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridCacheCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * @param ctx Context.
     */
    public GridCacheCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridRestCommand cmd) {
        switch (cmd) {
            case CACHE_GET:
            case CACHE_GET_ALL:
            case CACHE_PUT:
            case CACHE_ADD:
            case CACHE_PUT_ALL:
            case CACHE_REMOVE:
            case CACHE_REMOVE_ALL:
            case CACHE_REPLACE:
            case CACHE_INCREMENT:
            case CACHE_DECREMENT:
            case CACHE_CAS:
            case CACHE_APPEND:
            case CACHE_PREPEND:
            case CACHE_METRICS:
                return true;

            default:
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(final GridRestRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling cache REST request: " + req);

        String cacheName = value("cacheName", req);
        final Object key = value("key", req);
        Collection<Object> keys = values("k", req);

        final GridCacheRestResponse res = new GridCacheRestResponse();

        if (key == null &&
            req.getCommand() != CACHE_METRICS &&
            req.getCommand() != CACHE_GET_ALL &&
            req.getCommand() != CACHE_PUT_ALL &&
            req.getCommand() != CACHE_REMOVE_ALL) {
            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
            res.setError(missingParameter("key"));

            return new GridFinishedFuture<GridRestResponse>(ctx, res);
        }

        if (F.isEmpty(keys) && (
            req.getCommand() == CACHE_GET_ALL ||
            req.getCommand() == CACHE_PUT_ALL)) {
            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
            res.setError(missingParameter("k1"));

            return new GridFinishedFuture<GridRestResponse>(ctx, res);
        }

        final GridCache<Object, Object> cache = ctx.cache().cache(cacheName);

        if (cache == null) {
            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
            res.setError("Failed to find cache for given cache name (null for default cache): " + cacheName);

            return new GridFinishedFuture<GridRestResponse>(ctx, res);
        }

        // Set affinity node ID.
        UUID nodeId = null;

        if (key != null)
            nodeId = cache.mapKeyToNode(key).id();

        res.setAffinityNodeId(nodeId == null ? null : nodeId.toString());

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<GridRestResponse>(ctx);

        try {
            Object val;
            GridFuture<?> opFut;

            switch (req.getCommand()) {
                case CACHE_GET:
                    opFut = cache.getAsync(key);

                    opFut.listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_GET_ALL:
                    opFut = cache.getAllAsync(keys);

                    opFut.listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_PUT:
                    val = value("val", req);

                    if (val != null) {
                        opFut = cache.putxAsync(key, val);

                        opFut.listenAsync(new CIX1<GridFuture<?>>() {
                            @Override public void applyx(GridFuture<?> f) throws GridException {
                                res.setSuccessStatus((Boolean)f.get() ? GridRestResponse.STATUS_SUCCESS :
                                    GridRestResponse.STATUS_FAILED);

                                res.setResponse(f.get());

                                setExpiration(cache, key, req);

                                fut.onDone(res);
                            }
                        });
                    }
                    else {
                        res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                        res.setResponse(false);
                        res.setError(missingParameter("val"));

                        fut.onDone(res);
                    }

                    break;

                case CACHE_ADD:
                    if (!cache.containsKey(key)) {
                        val = value("val", req);

                        if (val != null) {
                            opFut = cache.putxAsync(key, val);

                            opFut.listenAsync(new CIX1<GridFuture<?>>() {
                                @Override public void applyx(GridFuture<?> f) throws GridException {
                                    res.setSuccessStatus((Boolean)f.get() ? GridRestResponse.STATUS_SUCCESS :
                                        GridRestResponse.STATUS_FAILED);

                                    res.setResponse(f.get());

                                    setExpiration(cache, key, req);

                                    fut.onDone(res);
                                }
                            });
                        }
                        else {
                            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
                            res.setResponse(false);
                            res.setError(missingParameter("val"));

                            fut.onDone(res);
                        }
                    }
                    else {
                        res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
                        res.setResponse(false);

                        fut.onDone(res);
                    }

                    break;

                case CACHE_PUT_ALL:
                    Collection<Object> vals = values("v", req);

                    if (keys.size() == vals.size()) {
                        Map<Object, Object> map = new GridLeanMap<Object, Object>(keys.size());

                        Iterator keysIt = keys.iterator();
                        Iterator valsIt = vals.iterator();

                        while (keysIt.hasNext() && valsIt.hasNext())
                            map.put(keysIt.next(), valsIt.next());

                        assert !keysIt.hasNext() && !valsIt.hasNext();

                        opFut = cache.putAllAsync(map);

                        opFut.listenAsync(new CI1<GridFuture<?>>() {
                            @Override public void apply(GridFuture<?> f) {
                                res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                                res.setResponse(true);

                                fut.onDone(res);
                            }
                        });
                    }
                    else {
                        res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                        res.setResponse(true);
                        res.setError("Number of keys and values must be equal.");

                        fut.onDone(res);
                    }

                    break;

                case CACHE_REMOVE:
                    opFut = cache.removexAsync(key);

                    opFut.listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setSuccessStatus((Boolean)f.get() ? GridRestResponse.STATUS_SUCCESS :
                                GridRestResponse.STATUS_FAILED);

                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_REMOVE_ALL:
                    opFut = !F.isEmpty(keys) ? cache.removeAllAsync(keys) : cache.removeAllAsync();

                    opFut.listenAsync(new CI1<GridFuture<?>>() {
                        @Override public void apply(GridFuture<?> f) {
                            res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                            res.setResponse(true);

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_REPLACE:
                    val = value("val", req);

                    if (val == null) {
                        res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
                        res.setError(missingParameter("val"));

                        fut.onDone(res);
                    }
                    else {
                        opFut = cache.replacexAsync(key, val);

                        opFut.listenAsync(new CIX1<GridFuture<?>>() {
                            @Override public void applyx(GridFuture<?> f) throws GridException {
                                setExpiration(cache, key, req);

                                res.setSuccessStatus((Boolean)f.get() ? GridRestResponse.STATUS_SUCCESS :
                                    GridRestResponse.STATUS_FAILED);

                                res.setResponse(f.get());

                                fut.onDone(res);
                            }
                        });
                    }

                    break;

                case CACHE_INCREMENT:
                    incrementOrDecrement(cache, (String)key, req, res, fut, false);

                    break;

                case CACHE_DECREMENT:
                    incrementOrDecrement(cache, (String)key, req, res, fut, true);

                    break;

                case CACHE_CAS:
                    Object val1 = value("val1", req);
                    Object val2 = value("val2", req);

                    opFut = val2 == null && val1 == null ? cache.removexAsync(key) :
                        val2 == null ? cache.putxIfAbsentAsync(key, val1) :
                            val1 == null ? cache.removeAsync(key, val2) :
                                cache.replaceAsync(key, val2, val1);

                    opFut.listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setSuccessStatus((Boolean)f.get() ? GridRestResponse.STATUS_SUCCESS :
                                GridRestResponse.STATUS_FAILED);

                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_APPEND:
                    appendOrPrepend(cache, key, req, res, fut, false);

                    break;

                case CACHE_PREPEND:
                    appendOrPrepend(cache, key, req, res, fut, true);

                    break;

                case CACHE_METRICS:
                    GridCacheMetrics metrics;

                    if (key != null) {
                        GridCacheEntry<Object, Object> entry = cache.entry(key);

                        assert entry != null;

                        metrics = entry.metrics();
                    }
                    else
                        // Cache metrics were requested.
                        metrics = cache.metrics();

                    assert metrics != null;

                    res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                    res.setResponse(new GridCacheRestMetrics(metrics.createTime(), metrics.readTime(),
                        metrics.writeTime(), metrics.reads(), metrics.writes(), metrics.hits(), metrics.misses()));

                    fut.onDone(res);

                    break;

                default:
                    assert false : "Invalid command for cache handler: " + req;
            }
        }
        catch (GridException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
            res.setError(e.getMessage());

            return new GridFinishedFuture<GridRestResponse>(ctx, res);
        }
        finally {
            if (log.isDebugEnabled())
                log.debug("Handled cache REST request [res=" + res + ", req=" + req + ']');
        }

        return fut;
    }

    /**
     * Sets cache entry expiration time.
     *
     * @param cache Cache.
     * @param key Key.
     * @param req Request.
     */
    private void setExpiration(GridCacheProjection<Object,Object> cache,
        Object key, GridRestRequest req) {
        assert cache != null;
        assert key != null;

        if (!cache.containsKey(key))
            return;

        Object exp = value("exp", req);

        if (exp == null)
            return;

        GridCacheEntry<Object, Object> entry = cache.entry(key);

        entry.timeToLive(exp instanceof String ? Long.valueOf((String)exp) : (Long)exp);
    }

    /**
     * Handles increment and decrement commands.
     *
     * @param cache Cache.
     * @param key Key.
     * @param req Request.
     * @param res Response.
     * @param fut Future.
     * @param decr Whether to decrement (increment otherwise).
     * @throws GridException In case of error.
     */
    private void incrementOrDecrement(GridCache<Object, Object> cache, String key,
        GridRestRequest req, final GridRestResponse res,
        final GridFutureAdapter<GridRestResponse> fut,
        boolean decr) throws GridException {
        assert cache != null;
        assert key != null;
        assert req != null;
        assert res != null;

        Object initObj = value("init", req);
        Object deltaObj = value("delta", req);

        String error = null;

        Long init = null;

        if (initObj != null) {
            if (initObj instanceof String) {
                try {
                    init = Long.valueOf((String)initObj);
                }
                catch (NumberFormatException ignored) {
                    error = invalidNumericParameter("init");
                }
            }
            else if (initObj instanceof Long)
                init = (Long)initObj;
        }

        Long delta = null;

        if (deltaObj != null) {
            if (deltaObj instanceof String) {
                try {
                    delta = Long.valueOf((String)deltaObj);
                }
                catch (NumberFormatException ignored) {
                    error = invalidNumericParameter("delta");
                }
            }
            else if (deltaObj instanceof Long)
                delta = (Long)deltaObj;
        }
        else
            error = missingParameter("delta");

        if (error == null) {
            GridCacheAtomicLong l = init != null ? cache.atomicLong(key, init, false) : cache.atomicLong(key);

            GridFuture<Long> opFut = l.addAndGetAsync(decr ? -delta : delta);

            opFut.listenAsync(new CIX1<GridFuture<?>>() {
                @Override public void applyx(GridFuture<?> f) throws GridException {
                    res.setSuccessStatus(GridRestResponse.STATUS_SUCCESS);
                    res.setResponse(f.get());

                    fut.onDone(res);
                }
            });
        }
        else {
            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
            res.setError(error);

            fut.onDone(res);
        }
    }

    /**
     * Handles append and prepend commands.
     *
     * @param cache Cache.
     * @param key Key.
     * @param req Request.
     * @param res Response.
     * @param fut Future.
     * @param prepend Whether to prepend.
     */
    private void appendOrPrepend(
        final GridCacheProjection<Object,Object> cache,
        final Object key,
        GridRestRequest req, final GridRestResponse res,
        final GridFutureAdapter<GridRestResponse> fut,
        final boolean prepend) {
        assert cache != null;
        assert key != null;
        assert req != null;
        assert res != null;

        final Object val = value("val", req);

        if (val != null) {
            GridFuture<Object> getFut = cache.getAsync(key);

            getFut.listenAsync(new CIX1<GridFuture<Object>>() {
                @Override public void applyx(GridFuture<Object> f) throws GridException {
                    Object currVal = f.get();

                    if (currVal != null) {
                        if (val instanceof String && currVal instanceof String) {
                            String newVal = prepend ? (String)val + currVal : currVal + (String)val;

                            GridFuture<Boolean> putFut = cache.putxAsync(key, newVal);

                            putFut.listenAsync(new CIX1<GridFuture<Boolean>>() {
                                @Override public void applyx(GridFuture<Boolean> f) throws GridException {
                                    res.setSuccessStatus((Boolean)f.get() ? GridRestResponse.STATUS_SUCCESS :
                                        GridRestResponse.STATUS_FAILED);

                                    res.setResponse(f.get());

                                    fut.onDone(res);
                                }
                            });
                        }
                        else {
                            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
                            res.setResponse(false);
                            res.setError("Incompatible types.");

                            fut.onDone(res);
                        }
                    }
                    else {
                        res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
                        res.setResponse(false);

                        fut.onDone(res);
                    }
                }
            });
        }
        else {
            res.setSuccessStatus(GridRestResponse.STATUS_FAILED);
            res.setResponse(false);
            res.setError(missingParameter("val"));

            fut.onDone(res);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCommandHandler.class, this);
    }
}
