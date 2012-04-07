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
 * @version 4.0.1c.07042012
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
        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<GridRestResponse>(ctx);

        try {
            GridRestCommand cmd = req.getCommand();

            if (key == null && (cmd == CACHE_GET || cmd == CACHE_PUT || cmd == CACHE_ADD || cmd == CACHE_REMOVE ||
                cmd == CACHE_REPLACE || cmd == CACHE_INCREMENT || cmd == CACHE_DECREMENT || cmd == CACHE_CAS ||
                cmd == CACHE_APPEND || cmd == CACHE_PREPEND))
                throw new GridException(missingParameter("key"));

            if (F.isEmpty(keys) && (cmd == CACHE_GET_ALL || cmd == CACHE_PUT_ALL))
                throw new GridException(missingParameter("k1"));

            final GridCache<Object, Object> cache = ctx.cache().cache(cacheName);

            if (cache == null)
                throw new GridException(
                    "Failed to find cache for given cache name (null for default cache): " + cacheName);

            // Set affinity node ID.
            UUID nodeId = null;

            if (key != null)
                nodeId = cache.mapKeyToNode(key).id();

            res.setAffinityNodeId(nodeId == null ? null : nodeId.toString());

            GridFuture<?> opFut;

            Object val;

            switch (cmd) {
                case CACHE_GET:
                    cache.getAsync(key).listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_GET_ALL:
                    cache.getAllAsync(keys).listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_PUT:
                    val = value("val", req);

                    if (val == null)
                        throw new GridException(missingParameter("val"));

                    cache.putxAsync(key, val).listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            setExpiration(cache, key, req);

                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_ADD:
                    val = value("val", req);

                    if (val == null)
                        throw new GridException(missingParameter("val"));

                    if (cache.containsKey(key)) {
                        res.setResponse(false);

                        fut.onDone(res);

                        break;
                    }

                    cache.putxAsync(key, val).listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            setExpiration(cache, key, req);

                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_PUT_ALL:
                    Collection<Object> vals = values("v", req);

                    if (vals == null)
                        throw new GridException(missingParameter("v"));

                    if (keys.size() != vals.size())
                        throw new GridException("Number of keys and values must be equal.");

                    Map<Object, Object> map = new GridLeanMap<Object, Object>(keys.size());

                    Iterator keysIt = keys.iterator();
                    Iterator valsIt = vals.iterator();

                    while (keysIt.hasNext() && valsIt.hasNext())
                        map.put(keysIt.next(), valsIt.next());

                    assert !keysIt.hasNext() && !valsIt.hasNext();

                    cache.putAllAsync(map).listenAsync(new CI1<GridFuture<?>>() {
                        @Override public void apply(GridFuture<?> f) {
                            res.setResponse(true);

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_REMOVE:
                    cache.removexAsync(key).listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_REMOVE_ALL:
                    opFut = F.isEmpty(keys) ? cache.removeAllAsync() : cache.removeAllAsync(keys);

                    opFut.listenAsync(new CI1<GridFuture<?>>() {
                        @Override public void apply(GridFuture<?> f) {
                            res.setResponse(true);

                            fut.onDone(res);
                        }
                    });

                    break;

                case CACHE_REPLACE:
                    val = value("val", req);

                    if (val == null)
                        throw new GridException(missingParameter("val"));

                    cache.replacexAsync(key, val).listenAsync(new CIX1<GridFuture<?>>() {
                        @Override public void applyx(GridFuture<?> f) throws GridException {
                            setExpiration(cache, key, req);

                            res.setResponse(f.get());

                            fut.onDone(res);
                        }
                    });

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

                    if (key == null)
                        // Cache metrics.
                        metrics = cache.metrics();
                    else {
                        GridCacheEntry<Object, Object> entry = cache.entry(key);

                        assert entry != null;

                        // Entry metrics.
                        metrics = entry.metrics();
                    }

                    assert metrics != null;

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

            return new GridFinishedFuture<GridRestResponse>(ctx, e);
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
    private void setExpiration(GridCacheProjection<Object, Object> cache,
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

        Long init = null;

        if (initObj != null) {
            if (initObj instanceof String) {
                try {
                    init = Long.valueOf((String)initObj);
                }
                catch (NumberFormatException ignored) {
                    throw new GridException(invalidNumericParameter("init"));
                }
            }
            else if (initObj instanceof Long)
                init = (Long)initObj;
        }

        if (deltaObj == null)
            throw new GridException(missingParameter("delta"));

        Long delta = null;

        if (deltaObj instanceof String) {
            try {
                delta = Long.valueOf((String)deltaObj);
            }
            catch (NumberFormatException ignored) {
                throw new GridException(invalidNumericParameter("delta"));
            }
        }
        else if (deltaObj instanceof Long)
            delta = (Long)deltaObj;

        GridCacheAtomicLong l = init != null ? cache.atomicLong(key, init, false) : cache.atomicLong(key);

        GridFuture<Long> opFut = l.addAndGetAsync(decr ? -delta : delta);

        opFut.listenAsync(new CIX1<GridFuture<?>>() {
            @Override public void applyx(GridFuture<?> f) throws GridException {
                res.setResponse(f.get());

                fut.onDone(res);
            }
        });
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
     * @throws GridException In case of any exception.
     */
    private void appendOrPrepend(
        final GridCacheProjection<Object, Object> cache,
        final Object key,
        GridRestRequest req, final GridRestResponse res,
        final GridFutureAdapter<GridRestResponse> fut,
        final boolean prepend) throws GridException {
        assert cache != null;
        assert key != null;
        assert req != null;
        assert res != null;

        final Object val = value("val", req);

        if (val == null)
            throw new GridException(missingParameter("val"));

        cache.getAsync(key).listenAsync(new CIX1<GridFuture<Object>>() {
            @Override public void applyx(GridFuture<Object> f) throws GridException {
                Object currVal = f.get();

                if (currVal == null) {
                    res.setResponse(false);

                    fut.onDone(res);

                    return;
                }

                if (!(val instanceof String && currVal instanceof String)) {
                    fut.onDone(new GridException("Incompatible types."));

                    return;
                }

                String newVal = prepend ? (String)val + currVal : currVal + (String)val;

                GridFuture<Boolean> putFut = cache.putxAsync(key, newVal);

                putFut.listenAsync(new CIX1<GridFuture<Boolean>>() {
                    @Override public void applyx(GridFuture<Boolean> f) throws GridException {
                        res.setResponse(f.get());

                        fut.onDone(res);
                    }
                });
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCommandHandler.class, this);
    }
}
