// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.message;

import java.util.*;

/**
 * Generic cache request.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridClientCacheRequest<K, V> extends GridClientAbstractMessage {
    /**
     * Available cache operations
     */
    @SuppressWarnings("PublicInnerClass")
    public enum GridCacheOperation {
        /** Cache put. */
        PUT,

        /** Cache put all. */
        PUT_ALL,

        /** Cache get. */
        GET,

        /** Cache get all. */
        GET_ALL,

        /** Cache remove. */
        RMV,

        /** Cache remove all. */
        RMV_ALL,

        /** Cache add (put only if not exists). */
        ADD,

        /** Cache replace (put only if exists).  */
        REPLACE,

        /** Cache compare and set. */
        CAS,

        /** Cache metrics request. */
        METRICS
    }

    /** Requested cache operation. */
    private GridCacheOperation op;

    /** Cache name. */
    private String cacheName;

    /** Key */
    private K key;

    /** Value (expected value for CAS). */
    private V val;

    /** New value for CAS. */
    private V val2;

    /** Keys and values for put all, get all, remove all operations. */
    private Map<K, V> vals;

    /** Bit map of cache flags to be enabled on cache projection */
    private int cacheFlagsOn;

    /**
     * Creates grid cache request.
     *
     * @param op Requested operation.
     */
    public GridClientCacheRequest(GridCacheOperation op) {
        this.op = op;
    }

    /**
     * @return Requested operation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * Gets cache name.
     *
     * @return Cache name, or {@code null} if not set.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Gets cache name.
     *
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Key.
     */
    public K key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(K key) {
        this.key = key;
    }

    /**
     * @return Value 1.
     */
    public V value() {
        return val;
    }

    /**
     * @param val Value 1.
     */
    public void value(V val) {
        this.val = val;
    }

    /**
     * @return Value 2.
     */
    public V value2() {
        return val2;
    }

    /**
     * @param val2 Value 2.
     */
    public void value2(V val2) {
        this.val2 = val2;
    }

    /**
     * @return Values map for batch operations.
     */
    public Map<K, V> values() {
        return vals;
    }

    /**
     * @param vals Values map for batch operations.
     */
    public void values(Map<K, V> vals) {
        this.vals = vals;
    }

    /**
     * @param keys Keys collection
     */
    public void keys(Iterable<K> keys) {
        vals = new HashMap<K, V>();

        for (K k : keys)
            vals.put(k, null);
    }

    /**
     * Set cache flags bit map.
     *
     * @param cacheFlagsOn Bit representation of cache flags.
     */
    public void cacheFlagsOn(int cacheFlagsOn) {
        this.cacheFlagsOn = cacheFlagsOn;
    }

    /**
     * Get cache flags bit map.
     * @return Bit representation of cache flags.
     */
    public int cacheFlagsOn() {
        return cacheFlagsOn;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientCacheCASRequest [op=" + op + ", key=" + key + ", val=" + val + ", val2=" + val2 + ", vals=" +
            vals + ", cacheFlagsOn=" + cacheFlagsOn + "]";
    }
}
