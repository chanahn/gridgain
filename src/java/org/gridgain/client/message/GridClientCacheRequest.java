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
 * @version 4.0.1c.07042012
 */
public class GridClientCacheRequest<K, V> extends GridClientAbstractMessage {
    /**
     * Available cache operations
     */
    @SuppressWarnings("PublicInnerClass")
    public enum GridCacheOperation {
        /** Cache put. */
        PUT((byte)0x01),

        /** Cache put all. */
        PUT_ALL((byte)0x02),

        /** Cache get. */
        GET((byte)0x03),

        /** Cache get all. */
        GET_ALL((byte)0x04),

        /** Cache remove. */
        RMV((byte)0x05),

        /** Cache remove all. */
        RMV_ALL((byte)0x06),

        /** Cache add (put only if not exists). */
        ADD((byte)0x07),

        /** Cache replace (put only if exists).  */
        REPLACE((byte)0x08),

        /** Cache compare and set. */
        CAS((byte)0x09),

        /** Cache metrics request. */
        METRICS((byte)0x0A);

        /** Operation code */
        private byte opCode;

        /**
         * Creates enum value.
         *
         * @param opCode Operation code.
         */
        GridCacheOperation(byte opCode) {
            this.opCode = opCode;
        }

        /**
         * @return Operation code.
         */
        public byte opCode() {
            return opCode;
        }

        /**
         * Tries to find enum value by operation code.
         *
         * @param val Operation code value.
         * @return Enum value.
         */
        public static GridCacheOperation findByOperationCode(int val) {
            switch (val) {
                case 1:
                    return PUT;
                case 2:
                    return PUT_ALL;
                case 3:
                    return GET;
                case 4:
                    return GET_ALL;
                case 5:
                    return RMV;
                case 6:
                    return RMV_ALL;
                case 7:
                    return ADD;
                case 8:
                    return REPLACE;
                case 9:
                    return CAS;
                case 10:
                    return METRICS;
                default:
                    throw new IllegalArgumentException("Invalid value: " + val);
            }
        }
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return new StringBuilder().
            append("GridClientCacheCASRequest [op=").
            append(op).
            append(", key=").
            append(key).
            append(", val=").
            append(val).
            append(", val2=").
            append(val2).
            append("vals=").
            append(vals).
            append("]").
            toString();
    }
}
