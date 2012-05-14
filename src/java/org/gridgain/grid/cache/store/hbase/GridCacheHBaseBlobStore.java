// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.hbase;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * {@link GridCacheStore} implementation backed by HBase.
 * <p>
 * Note that this class is intended for test purposes only and it is
 * not recommended to use it in production environment since it may
 * slow down performance.
 * <p>
 * Also note that HBase does not provide ACID guaranties for distributed
 * operations and it is up to end user to solve possible data inconsistencies.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridCacheHBaseBlobStore<K, V> extends GridCacheStoreAdapter<K, V> {
    /** Local TX delta attribute name. */
    private static final String TX_DELTA = "HBASE_TX_DELTA";

    /** Default table name. */
    public static final String DFLT_TABLE_NAME = "entries";

    /** Default column family name. */
    public static final String DFLT_COLUMN_NAME = "val";

    /** Default max table pool size. */
    public static final int DFLT_MAX_TABLE_POOL_SIZE = 10;

    /** Log. */
    @GridLoggerResource
    protected GridLogger log;

    /** HBase configuration. */
    private URL[] cfgUrls;

    /** Table name. */
    protected String tblName = DFLT_TABLE_NAME;

    /** Value column family descriptor. */
    protected HColumnDescriptor colDesc = new HColumnDescriptor(DFLT_COLUMN_NAME);

    /** Maximum table pool size. */
    protected int maxPoolSize = DFLT_MAX_TABLE_POOL_SIZE;

    /** Reusable HBase tables pool. */
    protected HTablePool tblPool;

    /** HBase admin. */
    protected HBaseAdmin admin;

    /** Marshaller. */
    @GridMarshallerResource
    private GridMarshaller marsh;

    /** */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable String cacheName, @Nullable GridCacheTx tx, Collection<? extends K> keys,
        GridInClosure2<K, V> c) throws GridException {
        List<Get> gets = new ArrayList<Get>(keys.size());

        for (K key : keys)
            gets.add(new Get(toBytes(key)));

        Result[] results = null;

        HTableInterface tbl = table();

        try {
            results = tbl.get(gets);
        }
        catch (IOException e) {
            throw new GridException("Failed to load values for keys: " + keys, e);
        }
        finally {
            close(tbl);
        }

        int i = 0;

        for (K key : keys) {
            Result r = results[i++];

            checkNull(r, key);

            V val = r.isEmpty() ? null : this.<V>fromBytes(r.value());

            c.apply(key, val);
        }
    }

    /** {@inheritDoc} */
    @Override public V load(@Nullable String cacheName, @Nullable GridCacheTx tx, K key) throws GridException {
        Get get = new Get(toBytes(key));

        HTableInterface tbl = table();

        Result r;

        try {
            r = tbl.get(get);
        }
        catch (IOException e) {
            throw new GridException("Failed to load value for key: " + key, e);
        }
        finally {
            close(tbl);
        }

        checkNull(r, key);

        if(r.isEmpty())
            return null;

        return fromBytes(r.value());
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable String cacheName, GridCacheTx tx, Map<? extends K, ? extends V> map)
        throws GridException {
        if (tx != null) {
            delta(tx).putAll(map);

            return;
        }

        List<Put> puts = new ArrayList<Put>(map.size());

        for (Map.Entry<? extends K ,? extends V> entry : map.entrySet())
            puts.add(newPut(entry.getKey(), entry.getValue()));

        HTableInterface tbl = table();

        try {
            tbl.put(puts);

            if (!tbl.isAutoFlush())
                tbl.flushCommits();
        }
        catch (IOException e) {
            throw new GridException("Failed to store entries: " + map, e);
        }
        finally {
            close(tbl);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable String cacheName, @Nullable GridCacheTx tx, K key, @Nullable V val)
        throws GridException {
        if (tx != null) {
            delta(tx).put(key, val);

            return;
        }

        Put put = newPut(key, val);

        HTableInterface tbl = table();

        try {
            tbl.put(put);

            if (!tbl.isAutoFlush())
                tbl.flushCommits();
        }
        catch (IOException e) {
            throw new GridException("Failed to store entry: " + key + "=" + val, e);
        }
        finally {
            close(tbl);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String cacheName, @Nullable GridCacheTx tx, K key) throws GridException {
        if (tx != null) {
            delta(tx).put(key, null);

            return;
        }

        Delete del = new Delete(toBytes(key));

        HTableInterface table = table();

        try {
            table.delete(del);
        }
        catch (IOException e) {
            throw new GridException("Failed to delete key: " + key, e);
        }
        finally {
            close(table);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable String cacheName, GridCacheTx tx, Collection<? extends K> keys)
        throws GridException {
        if (tx != null) {
            Map<K, V> delta = delta(tx);

            for (K key : keys)
                delta.put(key, null);

            return;
        }

        List<Delete> dels = new ArrayList<Delete>(keys.size());

        for (K key : keys)
            dels.add(new Delete(toBytes(key)));

        HTableInterface tbl = table();

        try {
            tbl.delete(dels);
        }
        catch (IOException e) {
            throw new GridException("Failed delete keys: " + keys, e);
        }
        finally {
            close(tbl);
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(@Nullable String cacheName, GridCacheTx tx, boolean commit) throws GridException {
        Map<K, V> delta = tx.removeMeta(TX_DELTA);

        if (!commit || F.isEmpty(delta))
            return;

        List<Row> ops = new ArrayList<Row>(delta.size());

        for (Map.Entry<K, V> e : delta.entrySet()) {
            K key = e.getKey();
            V val = e.getValue();

            if (val == null)
                ops.add(new Delete(toBytes(key)));
            else
                ops.add(newPut(key, val));
        }

        Object[] results;

        HTableInterface table = table();

        try {
            results = table.batch(ops);
        }
        catch (IOException e) {
            throw new GridException("Failed  to commit.", e);
        }
        catch (InterruptedException e) {
            throw new GridException("Unexpected interrupt.", e);
        }
        finally {
            close(table);
        }

        // check for errors
        int i = 0;

        for (K key : delta.keySet())
            checkNull(results[i++], key);
    }

    /**
     * If HBase operation result is null then operation was not successful.
     * Throwing exception in this case.
     *
     * @param result Result.
     * @param key Key on which operation was performed.
     * @throws GridException If does not pass check.
     */
    protected <R> void checkNull(R result, K key) throws GridException {
        if (result == null)
            throw new GridException("Failed to load value for key: " + key);
    }

    /**
     * Creates new 'put' operation.
     *
     * @param key Key.
     * @param val Value.
     * @return Created operation.
     * @throws GridException If failed.
     */
    private Put newPut(K key, V val) throws GridException {
        byte[] keyBytes = toBytes(key);
        byte[] valBytes = toBytes(val);

        Put put = new Put(keyBytes);

        put.add(colDesc.getName(), null, valBytes);

        return put;
    }

    /**
     * Gets local changes delta done in given transaction.
     *
     * @param tx Transaction.
     * @return Operations queue.
     */
    private Map<K, V> delta(GridCacheTx tx) {
        Map<K, V> m = tx.meta(TX_DELTA);

        if (m == null) {
            m = new LinkedHashMap<K, V>();

            Map<K, V> old = tx.addMeta(TX_DELTA, m);

            assert old == null;
        }

        return m;
    }

    /**
     * Close HBase table.
     *
     * @param tbl The table to close.
     */
    protected void close(HTableInterface tbl) throws GridException {
        if (tbl != null) {
            try {
                tbl.close();
            }
            catch (IOException e) {
                log.warning("Failed to close HBase table: " + tblName, e);
            }
        }
    }

    /**
     * Get HBase table to perform operations on.
     *
     * @return Table.
     * @throws GridException If failed.
     */
    protected HTableInterface table() throws GridException {
        if (initGuard.compareAndSet(false, true)) {
            try {
                Configuration cfg;

                if (F.isEmpty(cfgUrls))
                    cfg = HBaseConfiguration.create();
                else {
                    cfg = new Configuration();

                    for (URL url : cfgUrls)
                        cfg.addResource(url);
                }

                admin = new HBaseAdmin(cfg);

                if (admin.tableExists(tblName)) {
                    if (log.isDebugEnabled())
                        log.debug("HBase table exists: " + tblName);
                }
                else {
                    HTableDescriptor desc = new HTableDescriptor(tblName);

                    desc.addFamily(colDesc);

                    try {
                        admin.createTable(desc);

                        if (log.isDebugEnabled())
                            log.debug("Created HBase table: " + tblName);
                    }
                    catch (TableExistsException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("HBase table was concurrently created from elsewhere: " + tblName);
                    }
                }

                tblPool = new HTablePool(cfg, maxPoolSize);

                if (log.isDebugEnabled())
                    log.debug("HBase table pool initialized with size: " + maxPoolSize);

                initLatch.countDown();
            }
            catch (IOException e) {
                throw new GridException("Failed to initialize table: " + tblName, e);
            }
        }
        else if (initLatch.getCount() != 0) {
            try {
                initLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridException("Unexpected interruption.", e);
            }
        }

        return tblPool.getTable(tblName);
    }

    /**
     * Sets URLs to HBase XML configuration.
     *
     * @param cfgUrls Urls of HBase XML configurations.
     */
    public void setConfigurationUrls(URL... cfgUrls) {
        this.cfgUrls = cfgUrls;
    }

    /**
     * Sets HBase table name to store data.
     *
     * @param tblName Table name.
     */
    public void setTableName(String tblName) {
        this.tblName = tblName;
    }

    /**
     * Sets HBase value column descriptor.
     *
     * @param colDesc Descriptor.
     */
    public void setColumnDescriptor(HColumnDescriptor colDesc) {
        this.colDesc = colDesc;
    }

    /**
     * Maximum allowed HBase table pool size.
     *
     * @param maxPoolSize Max pool size.
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Drops HBase table.
     */
    void dropTable() throws IOException {
        if (initLatch.getCount() == 0) {
            admin.disableTable(tblName);

            admin.deleteTable(tblName);
        }
    }

    /**
     * Serialize object to byte array using marshaller.
     *
     * @param obj Object to convert to byte array.
     * @return Byte array.
     * @throws GridException If failed to convert.
     */
    protected byte[] toBytes(Object obj) throws GridException {
        GridByteArrayOutputStream bos = new GridByteArrayOutputStream();

        marsh.marshal(obj, bos);

        return bos.toByteArray();
    }

    /**
     * Deserialize object from byte array using marshaller.
     *
     * @param bytes Bytes to deserialize.
     * @param <X> Result object type.
     * @return Deserialized object.
     * @throws GridException If failed.
     */
    protected <X> X fromBytes(byte[] bytes) throws GridException {
        if (bytes == null || bytes.length == 0)
            return null;

        return marsh.unmarshal(new GridByteArrayInputStream(bytes), getClass().getClassLoader());
    }
}
