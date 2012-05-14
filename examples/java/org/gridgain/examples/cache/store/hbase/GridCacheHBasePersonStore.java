package org.gridgain.examples.cache.store.hbase;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.gridgain.examples.cache.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;


/**
 * Example of {@link UUID} implementation that uses HBase
 * and maps {@link Person} to HBase row.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridCacheHBasePersonStore extends GridCacheStoreAdapter<UUID, Person> {
    /** Default config path. */
    private static final String DFLT_CONFIG_PATH = "org/gridgain/examples/cache/store/hbase/hbase-site.xml";

    /** Local TX delta key. */
    private static final String TX_DELTA = "HBASE_TX_DELTA_PERSON";

    /** Table name. */
    private static final String TABLE_NAME = "persons";

    /** Maximum allowed pool size. */
    private static final int MAX_POOL_SIZE = 4;

    /** HBase table pool. */
    private HTablePool tblPool;

    /** HBase column descriptor for first name. */
    private final HColumnDescriptor firstName = new HColumnDescriptor("firstName");

    /** HBase column descriptor for last name. */
    private final HColumnDescriptor lastName = new HColumnDescriptor("lastName");

    /** HBase column descriptor for resume. */
    private final HColumnDescriptor resume = new HColumnDescriptor("resume");

    /**
     * Constructor.
     */
    public GridCacheHBasePersonStore() throws Exception {
        prepareDb();
    }

    /**
     * Does initialization.
     *
     * @throws IOException If failed.
     */
    private void prepareDb() throws IOException {
        Configuration cfg = new Configuration();

        cfg.addResource(DFLT_CONFIG_PATH);

        HBaseAdmin admin = new HBaseAdmin(cfg);

        if (!admin.tableExists(TABLE_NAME)) {
            HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);

            desc.addFamily(firstName);
            desc.addFamily(lastName);
            desc.addFamily(resume);

            admin.createTable(desc);
        }

        tblPool = new HTablePool(cfg, MAX_POOL_SIZE);
    }

    /**
     * Serialize UUID to bytes.
     *
     * @param uuid UUID to serialize.
     * @return Bytes.
     */
    private byte[] toBytes(UUID uuid) {
        byte[] bytes = new byte[16];

        Bytes.putLong(bytes, 0, uuid.getMostSignificantBits());
        Bytes.putLong(bytes, 8, uuid.getLeastSignificantBits());

        return bytes;
    }

    /**
     * Deserialize UUID.
     *
     * @param bytes Bytes deserialize from.
     * @return UUID.
     */
    private UUID fromBytes(byte[] bytes) {
        long msb = Bytes.toLong(bytes, 0, 8);
        long lsb = Bytes.toLong(bytes, 8, 8);

        return new UUID(msb, lsb);
    }

    /**
     * Creates person object from result.
     *
     * @param r Result.
     * @return Person.
     */
    private Person person(Result r) {
        Person p = new Person();

        p.setId(fromBytes(r.getRow()));
        p.setFirstName(Bytes.toString(r.getValue(firstName.getName(), null)));
        p.setLastName(Bytes.toString(r.getValue(lastName.getName(), null)));
        p.setResume(Bytes.toString(r.getValue(resume.getName(), null)));

        return p;
    }

    /** {@inheritDoc} */
    @Override public Person load(@Nullable String cacheName, @Nullable GridCacheTx tx, UUID key) throws GridException {
        HTableInterface t = tblPool.getTable(TABLE_NAME);

        try {
            Result r = t.get(new Get(toBytes(key)));

            if (r == null)
                throw new GridException("Failed to load key: " + key);

            if (r.isEmpty())
                return null;

            return person(r);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        finally {
            close(t);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable String cacheName, @Nullable GridCacheTx tx, UUID key, @Nullable Person val)
        throws GridException {
        if (tx != null) {
            delta(tx).put(key, val);

            return;
        }

        HTableInterface t = tblPool.getTable(TABLE_NAME);

        try {
            t.put(new Put(toBytes(key))
                .add(firstName.getName(), null, Bytes.toBytes(val.getFirstName()))
                .add(lastName.getName(), null, Bytes.toBytes(val.getLastName()))
                .add(resume.getName(), null, Bytes.toBytes(val.getResume())));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        finally {
            close(t);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String cacheName, @Nullable GridCacheTx tx, UUID key) throws GridException {
        if (tx != null) {
            delta(tx).put(key, null);

            return;
        }

        HTableInterface t = tblPool.getTable(TABLE_NAME);

        try {
            t.delete(new Delete(toBytes(key)));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        finally {
            close(t);
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(@Nullable String cacheName, GridCacheTx tx, boolean commit) throws GridException {
        Map<UUID, Person> m = tx.removeMeta(TX_DELTA);

        if (m == null || !commit)
            return;

        for (Map.Entry<UUID, Person> e : m.entrySet()) {
            if (e.getValue() == null)
                remove(cacheName, null, e.getKey());
            else
                put(cacheName, null, e.getKey(), e.getValue());
        }
    }

    /**
     * Closes HBase table.
     *
     * @param t Table.
     */
    private void close(@Nullable HTableInterface t) {
        if (t != null) {
            try {
                t.close();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Local delta for given transaction.
     *
     * @param tx Transaction.
     * @return Local delta.
     */
    private Map<UUID, Person> delta(GridCacheTx tx) {
        Map<UUID, Person> m = tx.meta(TX_DELTA);

        if (m == null) {
            m = new HashMap<UUID, Person>();

            tx.addMeta(TX_DELTA, m);
        }

        return m;
    }
}
