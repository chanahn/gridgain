// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.query.GridCacheQueryType.*;

/**
 * Query request.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public class GridCacheQueryRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private long id;

    /** */
    private String cacheName;

    /** Query id. */
    private int qryId;

    /** */
    private GridCacheQueryType type;

    /** */
    private String clause;

    /** */
    private String clsName;

    /** */
    private GridClosure<Object[], GridPredicate<? super K>> keyFilter;

    /** */
    private byte[] keyFilterBytes;

    /** */
    private GridClosure<Object[], GridPredicate<? super V>> valFilter;

    /** */
    private byte[] valFilterBytes;

    /** */
    private GridPredicate<GridCacheEntry<K, V>> prjFilter;

    /** */
    private byte[] prjFilterBytes;

    /** */
    private GridClosure<Object[], GridReducer<Map.Entry<K, Object>, Object>> rdc;

    /** */
    private byte[] rdcBytes;

    /** */
    private GridClosure<Object[], GridClosure<V, Object>> trans;

    /** */
    private byte[] transBytes;

    /** */
    private Object[] args;

    /** */
    private byte[] argsBytes;

    /** */
    private Object[] cArgs;

    /** */
    private byte[] cArgsBytes;

    /** */
    private int pageSize;

    /** */
    private boolean readThrough;

    /** */
    private boolean clone;

    /** */
    private boolean incBackups;

    /** */
    private boolean cancel;

    /** */
    private boolean single;

    /**
     * Required by {@link Externalizable}
     */
    public GridCacheQueryRequest() {
        // No-op.
    }

    /**
     * @param id Request to cancel.
     */
    public GridCacheQueryRequest(long id) {
        this.id = id;

        cancel = true;
    }

    /**
     * @param id Request id.
     * @param cacheName Cache name.
     * @param qryId Query id.
     * @param type Query type.
     * @param clause Query clause.
     * @param clsName Query class name.
     * @param keyFilter Key filter.
     * @param valFilter Value filter.
     * @param prjFilter Projection filter.
     * @param rdc Reducer.
     * @param trans Transformer.
     * @param pageSize Page size.
     * @param readThrough Read through flag.
     * @param clone {@code true} if values should be cloned.
     * @param incBackups {@code true} if need to include backups.
     * @param args Query arguments.
     * @param cArgs Query closure's arguments.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     */
    public GridCacheQueryRequest(
        long id,
        String cacheName,
        int qryId,
        GridCacheQueryType type,
        String clause,
        String clsName,
        GridClosure<Object[], GridPredicate<? super K>> keyFilter,
        GridClosure<Object[], GridPredicate<? super V>> valFilter,
        GridPredicate<GridCacheEntry<K, V>> prjFilter,
        GridClosure<Object[], GridReducer<Map.Entry<K, Object>, Object>> rdc,
        GridClosure<Object[], GridClosure<V, Object>> trans,
        int pageSize,
        boolean readThrough,
        boolean clone,
        boolean incBackups,
        Object[] args,
        Object[] cArgs,
        boolean single) {
        assert type != null;
        assert clause != null || type == SCAN;
        assert clsName != null || type == SCAN;

        this.id = id;
        this.cacheName = cacheName;
        this.qryId = qryId;
        this.type = type;
        this.clause = clause;
        this.clsName = clsName;
        this.keyFilter = keyFilter;
        this.valFilter = valFilter;
        this.prjFilter = prjFilter;
        this.rdc = rdc;
        this.trans = trans;
        this.pageSize = pageSize;
        this.readThrough = readThrough;
        this.clone = clone;
        this.incBackups = incBackups;
        this.args = args;
        this.cArgs = cArgs;
        this.single = single;
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.p2pMarshal(ctx);

        if (keyFilter != null) {
            prepareObject(keyFilter, ctx);

            keyFilterBytes = CU.marshal(ctx, keyFilter).array();
        }

        if (valFilter != null) {
            prepareObject(valFilter, ctx);

            valFilterBytes = CU.marshal(ctx, valFilter).array();
        }

        if (prjFilter != null) {
            prepareObject(prjFilter, ctx);

            prjFilterBytes = CU.marshal(ctx, prjFilter).array();
        }

        if (rdc != null) {
            prepareObject(rdc, ctx);

            rdcBytes = CU.marshal(ctx, rdc).array();
        }

        if (trans != null) {
            prepareObject(trans, ctx);

            transBytes = CU.marshal(ctx, trans).array();
        }

        if (!F.isEmpty(args)) {
            for (Object arg : args)
                prepareObject(arg, ctx);

            argsBytes = CU.marshal(ctx, args).array();
        }

        if (!F.isEmpty(cArgs)) {
            for (Object arg : cArgs)
                prepareObject(arg, ctx);

            cArgsBytes = CU.marshal(ctx, cArgs).array();
        }
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.p2pUnmarshal(ctx, ldr);

        GridMarshaller mrsh = ctx.marshaller();

        if (keyFilterBytes != null)
            keyFilter = U.unmarshal(mrsh, new ByteArrayInputStream(keyFilterBytes), ldr);

        if (valFilterBytes != null)
            valFilter = U.unmarshal(mrsh, new ByteArrayInputStream(valFilterBytes), ldr);

        if (prjFilterBytes != null)
            prjFilter = U.unmarshal(mrsh, new ByteArrayInputStream(prjFilterBytes), ldr);

        if (rdcBytes != null)
            rdc = U.unmarshal(mrsh, new ByteArrayInputStream(rdcBytes), ldr);

        if (transBytes != null)
            trans = U.unmarshal(mrsh, new ByteArrayInputStream(transBytes), ldr);

        if (argsBytes != null)
            args = U.unmarshal(mrsh, new ByteArrayInputStream(argsBytes), ldr);

        if (cArgsBytes != null)
            cArgs = U.unmarshal(mrsh, new ByteArrayInputStream(cArgsBytes), ldr);
    }

    /**
     * @return Request id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Query id.
     */
    public int queryId() {
        return qryId;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType type() {
        return type;
    }

    /**
     * @return Query clause.
     */
    public String clause() {
        return clause;
    }

    /**
     * @return Class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @return Flag indicating whether to read through.
     */
    public boolean readThrough() {
        return readThrough;
    }

    /**
     * @return Flag indicating whether to clone values.
     */
    public boolean cloneValues() {
        return clone;
    }

    /**
     * @return Flag indicating whether to include backups.
     */
    public boolean includeBackups() {
        return incBackups;
    }

    /**
     * @return Flag indicating that this is cancel request.
     */
    public boolean cancel() {
        return cancel;
    }

    /**
     * @return Key filter.
     */
    public GridClosure<Object[], GridPredicate<? super K>> keyFilter() {
        return keyFilter;
    }

    /**
     * @return Value filter.
     */
    public GridClosure<Object[], GridPredicate<? super V>> valueFilter() {
        return valFilter;
    }

    /** {@inheritDoc} */
    public GridPredicate<GridCacheEntry<K, V>> projectionFilter() {
        return prjFilter;
    }

    /**
     * @return Reducer.
     */
    public GridClosure<Object[], GridReducer<Map.Entry<K, Object>, Object>> reducer() {
        return rdc;
    }

    /**
     * @return Transformer.
     */
    public GridClosure<Object[], GridClosure<V, Object>> transformer() {
        return trans;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Closures' arguments.
     */
    public Object[] closureArguments() {
        return cArgs;
    }

    /**
     * @return {@code true} if single result requested, {@code false} otherwise.
     */
    public boolean single() {
        return single;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeLong(id);
        U.writeString(out, cacheName);
        out.writeInt(qryId);
        out.writeObject(type);
        U.writeString(out, clause);
        U.writeString(out, clsName);
        U.writeByteArray(out, keyFilterBytes);
        U.writeByteArray(out, valFilterBytes);
        U.writeByteArray(out, prjFilterBytes);
        U.writeByteArray(out, rdcBytes);
        U.writeByteArray(out, transBytes);
        U.writeByteArray(out, argsBytes);
        U.writeByteArray(out, cArgsBytes);
        out.writeInt(pageSize);
        out.writeBoolean(readThrough);
        out.writeBoolean(clone);
        out.writeBoolean(incBackups);
        out.writeBoolean(cancel);
        out.writeBoolean(single);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        id = in.readLong();
        cacheName = U.readString(in);
        qryId = in.readInt();
        type = (GridCacheQueryType)in.readObject();
        clause = U.readString(in);
        clsName = U.readString(in);
        keyFilterBytes = U.readByteArray(in);
        valFilterBytes = U.readByteArray(in);
        prjFilterBytes = U.readByteArray(in);
        rdcBytes = U.readByteArray(in);
        transBytes = U.readByteArray(in);
        argsBytes = U.readByteArray(in);
        cArgsBytes = U.readByteArray(in);
        pageSize = in.readInt();
        readThrough = in.readBoolean();
        clone = in.readBoolean();
        incBackups = in.readBoolean();
        cancel = in.readBoolean();
        single = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryRequest.class, this);
    }
}
