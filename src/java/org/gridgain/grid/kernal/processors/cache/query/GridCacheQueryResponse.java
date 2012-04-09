// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Query request.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public class GridCacheQueryResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** */
    private boolean finished;

    /** */
    private long requestId;

    /** */
    private int qryId;

    /** */
    private Throwable err;

    /** */
    @GridToStringInclude
    private Collection<byte[]> dataBytes;

    /** */
    @GridToStringInclude
    private Collection<Object> data;

    /**
     * Empty constructor for {@link Externalizable}
     */
    public GridCacheQueryResponse() {
        //No-op.
    }

    /**
     * @param requestId Request id.
     * @param qryId Query id.
     */
    public GridCacheQueryResponse(long requestId, int qryId) {
        this.requestId = requestId;
        this.qryId = qryId;
    }

    /**
     * @param requestId Request id.
     * @param qryId Query id.
     * @param finished Last response or not.
     */
    public GridCacheQueryResponse(long requestId, int qryId, boolean finished) {
        this.requestId = requestId;
        this.finished = finished;
        this.qryId = qryId;
    }

    /**
     * @param requestId Request id.
     * @param qryId Query id.
     * @param err Error.
     */
    public GridCacheQueryResponse(long requestId, int qryId, Throwable err) {
        this.requestId = requestId;
        this.qryId = qryId;
        this.err = err;
        finished = true;
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.p2pMarshal(ctx);

        dataBytes = marshalCollection(data, ctx);

        if (!F.isEmpty(data))
            for (Object o : data)
                if (o instanceof Map.Entry) {
                    Map.Entry e = (Map.Entry)o;

                    prepareObject(e.getKey(), ctx);
                    prepareObject(e.getValue(), ctx);
                }
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.p2pUnmarshal(ctx, ldr);

        data = unmarshalCollection(dataBytes, ctx, ldr);

        if (!F.isEmpty(data))
            for (Object obj : data)
                if (obj instanceof GridCacheQueryResponseEntry)
                    ((GridCacheQueryResponseEntry)obj).unmarshal(ctx.marshaller(), ldr);
    }

    /**
     * @return Query data.
     */
    public Collection<Object> data() {
        return data;
    }

    /**
     * @param data Query data.
     */
    @SuppressWarnings( {"unchecked"})
    public void data(Collection<?> data) {
        this.data = (Collection<Object>)data;
    }

    /**
     * @return If this is last response for this request or not.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @param finished If this is last response for this request or not.
     */
    public void finished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return requestId;
    }

    /**
     * @return Query id.
     */
    public int queryId() {
        return qryId;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(finished);
        out.writeLong(requestId());
        out.writeInt(qryId);
        out.writeObject(err);

        U.writeCollection(out, dataBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        finished = in.readBoolean();
        requestId = in.readLong();
        qryId = in.readInt();
        err = (Throwable)in.readObject();

        dataBytes = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryResponse.class, this);
    }
}
