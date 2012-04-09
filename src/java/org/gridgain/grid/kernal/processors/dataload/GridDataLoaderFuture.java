// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Data loader future.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
class GridDataLoaderFuture extends GridFutureAdapter<Object> {
    /** Data loader. */
    @GridToStringExclude
    private GridDataLoader dataLdr;

    /**
     * Default constructor for {@link Externalizable} support.
     */
    public GridDataLoaderFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param dataLdr Data loader.
     */
    GridDataLoaderFuture(GridKernalContext ctx, GridDataLoader dataLdr) {
        super(ctx);

        assert dataLdr != null;

        this.dataLdr = dataLdr;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws GridException {
        checkValid();

        if (onCancelled()) {
            dataLdr.close(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(dataLdr);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        dataLdr = (GridDataLoader)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDataLoaderFuture.class, this, super.toString());
    }
}
