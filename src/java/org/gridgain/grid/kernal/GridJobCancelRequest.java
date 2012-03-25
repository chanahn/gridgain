// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;

/**
 * This class defines externalizable job cancellation request.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridJobCancelRequest implements Externalizable {
    /** */
    private GridUuid sesId;

    /** */
    private GridUuid jobId;

    /** */
    private boolean sys;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public GridJobCancelRequest() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     */
    public GridJobCancelRequest(GridUuid sesId) {
        assert sesId != null;

        this.sesId = sesId;

        jobId = null;
    }

    /**
     * @param sesId Task session ID.
     * @param jobId Job ID.
     */
    public GridJobCancelRequest(GridUuid sesId, GridUuid jobId) {
        assert sesId != null;

        this.sesId = sesId;
        this.jobId = jobId;
    }

    /**
     * @param sesId Session ID.
     * @param jobId Job ID.
     * @param sys System flag.
     */
    public GridJobCancelRequest(GridUuid sesId, GridUuid jobId, boolean sys) {
        assert sesId != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.sys = sys;
    }

    /**
     * Gets execution ID of task to be cancelled.
     *
     * @return Execution ID of task to be cancelled.
     */
    public GridUuid getSessionId() {
        return sesId;
    }

    /**
     * Gets session ID of job to be cancelled. If {@code null}, then
     * all jobs for the specified task execution ID will be cancelled.
     *
     * @return Execution ID of job to be cancelled.
     */
    public GridUuid getJobId() {
        return jobId;
    }

    /**
     * @return {@code True} if request to cancel is sent out of system when task
     *       has already reduced and further results are no longer interesting.
     */
    public boolean isSystem() {
        return sys;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, sesId);
        U.writeGridUuid(out, jobId);

        out.writeBoolean(sys);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sesId = U.readGridUuid(in);
        jobId = U.readGridUuid(in);

        sys = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobCancelRequest.class, this);
    }
}
