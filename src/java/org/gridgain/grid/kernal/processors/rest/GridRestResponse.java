// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

/**
 * JSON response. Getters and setters must conform to JavaBean standard.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridRestResponse {
    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Authentication failure. */
    public static final int STATUS_AUTH_FAILED = 2;

    /** Success status. */
    private int successStatus;

    /** Session token. */
    private byte[] sesTokBytes;

    /** Session token string representation. */
    private String sesTokStr;

    /** Error. */
    private String err;

    /** Response object. */
    @GridToStringInclude
    private Object obj;

    /**
     *
     */
    public GridRestResponse() {
        // No-op.
    }

    /**
     * @param successStatus Success.
     */
    public GridRestResponse(int successStatus) {
        this.successStatus = successStatus;
    }

    /**
     * @param successStatus Success.
     * @param obj Response object.
     */
    public GridRestResponse(int successStatus, Object obj) {
        this.successStatus = successStatus;
        this.obj = obj;
    }

    /**
     * @param successStatus Success.
     * @param obj Response object.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public GridRestResponse(int successStatus, @Nullable Object obj, @Nullable String err) {
        this.successStatus = successStatus;
        this.obj = obj;
        this.err = err;
    }

    /**
     * @return Success flag.
     */
    public int getSuccessStatus() {
        return successStatus;
    }

    /**
     * @param successStatus Success flag.
     */
    public void setSuccessStatus(int successStatus) {
        this.successStatus = successStatus;
    }

    /**
     * @return Response object.
     */
    public Object getResponse() {
        return obj;
    }

    /**
     * @param obj Response object.
     */
    public void setResponse(@Nullable Object obj) {
        this.obj = obj;
    }

    /**
     * @return Error.
     */
    public String getError() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void setError(String err) {
        this.err = err;
    }

    /**
     * @return Session token for remote client.
     */
    public byte[] sessionTokenBytes() {
        return sesTokBytes;
    }

    /**
     * @param sesTokBytes Session token for remote client.
     */
    public void sessionTokenBytes(byte[] sesTokBytes) {
        this.sesTokBytes = sesTokBytes;
    }

    /**
     * @return String representation of session token.
     */
    public String getSessionToken() {
        return sesTokStr;
    }

    /**
     * @param sesTokStr String representation of session token.
     */
    public void setSessionToken(String sesTokStr) {
        this.sesTokStr = sesTokStr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestResponse.class, this);
    }
}
