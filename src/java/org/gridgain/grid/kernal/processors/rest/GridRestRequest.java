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

import java.util.*;

/**
 * Grid command request. Getters and setters must conform to JavaBean standard.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridRestRequest {
    /** Client id. */
    private UUID clientId;

    /** Client credentials. */
    @GridToStringExclude
    private Object cred;

    /** Client session token. */
    private byte[] sesTok;

    /** Command. */
    private GridRestCommand cmd;

    /** Path. */
    private String path;

    /** Parameters. */
    @GridToStringInclude
    private Map<String, Object> params;

    /**
     * Empty constructor.
     */
    public GridRestRequest() {
        params = new HashMap<String, Object>();
    }

    /**
     * @param cmd Command.
     */
    public GridRestRequest(GridRestCommand cmd) {
        this.cmd = cmd;

        params = new HashMap<String, Object>();
    }

    /**
     * @param cmd Command.
     * @param path Path.
     */
    public GridRestRequest(GridRestCommand cmd, String path) {
        this.cmd = cmd;
        this.path = path;

        params = new HashMap<String, Object>();
    }

    /**
     * @param cmd Command.
     * @param path Path.
     * @param params Parameters.
     */
    public GridRestRequest(GridRestCommand cmd, String path, Map<String, Object> params) {
        this.cmd = cmd;
        this.path = path;
        this.params = params;
    }

    /**
     * @return Command.
     */
    public GridRestCommand getCommand() {
        return cmd;
    }

    /**
     * @param cmd Command.
     */
    public void setCommand(GridRestCommand cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Parameters.
     */
    public Map<String, Object> getParameters() {
        return params;
    }

    /**
     * @param params Parameters.
     */
    public void setParameters(Map<String, Object> params) {
        this.params.putAll(params);
    }

    /**
     * Clears parameters.
     */
    public void clearParameters() {
        params.clear();
    }

    /**
     * @param name Parameter name.
     * @return Parameter value.
     */
    @SuppressWarnings( {"unchecked"})
    public <T> T parameter(String name) {
        return (T)params.get(name);
    }

    /**
     * @param name Name.
     * @param val Value.
     */
    public void parameter(String name, Object val) {
        params.put(name, val);
    }

    /**
     * @return Path.
     */
    public String getPath() {
        return path;
    }

    /**
     * @param path Path.
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Gets client id that performed request.
     *
     * @return Client id.
     */
    public UUID getClientId() {
        return clientId;
    }

    /**
     * Sets client id that performed request.
     *
     * @param clientId Client id.
     */
    public void setClientId(UUID clientId) {
        this.clientId = clientId;
    }

    /**
     * Gets client credentials for authentication process.
     *
     * @return Credentials.
     */
    public Object getCredentials() {
        return cred;
    }

    /**
     * Sets client credentials for authentication.
     *
     * @param cred Credentials.
     */
    public void setCredentials(Object cred) {
        this.cred = cred;
    }

    /**
     * Gets session token for already authenticated client.
     *
     * @return Session token.
     */
    public byte[] getSessionToken() {
        return sesTok;
    }

    /**
     * Sets session token for already authenticated client.
     *
     * @param sesTok Session token.
     */
    public void setSessionToken(byte[] sesTok) {
        this.sesTok = sesTok;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestRequest.class, this);
    }
}
