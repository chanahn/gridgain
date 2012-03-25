// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.logger.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Abstract command handler.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public abstract class GridRestCommandHandlerAdapter implements GridRestCommandHandler {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Log. */
    protected final GridLogger log;

    /**
     * @param ctx Context.
     */
    protected GridRestCommandHandlerAdapter(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridRestCommand cmd) {
        return false;
    }

    /**
     * @param key Key.
     * @param req Request.
     * @return Value.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable protected <T> T value(String key, GridRestRequest req) {
        assert key != null;
        assert req != null;

        Object val = req.parameter(key);

        if (val != null && val instanceof String)
            return ((String)val).isEmpty() ? null : (T)val;

        return (T)val;
    }

    /**
     * Gets values referenced by sequential keys, e.g. {@code key1...keyN}.
     *
     * @param keyPrefix Key prefix, e.g. {@code key} for {@code key1...keyN}.
     * @param req Request.
     * @return Values.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable protected <T> List<T> values(String keyPrefix, GridRestRequest req) {
        assert keyPrefix != null;
        assert req != null;

        List<T> vals = new LinkedList<T>();
        Set<String> keys = req.getParameters().keySet();

        int i = 1;

        while (true) {
            String key = keyPrefix + i++;

            if (keys.contains(key))
                vals.add(this.<T>value(key, req));
            else
                break;
        }

        return vals;
    }

    /**
     * Return missing parameter error message.
     *
     * @param param Parameter name.
     * @return Missing parameter error message.
     */
    protected String missingParameter(String param) {
        return "Failed to find mandatory parameter in request: " + param;
    }

    /**
     * Return invalid numeric parameter error message.
     *
     * @param param Parameter name.
     * @return Invalid numeric parameter error message.
     */
    protected String invalidNumericParameter(String param) {
        return "Failed to parse numeric parameter: " + param;
    }
}
