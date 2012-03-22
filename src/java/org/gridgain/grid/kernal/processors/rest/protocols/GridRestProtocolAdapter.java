// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;
import java.security.*;
import java.util.*;

/**
 * Abstract protocol adapter.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public abstract class GridRestProtocolAdapter implements GridRestProtocol {
    /** Context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    protected final GridLogger log;

    /** Secret key. */
    protected final String secretKey;

    /**
     * @param ctx Context.
     */
    @SuppressWarnings({"OverriddenMethodCallDuringObjectConstruction"})
    protected GridRestProtocolAdapter(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;

        log = ctx.log(getClass());

        secretKey = ctx.config().getRestSecretKey();
    }

    /**
     * Authenticates current request.
     * <p>
     * Token consists of 2 parts separated by semicolon:
     * <ol>
     *     <li>Timestamp (time in milliseconds)</li>
     *     <li>Base64 encoded SHA1 hash of {1}:{secretKey}</li>
     * </ol>
     *
     * @param tok Authentication token.
     * @return {@code true} if authentication info provided in request is correct.
     */
    protected boolean authenticate(@Nullable String tok) {
        if (F.isEmpty(secretKey))
            return true;

        if (F.isEmpty(tok))
            return false;

        StringTokenizer st = new StringTokenizer(tok, ":");

        if (st.countTokens() != 2)
            return false;

        String ts = st.nextToken();
        String hash = st.nextToken();

        String s = ts + ':' + secretKey;

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            BASE64Encoder enc = new BASE64Encoder();

            md.update(s.getBytes());

            String compHash = enc.encode(md.digest());

            return hash.equalsIgnoreCase(compHash);
        }
        catch (NoSuchAlgorithmException e) {
            U.error(log, "Failed to check authentication signature.", e);
        }

        return false;
    }

    /**
     * @return Start information string.
     */
    protected String startInfo() {
        return "Command protocol successfully started: " + name();
    }

    /**
     * @return Stop information string.
     */
    protected String stopInfo() {
        return "Command protocol successfully stopped: " + name();
    }

    /**
     * @param cond Condition to check.
     * @param condDesc Error message.
     * @throws GridException If check failed.
     */
    protected final void assertParameter(boolean cond, String condDesc) throws GridException {
        if (!cond)
            throw new GridException("REST protocol parameter failed condition check: " + condDesc);
    }
}
