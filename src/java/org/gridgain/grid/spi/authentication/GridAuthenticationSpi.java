// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

/**
 * Authentication SPI used for authenticating grid nodes and remote clients. This SPI
 * supports only {@code authentication} and does not provide any {@code authorization}
 * functionality.
 * <p>
 * The default authentication SPI is {@link org.gridgain.grid.spi.authentication.noop.GridNoopAuthenticationSpi}
 * which permits any request.
 * <p>
 * Gridgain provides the following {@code GridAuthenticationSpi} implementations:
 * <ul>
 * <li>
 *     {@link org.gridgain.grid.spi.authentication.noop.GridNoopAuthenticationSpi} - permits any request.
 * </li>
 * <li>
 *     {@link org.gridgain.grid.spi.authentication.passcode.GridPasscodeAuthenticationSpi} -
 *     validates authentication with passcode phrase.
 * </li>
 * <li>
 *     {@link org.gridgain.grid.spi.authentication.jaas.GridJaasAuthenticationSpi} -
 *     validates authentication with JAAS Java extension.
 * </li>
 * </ul>
 * <p>
 * <b>NOTE:</b> multiple authentication SPIs may be started on the same grid node. In this case
 * GridGain will differentiate between them based on {@link #supported(GridSecuritySubjectType)}
 * value. The first SPI which returns {@code true} for a given subject type will be used for
 * authentication.
 * <p>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link Grid#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
@GridSpiConsistencyEnforced
public interface GridAuthenticationSpi extends GridSpi, GridSpiJsonConfigurable {
    /**
     * Checks if given subject is supported by this SPI. If not, then next authentication SPI
     * in the list will be checked.
     *
     * @param subjType Subject type.
     * @return {@code True} if subject type is supported, {@code false} otherwise.
     */
    boolean supported(GridSecuritySubjectType subjType);

    /**
     * Authenticates a given subject (either node or remote client).
     *
     * @param subjType Subject type.
     * @param subjId Unique subject ID such as local or remote node ID, client ID, etc.
     * @param credentials Authentication parameters (may be {@code null} or empty based on implementation).
     *      The map of parameters may be different for different subject types. Refer to specific
     *      authentication SPI documentation for a list of required parameters.
     * @return {@code true} if authentication passed, {@code false} if authentication failed.
     * @throws GridSpiException If authentication resulted in system error.
     *      Note that bad credentials should not cause this exception.
     */
    boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable Object credentials)
        throws GridSpiException;
}
