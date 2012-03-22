// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.multicast;

import org.gridgain.grid.spi.discovery.*;
import java.util.*;

/**
 * This enumeration defines types for multicast discovery messages.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
enum GridMulticastDiscoveryMessageType {
    /** Ping request is sent when {@link GridDiscoverySpi#pingNode(UUID)} is called. */
    PING_REQUEST,

    /** Response to PING request. */
    PING_RESPONSE,

    /**
     * Node attributes request. Receiving this request node
     * has to send its attributes back.
     * */
    ATTRS_REQUEST,

    /**
     * Response to attribute request.
     */
    ATTRS_RESPONSE,

    /** Final confirmation that attributes response was processed. */
    ATTRS_CONFIRMED,

    /** Request processing failed due to authentication restrictions. */
    AUTH_FAILED
}
