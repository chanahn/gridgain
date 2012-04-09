// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation;

import org.gridgain.grid.*;
import org.gridgain.grid.loaders.cmdline.*;
import org.gridgain.grid.spi.discovery.*;

/**
 * Policy that defines how node will react on topology segmentation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 * @see GridSegmentationResolver
 */
public enum GridSegmentationPolicy {
    /**
     * When segmentation policy is {@code RESTART_JVM}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and then JVM will be restarted.
     * Note, that this will work <b>only</b> if GridGain is started with {@link GridCommandLineLoader}
     * via standard <code>ggstart.{sh|bat}</code> shell script.
     */
    RESTART_JVM,

    /**
     * When segmentation policy is {@code STOP}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and then particular grid node
     * will be stopped via call to {@link GridFactory#stop(String, boolean, boolean)}.
     */
    STOP,

    /**
     * When segmentation policy is {@code RECONNECT}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} and then discovery manager will
     * try to reconnect discovery SPI to topology issuing
     * {@link GridEventType#EVT_NODE_RECONNECTED} event on reconnect.
     * <p>
     * Note, that this policy is not allowed when distributed data grid is enabled.
     * <p>
     * This policy can be used only with {@link GridDiscoverySpi} implementation that
     * has support for reconnect (i.e. annotated with {@link GridDiscoverySpiReconnectSupport}
     * annotation).
     */
    RECONNECT,

    /**
     * When segmentation policy is {@code NOOP}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and it is up to user to
     * implement logic to handle this event.
     */
    NOOP
}

