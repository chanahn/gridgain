// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import java.lang.annotation.*;

/**
 * SPIs that have this annotation present will be checked for consistency within grid.
 * If SPIs are not consistent, then warning will be printed out to the log.
 * <p>
 * Note that SPI consistency courtesy log can also be disabled by disabling
 * {@link org.gridgain.grid.GridConfiguration#COURTESY_LOGGER_NAME} category in log configuration.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GridSpiConsistencyChecked {
    /**
     * Optional consistency check means that check will be performed only if
     * SPI class names and versions match.
     */
    @SuppressWarnings("JavaDoc")
    public boolean optional();
}
