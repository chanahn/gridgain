// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.controllers.license;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.controllers.*;
import org.jetbrains.annotations.*;

/**
 * Kernal controller responsible for license management.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public interface GridLicenseController extends GridController {
    /**
     * This method is called periodically by the GridGain to check the license
     * conformance.
     *
     * @throws GridLicenseException Thrown in case of any license violation.
     */
    public void checkLicense() throws GridLicenseException;

    /**
     * Upload the new license into the current node. Throw the exception if the license is not validated.
     *
     * @param licenseCtx String - The string representation of the license file.
     * @throws GridLicenseException - Throw the exception in the case of failed validation.
     */
    public void updateLicense(String licenseCtx) throws GridLicenseException;

    /**
     * Acks the license to the log.
     */
    public void ackLicense();

    /**
     * Gets enterprise license descriptor.
     *
     * @return Enterprise license descriptor.
     */
    @Nullable public GridEnterpriseLicense license();
}
