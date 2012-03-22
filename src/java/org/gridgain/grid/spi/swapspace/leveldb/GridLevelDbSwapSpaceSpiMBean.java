// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace.leveldb;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;
import org.jetbrains.annotations.*;

/**
 * Management bean for {@link GridLevelDbSwapSpaceSpi}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
@GridMBeanDescription("MBean that provides administrative and configuration information on LevelDB-based swapspace SPI.")
public interface GridLevelDbSwapSpaceSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets path to the directory where all swap space values are saved.
     *
     * @return Path to the swap space directory.
     */
    @GridMBeanDescription("Path to the directory where all swap space values are saved.")
    public String getRootFolderPath();

    /**
     * Gets root folder index range. Root folder
     *
     * @return Root folder index range.
     */
    @GridMBeanDescription("Root folder index range.")
    public int getRootFolderIndexRange();

    /**
     * Gets flag controlling whether swap space should survive restarts.
     *
     * @return {@code True} if data must not be deleted at SPI stop, otherwise {@code false}.
     */
    @GridMBeanDescription("Flag controlling whether swap space should survive restarts.")
    public boolean isPersistent();

    /**
     * Gets maximum count (number of entries) for all swap spaces.
     *
     * @return Maximum count for all swap spaces.
     */
    @GridMBeanDescription("Default maximum entry count for each swap space.")
    public long getDefaultMaxSwapCount();

    /**
     * Gets maximum entry counts per space.
     *
     * @return Maximum count for individual swap spaces.
     */
    @GridMBeanDescription("Maximum entry counts per space.")
    @Nullable public String getMaxSwapCountsFormatted();

    /**
     * Gets swap space eviction overflow ratio. Evictions will start when swap space entry count
     * overgrows allowed maximum by this ratio.
     *
     * @return cntOverflowRatio Overflow ratio.
     */
    @GridMBeanDescription("Eviction overflow ratio.")
    public double getEvictOverflowRatio();

    /**
     * Gets eviction frequency (in milliseconds) set for swap spaces.
     *
     * @return evictFrequency Eviction frequency in milliseconds.
     */
    @GridMBeanDescription("Eviction frequency (in milliseconds).")
    public long getEvictFrequency();

    /**
     * Gets eviction policy set for swap spaces.
     *
     * @return evictPolicy Eviction policy converted to string string
     * to avoid need of corresponding class in tool classpath.
     */
    @GridMBeanDescription("Eviction policy.")
    public String getEvictPolicyFormatted();

    /**
     * Gets current total swap entries size on disk in all spaces.
     *
     * @return Total swap entries size on disk (in all spaces).
     */
    @GridMBeanDescription("Total swap disk size for all spaces.")
    public long getTotalDiskSize();

    /**
     * Gets current data size for all values in all spaces.
     *
     * @return Total size of data in bytes (in all spaces).
     */
    @GridMBeanDescription("Total uncompressed swap size for all spaces.")
    public long getTotalUncompressedSize();

    /**
     * Gets current total swap entries size on disk in the given space.
     *
     * @param space Space name.
     * @return Total swap entries size on disk for the given space.
     */
    @GridMBeanDescription("Swap space disk size.")
    public long getDiskSize(String space);

    /**
     * Gets current data size for all values in the space.
     *
     * @param space Space name.
     * @return Total size of data in bytes in the space.
     */
    @GridMBeanDescription("Swap space uncompressed size.")
    public long getUncompressedSize(String space);

    /**
     * Gets current total entries count (in all spaces).
     *
     * @return Total entries count (in all spaces).
     */
    @GridMBeanDescription("Current entries count in all spaces.")
    public long getTotalCount();

    /**
     * Gets current entries count in space with the given name.
     *
     * @param space Name of the space.
     * @return Total entries count in the space.
     */
    @GridMBeanDescription("Current entry count in space.")
    public long getCount(String space);

    /**
     * Gets total data size ever written to swap (to all spaces).
     *
     * @return Total data size ever written (to all spaces).
     */
    @GridMBeanDescription("Total data size ever written to all spaces.")
    public long getTotalStoredSize();

    /**
     * Gets total entries count ever written to swap (to all spaces).
     *
     * @return Total entries count ever written (to all spaces).
     */
    @GridMBeanDescription("Total entry count ever written to all spaces.")
    public long getTotalStoredCount();

    /**
     * Prints space stats to log with INFO level.
     */
    @GridMBeanDescription("Prints space stats to log with INFO level.")
    public void printSpacesStats();
}
