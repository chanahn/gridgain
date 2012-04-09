// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace.leveldb;

import org.fusesource.leveldbjni.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.iq80.leveldb.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridEventType.*;

/**
 * LevelDB-based implementation of swap space SPI.
 * <p>
 * LevelDB is a fast key-value storage library written at Google, for more information see
 * <a href="http://code.google.com/p/leveldb/">LevelDB Project</a>.
 * <p>
 * Key-value pairs are stored in LevelDB instances on disk. Different swap eviction policies
 * exist for removing unused data from storage whenever storage exceeds pre-configured maximum limit.
 * The following eviction policies are available:
 * <ul>
 *  <li>
 *      {@link GridLevelDbEvictPolicy#EVICT_DISABLED} - default policy. This is the most light-weight
 *      policy which allows for unlimited growth of swap space. When using this policy swap should
 *      be cleaned manually (e.g. during off-peak hours) to prevent unlimited growth.
 *  </li>
 *  <li>
 *      {@link GridLevelDbEvictPolicy#EVICT_OPTIMIZED_SMALL} - with this policy the internal data structures
 *      are optimized for cases when stored values are small.
 *  </li>
 *  <li>
 *      {@link GridLevelDbEvictPolicy#EVICT_OPTIMIZED_LARGE} - with this policy the internal data structures
 *      are optimized for cases when stored values are large.
 *  </li>
 * </ul>
 * <p>
 * Separate directories for LevelDB instances are created for each space under root folder configured via
 * {@link #setRootFolderPath(String)}. Spaces (and their directory structure) are initialized when the space
 * is first used.
 * <p>
 * Every space has a name and when used in combination with data grid, space name represents the actual
 * cache name associated with this swap space. Default name is {@code null} which is represented by
 * {@link #DFLT_SPACE_NAME}.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional SPI configuration.</h2>
 * <ul>
 *     <li>Root folder path (see {@link #setRootFolderPath(String)}).</li>
 *     <li>Root folder index range (see {@link #setRootFolderIndexRange(int)}).</li>
 *     <li>Persistent (see {@link #setPersistent(boolean)}).</li>
 *     <li>Max swap count (see {@link #setDefaultMaxSwapCount(long)}).</li>
 *     <li>Max swap count map (see {@link #setMaxSwapCounts(Map)}).</li>
 *     <li>Count overflow ratio (see {@link #setEvictOverflowRatio(double)}).</li>
 *     <li>Eviction session frequency in milliseconds (see {@link #setEvictFrequency(long)}).</li>
 *     <li>Eviction policy (see {@link #setEvictPolicy(GridLevelDbEvictPolicy)}).</li>
 * </ul>
 * <h2 class="header">Optional LevelDB parameters configuration.</h2>
 * For LevelDB configuration parameters explanation see <a href="http://leveldb.googlecode.com/svn/trunk/doc/index.html">LevelDB documentation</a>.
 * <ul>
 *     <li>LevelDb cache size (see {@link #setLevelDbCacheSize(long)}).</li>
 *     <li>LevelDB write buffer size (see {@link #setLevelDbWriteBufferSize(int)}).</li>
 *     <li>LevelDB block size (see {@link #setLevelDbBlockSize(int)}).</li>
 *     <li>LevelDB block paranoid checksums flag (see {@link #setLevelDbParanoidChecks(boolean)}).</li>
 *     <li>LevelDB block verify checksums flag (see {@link #setLevelDbVerifyChecksums(boolean)}).</li>
 *     </ul>
 * <h2 class="header">Java Example</h2>
 * GridLevelDbSwapSpaceSpi is configured by default and should be explicitly configured
 * only if some SPI configuration parameters need to be overridden.
 * <pre name="code" class="java">
 * GridLevelDbSwapSpaceSpi spi = new GridLevelDbSwapSpaceSpi();
 *
 * // Configure root folder path.
 * spi.setRootFolderPath("/path/to/swap/folder");
 *
 * // Set eviction policy.
 * spi.setEvictPolicy(GridLevelDbEvictPolicy.EVICT_OPTIMIZED_SMALL);
 *
 * GridConfigurationAdapter cfg = new GridConfigurationAdapter();
 *
 * // Override default swap space SPI.
 * cfg.setSwapSpaceSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridLevelDbSwapSpaceSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id=&quot;grid.cfg&quot; class=&quot;org.gridgain.grid.GridConfigurationAdapter&quot; scope=&quot;singleton&quot;&gt;
 *     ...
 *     &lt;property name=&quot;swapSpaceSpi&quot;&gt;
 *         &lt;bean class=&quot;org.gridgain.grid.spi.swapspace.leveldb.GridLevelDbSwapSpaceSpi&quot;&gt;
 *             &lt;property name=&quot;rootFolderPath&quot; value=&quot;/path/to/swap/folder&quot;/&gt;
 *             &lt;property name=&quot;evictPolicy&quot; value=&quot;EVICT_OPTIMIZED_SMALL&quot;/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 * @see GridSwapSpaceSpi
 */
@GridSpiInfo(
    author = "GridGain Systems",
    url = "www.gridgain.com",
    email = "support@gridgain.com",
    version = "4.0.1c.09042012")
@GridSpiMultipleInstancesSupport(true)
public class GridLevelDbSwapSpaceSpi extends GridSpiAdapter implements GridSwapSpaceSpi, GridLevelDbSwapSpaceSpiMBean {
    /** Name for default (or {@code null}) space. */
    public static final String DFLT_SPACE_NAME = "gg-dflt-space";

    /**
     * Default directory path for swap files location. Grid name, node ID and
     * index (only if necessary) will be appended to this path using dashes as
     * separators.
     * <p>
     * If {@link #setRootFolderPath(String)} is not configured and {@code GRIDGAIN_HOME}
     * system property is set, this folder will be created under {@code GRIDGAIN_HOME}.
     * <p>
     * If {@link #setRootFolderPath(String)} is not configured and {@code GRIDGAIN_HOME}
     * system property is not set, this folder will be created under {@code java.io.tmpdir}.
     */
    public static final String DFLT_ROOT_FOLDER_PATH = "work/swapspace/leveldb";

    /** File to get lock on when SPI starts to ensure exclusive access. */
    public static final String LOCK_FILE_NAME = "swap-lock";

    /** Default max root folder index. */
    public static final int DFLT_ROOT_FOLDER_IDX_RANGE = 100;

    /** Default maximum entries count for all spaces. */
    public static final long DFLT_MAX_SWAP_CNT = Integer.MAX_VALUE;

    /** Default size/count overflow ratio. */
    public static final double DFLT_OVERFLOW_RATIO = 1.3;

    /** Default value for persistent flag. */
    public static final boolean DFLT_PERSISTENT = false;

    /** Default frequency for evict. */
    public static final long DFLT_EVICT_FREQ = 1000;

    /** Default evict policy. */
    public static final GridLevelDbEvictPolicy DFLT_EVICT_POLICY = GridLevelDbEvictPolicy.EVICT_DISABLED;

    /** Default LevelDB cache size. */
    public static final long DFLT_LEVELDB_CACHE_SIZE = 120 * 1024 * 1024;

    /** Default LevelDB cache size. */
    public static final int DFLT_LEVELDB_WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

    /** Default LevelDB paranoid checks flag. */
    public static final boolean DFLT_LEVELDB_PARANOID_CHECKS = false;

    /** Default LevelDB verify checksums flag. */
    public static final boolean DFLT_LEVELDB_VERIFY_CHECKSUMS = false;

    /** Default LevelDB block size. */
    public static final int DFLT_LEVELDB_BLOCK_SIZE = 4096;

    /** Total size. */
    private final AtomicLong totalSize = new AtomicLong();

    /** Total count. */
    private final AtomicLong totalCnt = new AtomicLong();

    /** Total stored data size. */
    private final AtomicLong totalStoredSize = new AtomicLong();

    /** Total stored items count. */
    private final AtomicLong totalStoredCnt = new AtomicLong();

    /** Root folder file. */
    private File rootFolder;

    /** Spaces. */
    private final ConcurrentMap<String, GridLevelDbSpace> spaces = new ConcurrentHashMap<String, GridLevelDbSpace>();

    /** Spaces initialization mutex to avoid creation of database for spaces with same name in several threads. */
    private final Object spacesInitMux = new Object();

    /** Grid name. */
    @GridNameResource
    private String gridName;

    /** Local node ID. */
    @GridLocalNodeIdResource
    private UUID locNodeId;

    /** Marshaller. */
    @GridMarshallerResource
    private GridMarshaller marsh;

    /** Grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Root folder */
    private String rootFolderPath;

    /** File to lock with. */
    private RandomAccessFile rootFolderLockFile;

    /** Lock to ensure exclusive access. */
    private FileLock rootFolderLock;

    /** Root folder range. */
    private int rootFolderIdxRange = DFLT_ROOT_FOLDER_IDX_RANGE;

    /** Workers. */
    private final Collection<GridSpiThread> workers = new LinkedList<GridSpiThread>();

    /** Listener. */
    private volatile GridSwapSpaceSpiListener lsnr;

    /** SPI stopping flag. */
    private volatile boolean spiStopping;

    /** Evict thread check period. */
    private long evictFreq = DFLT_EVICT_FREQ;

    /** Evict policy. */
    private GridLevelDbEvictPolicy evictPlc = DFLT_EVICT_POLICY;

    /** Maximum entries count for all spaces. */
    private long dfltMaxSwapCnt = DFLT_MAX_SWAP_CNT;

    /** Persistent flag. */
    private boolean persistent = DFLT_PERSISTENT;

    /** Count overflow ratio. */
    private double evictOverflowRatio = DFLT_OVERFLOW_RATIO;

    /** LevelDB cache size. */
    private long levelDbCacheSize = DFLT_LEVELDB_CACHE_SIZE;

    /** LevelDB write buffer size. */
    private int levelDbWriteBufSize = DFLT_LEVELDB_WRITE_BUFFER_SIZE;

    /** LevelDB paranoid checks flag. */
    private boolean levelDbParanoidChecks = DFLT_LEVELDB_PARANOID_CHECKS;

    /** LevelDB verify checksums flag. */
    private boolean levelDbVerifyChecksums = DFLT_LEVELDB_VERIFY_CHECKSUMS;

    /** LevelDB verify block size. */
    private int levelDbBlockSize = DFLT_LEVELDB_BLOCK_SIZE;

    /** Maximum entries count for individual spaces. */
    private Map<String, Long> maxSwapCnts;

    /** Lock used in wakeup mechanism. */
    private final Lock wakeUpLock = new ReentrantLock();

    /** Condition used in wakeup mechanism. */
    private final Condition wakeUpCond = wakeUpLock.newCondition();

    /** {@inheritDoc} */
    @Override public String getMaxSwapCountsFormatted() {
        if (maxSwapCnts == null)
            return "";

        StringBuilder sb = new StringBuilder();

        for(Map.Entry<String, Long> entry : maxSwapCnts.entrySet()) {
            if (sb.length() > 0)
                sb.append(", ");

            sb.append(entry.getKey());
            sb.append(" = ");
            sb.append(entry.getValue());
        }

        return sb.toString();
    }

    /**
     * Sets maximum count (number of entries) for individual spaces.
     * <p>
     * If map is set and contains value for some space, this value takes
     * precedence over {@link #getDefaultMaxSwapCount()}}.
     * <p>
     * Map should accept {@code null} keys.
     *
     * @param maxSwapCnts Maximum count for individual swap spaces.
     */
    @GridSpiConfiguration(optional = true)
    public void setMaxSwapCounts(Map<String, Long> maxSwapCnts) {
        this.maxSwapCnts = maxSwapCnts;
    }

    /** {@inheritDoc} */
    @GridSpiConfiguration(optional = true)
    @Override public double getEvictOverflowRatio() {
        return evictOverflowRatio;
    }

    /**
     * Sets overflow ratio for swap evictions. Evictions will start once swap space
     * overgrows the maximum allowed entry count by this ratio.
     *
     * @param evictOverflowRatio Value to set.
     */
    @GridSpiConfiguration(optional = true)
    public void setEvictOverflowRatio(double evictOverflowRatio) {
        this.evictOverflowRatio = evictOverflowRatio;
    }

    /** {@inheritDoc} */
    @Override public boolean isPersistent() {
        return persistent;
    }

    /**
     * Sets persistent flag to control whether swap should survive system restarts.
     *
     * @param persistent Persistent flag.
     */
    @GridSpiConfiguration(optional = true)
    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    /** {@inheritDoc} */
    @Override public long getEvictFrequency() {
        return evictFreq;
    }

    /**
     * Sets swap eviction frequency (in milliseconds). Swap eviction is a process of evicting
     * unused swap entries whenever swap grows too large.
     *
     * @param evictFreq Eviction frequency.
     */
    @GridSpiConfiguration(optional = true)
    public void setEvictFrequency(long evictFreq) {
        this.evictFreq = evictFreq;
    }

    /** {@inheritDoc} */
    @Override public String getEvictPolicyFormatted() {
        return evictPlc.toString();
    }

    /**
     * Sets evict policy. Swap eviction is a process of evicting
     * unused swap entries whenever swap grows too large.
     *
     * @param evictPlc Value to set.
     */
    @GridSpiConfiguration(optional = true)
    public void setEvictPolicy(GridLevelDbEvictPolicy evictPlc) {
        this.evictPlc = evictPlc;
    }

    /** {@inheritDoc} */
    @Override public long getDefaultMaxSwapCount() {
        return dfltMaxSwapCnt;
    }

    /**
     * Sets default max swap count.
     *
     * @param dfltMaxSwapCnt Value to set.
     */
    @GridSpiConfiguration(optional = true)
    public void setDefaultMaxSwapCount(long dfltMaxSwapCnt) {
        this.dfltMaxSwapCnt = dfltMaxSwapCnt;
    }

    /** {@inheritDoc} */
    @Override public String getRootFolderPath() {
        return rootFolderPath != null ? rootFolderPath : root().getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override public int getRootFolderIndexRange() {
        return rootFolderIdxRange;
    }

    /**
     * Sets root folder index range value.
     * <p>
     * If {@link #setRootFolderPath(String)} is not set and default path
     * is locked (e.g. by other grid running on the same host) SPI will
     * try next folder appending index to {@code GRIDGAIN_HOME}/{@link #DFLT_ROOT_FOLDER_PATH}.
     * If {@link #setRootFolderPath(String)} is set, SPI tries to lock configured path only and
     * this parameter is ignored.
     * <p>
     * If not provided, default value is {@link #DFLT_ROOT_FOLDER_IDX_RANGE}.
     *
     * @param rootFolderIdxRange Root folder index range.
     */
    @GridSpiConfiguration(optional = true)
    public void setRootFolderIndexRange(int rootFolderIdxRange) {
        this.rootFolderIdxRange = rootFolderIdxRange;
    }

    /**
     * Sets path to a directory where swap space values will be stored. The
     * path can either be absolute or relative to {@code GRIDGAIN_HOME} system
     * or environment variable.
     * <p>
     * If not provided, default value is {@link #DFLT_ROOT_FOLDER_PATH}.
     *
     * @param rootFolderPath Absolute or GridGain installation home folder relative path
     *      where swap space values will be stored.
     */
    @GridSpiConfiguration(optional = true)
    public void setRootFolderPath(String rootFolderPath) {
        this.rootFolderPath = rootFolderPath;
    }

    /**
     * Sets in-memory cache size for LevelDB. For more information on LevelDB configuration
     * see <a href="http://leveldb.googlecode.com/svn/trunk/doc/index.html">LevelDB documentation</a>.
     *
     * @param levelDbCacheSize LevelDB cache size.
     */
    @GridSpiConfiguration(optional = true)
    public void setLevelDbCacheSize(long levelDbCacheSize) {
        this.levelDbCacheSize = levelDbCacheSize;
    }

    /**
     * Sets LevelDB write buffer size. For more information on LevelDB configuration
     * see <a href="http://leveldb.googlecode.com/svn/trunk/doc/index.html">LevelDB documentation</a>.
     *
     * @param levelDbWriteBufSize LevelDB write buffer size.
     */
    @GridSpiConfiguration(optional = true)
    public void setLevelDbWriteBufferSize(int levelDbWriteBufSize) {
        this.levelDbWriteBufSize = levelDbWriteBufSize;
    }

    /**
     * Sets LevelDB paranoid checks. For more information on LevelDB configuration
     * see <a href="http://leveldb.googlecode.com/svn/trunk/doc/index.html">LevelDB documentation</a>.
     *
     * @param levelDbParanoidChecks  LevelDB paranoid checks flag.
     */
    @GridSpiConfiguration(optional = true)
    public void setLevelDbParanoidChecks(boolean levelDbParanoidChecks) {
        this.levelDbParanoidChecks = levelDbParanoidChecks;
    }

    /**
     * Sets flag to enable checksum verification for LevelDB. For more information on LevelDB configuration
     * see <a href="http://leveldb.googlecode.com/svn/trunk/doc/index.html">LevelDB documentation</a>.
     *
     * @param levelDbVerifyChecksums LevelDB verify checksums flag.
     */
    @GridSpiConfiguration(optional = true)
    public void setLevelDbVerifyChecksums(boolean levelDbVerifyChecksums) {
        this.levelDbVerifyChecksums = levelDbVerifyChecksums;
    }

    /**
     * Sets LevelDB block size. For more information on LevelDB configuration
     * see <a href="http://leveldb.googlecode.com/svn/trunk/doc/index.html">LevelDB documentation</a>.
     *
     * @param levelDbBlockSize LevelDB block size.
     */
    @GridSpiConfiguration(optional = true)
    public void setLevelDbBlockSize(int levelDbBlockSize) {
        this.levelDbBlockSize = levelDbBlockSize;
    }

    /**
     * Returns key to byte array. Key data itself is padded with partition info.
     *
     * @param key GridSwapKey key to convert to bytes.
     * @throws GridSpiException in case of marshaling errors.
     * @return Byte array.
     */
    private byte[] dbKeyBytes(GridSwapKey key) throws GridSpiException {
        try {
            byte[] keyBytes = key.keyBytes() != null ? key.keyBytes() : U.marshal(marsh, key.key()).entireArray();

            return U.join(U.intToBytes(key.partition()), keyBytes);
        }
        catch (GridException e) {
            throw new GridSpiException("Error marshaling key: " + key, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void printSpacesStats() {
        if (log.isInfoEnabled()) {
            for (GridLevelDbSpace space : spaces.values())
                log.info("Space stats: " + space);
        }
    }

    /** {@inheritDoc} */
    @Override public long getTotalStoredSize() {
        return totalStoredSize.get();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStoredCount() {
        return totalStoredCnt.get();
    }

    /** {@inheritDoc} */
    @Override public long getTotalDiskSize() {
        int size = 0;

        for(GridLevelDbSpace sp : spaces.values())
            size += sp.diskSize();

        return size;
    }

    /** {@inheritDoc} */
    @Override public long getTotalUncompressedSize() {
        if (evictPlc != GridLevelDbEvictPolicy.EVICT_DISABLED)
            return totalSize.get();

        int size = 0;

        for(GridLevelDbSpace space : spaces.values())
            size += space.uncompressedSize();

        return size;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSize(String space) {
        try {
            GridLevelDbSpace sp = space(space, false);

            return sp == null ? 0 : sp.diskSize();
        }
        catch (GridSpiException ignored) {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override public long getUncompressedSize(@Nullable String space) {
        try {
            return size(space);
        }
        catch(GridException ignored) {
                return 0;
        }
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String space) throws GridSpiException {
        GridLevelDbSpace sp = space(space, false);

        return sp == null ? 0 : sp.uncompressedSize();
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String space) throws GridSpiException {
        GridLevelDbSpace sp = space(space, false);

        return sp == null ? 0 : sp.count();
    }

    /** {@inheritDoc} */
    @Override public long getCount(@Nullable String space) {
        try {
            return count(space);
        }
        catch(GridException ignored) {
            return 0;
        }
    }

    /**
     * Method to update global SPI counters from other classes.
     *
     * @param addCnt Addition to global count (may be negative).
     * @param addSize Addition to global size (may be negative).
     */
    void updateCounters(long addCnt, long addSize) {
        totalSize.addAndGet(addSize);
        totalCnt.addAndGet(addCnt);

        if (addSize > 0)
            totalStoredSize.addAndGet(addSize);

        if (addCnt > 0)
            totalStoredCnt.addAndGet(addCnt);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable final String gridName) throws GridSpiException {
        startStopwatch();

        spiStopping = false;

        if (rootFolderPath == null)
            assertParameter(rootFolderIdxRange > 0, "rootFolderIdxRange > 0");

        initRootFolder();

        registerMBean(gridName, this, GridLevelDbSwapSpaceSpiMBean.class);

        if (log.isDebugEnabled()) {
            log.debug(configInfo("rootFolderPath", getRootFolderPath()));

            log.debug(startInfo());
        }
    }

    /**
     * Stops SPI.
     *
     * @throws GridSpiException In case of errors.
     */
    @Override public void spiStop() throws GridSpiException {
        spiStopping = true;

        unregisterMBean();

        for(GridLevelDbSpace space: spaces.values())
            space.waitLocks();

        for(GridLevelDbSpace space: spaces.values())
            space.close();

        spaces.clear();

        U.interrupt(workers);

        U.joinThreads(workers, log);

        U.releaseQuiet(rootFolderLock);

        U.closeQuiet(rootFolderLockFile);

        if (!persistent)
            U.delete(root());

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param name Name.
     * @return Space (created, if needed and reserved).
     * @throws GridSpiException If failed.
     */
    private GridLevelDbSpace space(@Nullable String name) throws GridSpiException {
        GridLevelDbSpace space = space(name, true);

        assert space != null;

        return space;
    }

    /**
     * @param name Name.
     * @param create Create flag.
     * @return Space.
     * @throws GridSpiException If failed.
     */
    @Nullable private GridLevelDbSpace space(@Nullable String name, boolean create) throws GridSpiException {
        String maskedName = maskNull(name);

        GridLevelDbSpace space = spaces.get(maskedName);

        if (space != null || !create)
            return space;

        synchronized (spacesInitMux) {
            space = spaces.get(maskedName);

            if (space != null)
                return space;

            space = initSpace(maskedName);

            return space;
        }
    }

    /**
     * Method to create and init space based on settings.
     *
     * @param name Space name.
     * @return Initialized space.
     * @throws GridSpiException In case of error.
     */
    private GridLevelDbSpace initSpace(String name) throws GridSpiException {
        GridLevelDbSpace space = null;

        switch (evictPlc) {
            case EVICT_DISABLED:
                space = new GridLevelDbEvictDisabledSpace(name);

                break;
            case EVICT_OPTIMIZED_SMALL:
                space = new GridLevelDbOptimizedSmallSpace(name);

                break;
            case EVICT_OPTIMIZED_LARGE:
                space = new GridLevelDbOptimizedLargeSpace(name);
        }

        space.init(persistent);

        if (space instanceof GridLevelDbEvictSpace) {
            GridSpiThread wrk = new SpaceEvictThread((GridLevelDbEvictSpace)space);

            workers.add(wrk);

            wrk.start();
        }

        spaces.put(name, space);

        return space;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"TooBroadScope"})
    @Override public void store(@Nullable String spaceName, GridSwapKey key, @Nullable byte[] val,
        GridSwapContext ctx) throws GridSpiException {
        assert key != null;
        assert ctx != null;

        GridLevelDbSpace space = space(spaceName);

        byte[] keyBytes = dbKeyBytes(key);

        space.put(keyBytes, val);

        notifySwapManager(EVT_SWAP_SPACE_DATA_STORED, spaceName, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public void storeAll(@Nullable String spaceName, Map<GridSwapKey, byte[]> pairs,
        GridSwapContext ctx)
        throws GridSpiException {
        assert pairs != null;
        assert ctx != null;

        GridLevelDbSpace space = space(spaceName);

        for (Map.Entry<GridSwapKey, byte[]> entry : pairs.entrySet()) {
            byte[] keyBytes = dbKeyBytes(entry.getKey());

            space.put(keyBytes, entry.getValue());

            notifySwapManager(EVT_SWAP_SPACE_DATA_STORED, spaceName, keyBytes);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] read(@Nullable String spaceName, GridSwapKey key, GridSwapContext ctx)
        throws GridSpiException {
        assert key != null;
        assert ctx != null;

        GridLevelDbSpace space = space(spaceName);

        byte[] keyBytes = dbKeyBytes(key);

        byte[] val = space.get(keyBytes);

        notifySwapManager(EVT_SWAP_SPACE_DATA_READ, spaceName, keyBytes);

        if (log.isDebugEnabled())
            if (val != null)
                log.debug("Entry was read from cache [space=" + spaceName + ", key=" + key + ']');
            else
                log.debug("Entry was not found [space=" + spaceName + ", key=" + key + ']');

        return val;
    }

    /** {@inheritDoc} */
    @Override public Map<GridSwapKey, byte[]> readAll(String spaceName, Iterable<GridSwapKey> keys,
        GridSwapContext ctx) throws GridSpiException {
        assert keys != null;
        assert ctx != null;

        Map<GridSwapKey, byte[]> res = new HashMap<GridSwapKey, byte[]>();

        for (GridSwapKey key : keys)
            res.put(key, read(spaceName, key, ctx));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, GridSwapKey key,
        @Nullable GridInClosure<byte[]> c, GridSwapContext ctx) throws GridSpiException {
        assert key != null;
        assert ctx != null;

        GridLevelDbSpace space = space(spaceName);

        byte[] keyBytes = dbKeyBytes(key);
        
        if(c != null)
            c.apply(space.get(keyBytes));

        space.delete(keyBytes);

        notifySwapManager(EVT_SWAP_SPACE_DATA_REMOVED, spaceName, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable String spaceName, Collection<GridSwapKey> keys,
        @Nullable final GridInClosure2<GridSwapKey, byte[]> c, GridSwapContext ctx) throws GridSpiException {
        assert keys != null;
        assert ctx != null;

        for (final GridSwapKey key : keys) {
            CI1<byte[]> c1 = null;

            if (c != null) {
                c1 = new CI1<byte[]>() {
                    @Override public void apply(byte[] val) {
                        c.apply(key, val);
                    }
                };
            }

            remove(spaceName, key, c1, ctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(@Nullable String spaceName) throws GridSpiException {
        GridLevelDbSpace space = space(maskNull(spaceName), false);

        if (space == null) {
            if (log.isDebugEnabled())
                log.debug("Clear cancelled (unknown space): " + spaceName);

            return;
        }

        space.clear();
    }

    /** {@inheritDoc} */
    @Override public long getTotalCount() {
        if (evictPlc != GridLevelDbEvictPolicy.EVICT_DISABLED)
            return totalCnt.get();

        int cnt = 0;

        for(GridLevelDbSpace space : spaces.values())
            cnt += space.count();

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public void setListener(GridSwapSpaceSpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> partitions(@Nullable String spaceName) throws GridSpiException {
        GridLevelDbSpace space = space(spaceName, false);

        return space == null ? null : space.partitions();
    }

    /**
     * @param evtType Event type.
     * @param spaceName Space name.
     * @param keyBytes Key bytes (for eviction notification only).
     */
    void notifySwapManager(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes) {
        GridSwapSpaceSpiListener evtLsnr = lsnr;

        byte[] keyBytesNoPart = null;

        if (keyBytes != null)
            keyBytesNoPart = Arrays.copyOfRange(keyBytes, 4, keyBytes.length);

        if (evtLsnr != null)
            evtLsnr.onSwapEvent(evtType, spaceName, keyBytesNoPart);
    }

    /**
     * @param name Name.
     * @return Masked name.
     * @throws GridSpiException If space name is invalid.
     */
    private String maskNull(String name) throws GridSpiException {
        if (name == null)
            return DFLT_SPACE_NAME;

        else if (name.isEmpty())
            throw new GridSpiException("Space name cannot be empty: " + name);

        else if (DFLT_SPACE_NAME.equalsIgnoreCase(name))
            throw new GridSpiException("Space name is reserved for default space: " + name);

        else if (name.contains("/") || name.contains("\\"))
            throw new GridSpiException("Space name contains invalid characters: " + name);

        return name;
    }

    /**
     * @throws GridSpiException If failed.
     */
    private void initRootFolder() throws GridSpiException {
        if (rootFolderPath == null) {
            String path = DFLT_ROOT_FOLDER_PATH + "-" + gridName + "-" + locNodeId;

            for (int i = 0; i <= rootFolderIdxRange; i++) {
                try {
                    tryInitRootFolder(path + (i > 0 ? "-" + i : ""));

                    // Successful init.
                    break;
                }
                catch (GridSpiException e) {
                    if (i == rootFolderIdxRange)
                        // No more attempts left.
                        throw e;

                    if (log.isDebugEnabled())
                        log.debug("Failed to initialize root folder [path=" + rootFolderPath +
                            ", err=" + e.getMessage() + ']');
                }
            }
        }
        else
            tryInitRootFolder(rootFolderPath);

        if (log.isDebugEnabled())
            log.debug("Initialized root folder: " + root().getAbsolutePath());
    }

    /**
     * @param path Path.
     * @throws GridSpiException If failed.
     */
    private void tryInitRootFolder(String path) throws GridSpiException {
        assert path != null;

        checkOrCreateRootFolder(path);

        lockRootFolder();
    }

    /**
     * @param path Root folder path.
     * @throws GridSpiException If failed.
     */
    private void checkOrCreateRootFolder(String path) throws GridSpiException {
        root(new File(path));

        if (root().isAbsolute()) {
            if (!U.mkdirs(root()))
                throw new GridSpiException("Swap space directory does not exist and cannot be created: " +
                    root());
        }
        else if (!F.isEmpty(getGridGainHome())) {
            // Create relative by default.
            root(new File(getGridGainHome(), path));

            if (!U.mkdirs(root()))
                throw new GridSpiException("Swap space directory does not exist and cannot be created: " +
                    root());
        }
        else {
            String tmpDirPath = System.getProperty("java.io.tmpdir");

            if (tmpDirPath == null)
                throw new GridSpiException("Failed to initialize swap space directory " +
                    "with unknown GRIDGAIN_HOME (system property 'java.io.tmpdir' does not exist).");

            root(new File(tmpDirPath, path));
        }

        if (!persistent) {
            if (root().exists() && !U.delete(root()))
                throw new GridSpiException("Failed to clear swap space directory: " + root());

            if (!U.mkdirs(root()))
                throw new GridSpiException("Failed to initialize swap space directory: " + root());
        }
        else {
            if (!root().isDirectory())
                throw new GridSpiException("Swap space directory path does not represent a valid directory: " + root());

            if (!root().canRead() || !root().canWrite())
                throw new GridSpiException("Can not write or read from swap space directory: " + root());

            if (persistent) {
                if (log.isDebugEnabled())
                    log.debug("Initializing persisted spaces.");

                for (File f : root().listFiles()) {
                    if (!f.isDirectory())
                        continue;

                    initSpace(f.getName());
                }
            }
        }
    }

    /**
     * @throws GridSpiException If failed.
     */
    private void lockRootFolder() throws GridSpiException {
        assert root() != null;

        File lockFile = new File(root(), LOCK_FILE_NAME);

        boolean err = true;

        try {
            rootFolderLockFile = new RandomAccessFile(lockFile, "rw");

            /* Lock to ensure exclusive access. */
            rootFolderLock = rootFolderLockFile.getChannel().tryLock(0, Long.MAX_VALUE, false);

            if (rootFolderLock == null)
                throw new GridSpiException("Failed to get exclusive lock on lock-file: " + lockFile);

            err = false;

            if (log.isDebugEnabled())
                log.debug("Successfully locked on: " + lockFile);
        }
        catch (FileNotFoundException e) {
            throw new GridSpiException("Failed to create lock-file: " + lockFile, e);
        }
        catch (IOException e) {
            throw new GridSpiException("Failed to get exclusive lock on lock-file: " + lockFile, e);
        }
        catch (OverlappingFileLockException e) {
            throw new GridSpiException("Failed to get exclusive lock on lock-file: " + lockFile, e);
        }
        finally {
            if (err)
                U.closeQuiet(rootFolderLockFile);
        }
    }

    /**
     * Folder for {@link #rootFolderPath}.
     *
     * @return Root folder where SPI data is stored.
     */
    File root() {
        return rootFolder;
    }

    /**
     * Setter for folder for {@link #rootFolderPath}.
     *
     * @param rootFolder new value.
     */
    void root(File rootFolder) {
        this.rootFolder = rootFolder;
    }

    /**
     * Thread that runs evictions on a space.
     */
    private class SpaceEvictThread extends GridSpiThread {
        /**
         * Space to work on.
         */
        private GridLevelDbEvictSpace space;

        /**
         * Creates eviction thread that will watch specified {@code space}.
         *
         * @param space Space to watch.
         */
        private SpaceEvictThread(GridLevelDbEvictSpace space) {
            super(gridName, "leveldb-swap-space-worker", log);
            this.space = space;
        }

        /**
         * Eviction thread body
         *
         * @throws InterruptedException In case of errors.
         */
        @Override protected void body() throws InterruptedException {
            while (!spiStopping) {
                waitForTasks();

                if (spiStopping)
                    return;

                long maxCnt = max(maxSwapCnts, dfltMaxSwapCnt);

                long evictCnt = 0;

                if ((maxCnt > 0) && (space.count() > maxCnt * evictOverflowRatio))
                    evictCnt = space.count() - maxCnt;

                if (evictCnt > 0)
                    space.evictSafe(evictCnt);
            }
        }

        /**
         *
         * @param map Map to get from per-space settings.
         * @param dflt Default global value to return in case per-space is missing.
         * @return Max value for this space from map or 0 if not found.
         */
        private long max(Map<String, Long> map, long dflt) {
            if (map != null) {
                Long max = map.get(space.name());

                if (max != null)
                    return max;
            }

            return dflt;
        }

        /**
         * Waits for wakeup lock notification or until timeout expires.
         *
         * @throws InterruptedException If waiting thread was interrupted.
         */
        private void waitForTasks() throws InterruptedException {
            wakeUpLock.lock();

            try {
                wakeUpCond.await(evictFreq, TimeUnit.MILLISECONDS);
            }
            finally {
                wakeUpLock.unlock();
            }
        }
    }

    /**
     * Class that holds space info and has some space operations like put.
     */
    private abstract class GridLevelDbSpace {
        /** Space name, set in constructor. */
        protected final String name;

        /** Space partitions storage. */
        @GridToStringInclude
        protected final Collection<Integer> parts = new HashSet<Integer>();

        /** Folder to hold space data. */
        @GridToStringExclude
        protected File spaceFolder;

        /** Total size of data (values) stored in space. */
        protected final AtomicLong size = new AtomicLong();

        /** Number of data stored in space. */
        protected final AtomicLong cnt = new AtomicLong();

        /**
         * @param name Space name.
         */
        GridLevelDbSpace(String name) {
            this.name = name;
        }

        /**
         * @param spiStart {@code True} if init is invoked on SPI start (persistent swap).
         * @throws GridSpiException If failed.
         */
        abstract void init(boolean spiStart) throws GridSpiException;

        /**
         * Stores key-value pair to space. If value is null, key is in fact deleted.
         *
         * @param key Data key.
         * @param val Data value.
         */
        abstract void put(byte[] key, byte[] val);

        /**
         * Similar to LevelDB get.
         * @param key Key bytes.
         * @return Fetched value or null if entry was not found.
         */
        abstract byte[] get(byte[] key);

        /**
         * Similar to LevelDB delete, but also performs some statistics accounting.
         * @param key Key bytes.
         */
        abstract void delete(byte[] key);

        /**
         * Closes space and clears the resources.
         */
        abstract void close();

        /**
         * @return LevelDB writeOptions based on SPI configuration.
         */
        protected Options writeOptions() {
            Options options = new Options().createIfMissing(true);

            options.cacheSize(levelDbCacheSize);
            options.writeBufferSize(levelDbWriteBufSize);
            options.paranoidChecks(levelDbParanoidChecks);
            options.verifyChecksums(levelDbVerifyChecksums);
            options.blockSize(levelDbBlockSize);

            return options;
        }

        /**
         * Clears database.
         *
         * @throws GridSpiException if error happens.
         */
        public void clear() throws GridSpiException {
            updateCnt(-cnt.get(), -size.get());

            cnt.set(0);
            size.set(0);

            close();

            U.delete(spaceFolder);

            init(false);

            notifySwapManager(EVT_SWAP_SPACE_CLEARED, name, null);
        }

        /**
         * @return Space name.
         */
        String name() {
            return name;
        }

        /**
         * @param addSize Size in bytes.
         * @param addCnt Count.
         */
        protected void updateCnt(long addCnt, long addSize) {
            cnt.addAndGet(addCnt);
            size.addAndGet(addSize);

            // Update global counters as well.
            updateCounters(addCnt, addSize);
        }

        /**
         * @return Size in bytes.
         */
        protected long uncompressedSize() {
            return size.get();
        }

        /**
         * @return Size of swap space files in bytes.
         */
        protected long diskSize() {
            return diskSize(spaceFolder);
        }

        /**
         * Calculates directory size recursively.
         *
         * @param f File or directory to traverse.
         * @return Sum of sizes in bytes.
         */
        private long diskSize(File f) {
            if(f.isFile())
                return f.length();

            int size = 0;

            for(File child : f.listFiles())
                size += diskSize(child);

            return size;
        }

        /**
         * @return Entries count.
         */
        protected long count() {
            return cnt.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridLevelDbSpace.class, this);
        }

        /**
         * Method to be called when stopping space to make sure that nobody holds lock.
         */
        public void waitLocks() {
            // No-op.
        }

        /**
         * @return Collection of partitions in this space.
         */
        public Collection<Integer> partitions() {
            return new ArrayList<Integer>(parts);
        }
    }

    /**
     * LevelDB based space to store data that is a simple wrapper for db methods without eviction.
     *
     * @author 2012 Copyright (C) GridGain Systems
     * @version 4.0.1c.09042012
     */
    private class GridLevelDbEvictDisabledSpace extends GridLevelDbSpace {
        /** */
        private DB db;

        /** */
        private static final String DB_FOLDER = "db";

        /**
         * @param name Space name.
         */
        GridLevelDbEvictDisabledSpace(String name) {
            super(name);
        }

        /** {@inheritDoc} */
        @Override void init(boolean spiStart) throws GridSpiException {
            if (db != null)
                return;

            spaceFolder = new File(root(), name);

            File dbFolder  = new File(spaceFolder, DB_FOLDER);

            if (!U.mkdirs(spaceFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + spaceFolder + ']');

            if (!U.mkdirs(dbFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + dbFolder + ']');

            try {
                db = JniDBFactory.factory.open(dbFolder, writeOptions());
            }
            catch (IOException e) {
                throw new GridSpiException("Failed to create a LevelDB database for space [space=" + name +
                    ", folder=" + spaceFolder + ']', e);
            }

            if (isPersistent()) { // Need to init data from database.
                DBIterator dbIter = db.iterator();

                dbIter.seekToFirst();

                while (dbIter.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = dbIter.next();

                    byte[] key  = entry.getKey();

                    int part = U.bytesToInt(key, 0);

                    parts.add(part);
                }

                dbIter.close();
            }

            if (log.isDebugEnabled())
                log.debug("Space has been initialized: " + this);
        }

        /**
         * Stores key-value pair to space. If value is null, key is in fact deleted.
         *
         * @param key Data key.
         * @param val Data value.
         */
        @Override public void put(byte[] key, byte[] val) {
            if (spiStopping)
                return;

            if (val != null)
                db.put(key, val);
            else
                db.delete(key);

            updateCnt(1, val == null ? 0 : val.length);
        }

        /**
         * Similar to LevelDB get.
         * @param key Key bytes.
         * @return Fetched value or null if entry was not found.
         */
        @Override public byte[] get(byte[] key) {
            if (spiStopping)
                return null;

            return db.get(key);
        }

        /**
         * Similar to LevelDB delete, but also performs some statistics accounting.
         * @param key Key bytes.
         */
        @Override public void delete(byte[] key) {
            if (spiStopping)
                return;

            db.delete(key);
        }

        /** {@inheritDoc} */
        @Override void close() {
            db.close();

            // Init checks for null.
            db = null;

            if (!isPersistent())
                U.delete(spaceFolder);
        }

        @Override protected long uncompressedSize() {
            if (spiStopping)
                return 0;

            ReadOptions readOptions = new ReadOptions();

            readOptions.fillCache(false); // recommended for bulk operations.

            DBIterator iter = db.iterator(readOptions);

            iter.seekToFirst();

            int size = 0;

            while (iter.hasNext() && !spiStopping)
                size += iter.next().getValue().length;

            return size;
        }

        @Override protected long count() {
            if (db == null || spiStopping)
                return 0;

            ReadOptions readOptions = new ReadOptions();

            readOptions.fillCache(false); // recommended for bulk operations.

            DBIterator iter = db.iterator(readOptions);

            iter.seekToFirst();

            int cnt = 0;

            while(iter.hasNext() && !spiStopping) {
                cnt++;

                iter.next();
            }

            iter.close();

            return cnt;
        }
    }

    /**
     * Abstract base class for LevelDB based stores with evictions.
     *
     * @author 2012 Copyright (C) GridGain Systems
     * @version 4.0.1c.09042012
     */
    private abstract class GridLevelDbEvictSpace extends GridLevelDbSpace {
        /** Number of read/write locks to perform database operations. */
        protected static final int LOCKS_CNT = 1024;

        /** Locks to put and retrieve data. */
        protected final ReadWriteLock[] locks = new ReadWriteLock[LOCKS_CNT];

        /** Length of timestamp in bytes. */
        protected static final int TSTAMP_BYTES_IN_META = 4;

        /**
         * @param name Space name.
         */
        GridLevelDbEvictSpace(String name) {
            super(name);

            for (int i = 0; i < locks.length; i++)
                locks[i] = new ReentrantReadWriteLock();
        }

        /**
         * @return Timestamp in seconds (can hold up to 68 years).
         */
        protected int timestamp() {
            return (int)(System.currentTimeMillis() / 1000);
        }

        /**
         * Performs main eviction operations, other methods are wrappers.
         *
         * @param evictCnt Number of entries to evict.
         */
        abstract void evict(long evictCnt);

        /**
         * @param evictCnt Number of entries to evict.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        public void evictSafe(long evictCnt) {
            if (spiStopping)
                return;

            if (evictCnt <= 0)
                return;

            long cntBeforeEvict = cnt.get();

            evict(evictCnt);

            if (cntBeforeEvict < cnt.get()) {
                for(ReadWriteLock lock : locks)
                    lock.writeLock().lock();

                try {
                    evict(evictCnt);
                }
                finally {
                    for(ReadWriteLock lock : locks)
                        lock.writeLock().unlock();
                }
            }
        }

        /**
         * Method to be called when stopping space to make sure that nobody holds lock.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void waitLocks() {
            assert spiStopping;

            for (ReadWriteLock lock : locks) {
                lock.writeLock().lock();
                lock.writeLock().unlock();
            }
        }

        /**
         * Returns ReadWrite lock for a key.
         * @param key Key bytes.
         * @return Lock to use for the key.
         */
        protected ReadWriteLock lock(byte[] key) {
            return locks[Math.abs(Arrays.hashCode(key)) % locks.length];
        }
    }

    /**
     * * LevelDB based space with eviction optimized for case when values are small.
     *
     * @author 2012 Copyright (C) GridGain Systems
     * @version 4.0.1c.09042012
     */
    private class GridLevelDbOptimizedLargeSpace extends GridLevelDbEvictSpace {
        /** Folder to store key->value mapping. */
        private static final String KEY_VAL_DB_FOLDER = "keyval";

        /** Folder to store key->metadata mapping. */
        private static final String KEY_META_DB_FOLDER = "keymeta";

        /** LevelDB to hold real values. */
        @GridToStringExclude
        private DB keyValDb;

        /** LevelDB to store metadata. */
        @GridToStringExclude
        private DB keyMetaDb;

        /** Eviction queue. */
        private Queue<GridTuple2<byte[], Integer>> evictQ = new ConcurrentLinkedQueue<GridTuple2<byte[], Integer>>();

        /**
         * @param name Space name.
         */
        GridLevelDbOptimizedLargeSpace(String name) {
            super(name);
        }

        /** {@inheritDoc} */
        @Override void init(boolean spiStart) throws GridSpiException {
            if (keyMetaDb != null || keyValDb != null)
                return;

            spaceFolder = new File(root(), name);

            File keyValDbFolder  = new File(spaceFolder, KEY_VAL_DB_FOLDER);
            File keyMetaDbFolder = new File(spaceFolder, KEY_META_DB_FOLDER);

            if (!U.mkdirs(spaceFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + spaceFolder + ']');

            if (!U.mkdirs(keyValDbFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + keyValDbFolder + ']');

            if (!U.mkdirs(keyMetaDbFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + keyMetaDbFolder + ']');

            try {
                keyValDb = JniDBFactory.factory.open(keyValDbFolder, writeOptions());
                keyMetaDb = JniDBFactory.factory.open(keyMetaDbFolder, writeOptions());
            }
            catch (IOException e) {
                throw new GridSpiException("Failed to create a LevelDB database for space [space=" + name +
                    ", folder=" + spaceFolder + ']', e);
            }

            if (isPersistent()) { // Need to init data from database.
                DBIterator dbIter = keyMetaDb.iterator();

                dbIter.seekToFirst();

                while (dbIter.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = dbIter.next();

                    byte[] key  = entry.getKey();
                    byte[] meta = entry.getValue();

                    int valSize = U.bytesToInt(meta, TSTAMP_BYTES_IN_META);

                    updateCnt(1, valSize);

                    int part = U.bytesToInt(key, 0);

                    parts.add(part);
                }

                dbIter.close();
            }

            if (log.isDebugEnabled())
                log.debug("Space has been initialized: " + this);
        }

        /**
         * Stores key-value pair to space. If value is null, key is in fact deleted.
         *
         * @param key Data key.
         * @param val Data value.
         */
        @Override public void put(byte[] key, byte[] val) {
            if (spiStopping)
                return;

            int tstamp = timestamp();

            ReadWriteLock lock = lock(key);

            lock.writeLock().lock();

            try {
                if (spiStopping) // Double-check in case thread was waiting on lock too long.
                    return;

                byte[] existingValMeta = keyMetaDb.get(key);

                int addedCnt = 1;
                int addedSize = 0;

                if (existingValMeta != null) {
                    int valSize = U.bytesToInt(existingValMeta, TSTAMP_BYTES_IN_META);

                    addedCnt -= 1;
                    addedSize -= valSize;

                    keyValDb.delete(U.join(existingValMeta, key));
                }

                if (val != null) {
                    byte[] newValMeta = U.join(U.intToBytes(tstamp), U.intToBytes(val.length));
                    byte[] keyWithMeta = U.join(newValMeta, key);

                    keyValDb.put(keyWithMeta, val);
                    keyMetaDb.put(key, newValMeta);

                    addedSize += val.length;
                }
                else
                    keyMetaDb.delete(key);

                parts.add(U.bytesToInt(key, 0));

                updateCnt(addedCnt, addedSize);
            }
            finally {
                lock.writeLock().unlock();
            }

            evictSingle();
        }

        /**
         * Similar to LevelDB get.
         * @param key Key bytes.
         * @return Fetched value or null if entry was not found.
         */
        @Override public byte[] get(byte[] key) {
            if (spiStopping)
                return null;

            ReadWriteLock lock = lock(key);

            lock.readLock().lock();

            try {
                if (spiStopping) // Double-check in case thread was waiting on lock too long.
                    return null;

                byte[] valMeta = keyMetaDb.get(key);

                if (valMeta == null)
                    return null;

                return keyValDb.get(U.join(valMeta, key));
            }
            finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Similar to LevelDB delete, but also performs some statistics accounting.
         * @param key Key bytes.
         */
        @Override public void delete(byte[] key) {
            if (spiStopping)
                return;

            ReadWriteLock lock = lock(key);

            lock.writeLock().lock();

            try {
                if (spiStopping) // Double-check in case thread was waiting on lock too long.
                    return;

                byte[] valMeta = keyMetaDb.get(key);

                if (valMeta != null) {
                    int valSize = U.bytesToInt(valMeta, TSTAMP_BYTES_IN_META);

                    updateCnt(-1, -valSize);

                    keyValDb.delete(U.join(valMeta, key));
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Closes database and let resources go.
         */
        @Override void close() {
            keyValDb.close();
            keyMetaDb.close();

            // Init checks for null.
            keyValDb = null;
            keyMetaDb = null;

            if (!isPersistent())
                U.delete(spaceFolder);

            updateCnt(-cnt.get(), -size.get());
        }

        /**
         * Performs main eviction operations, other methods are wrappers.
         * @param evictCnt Number of entries to evict.
         */
        @Override protected void evict(long evictCnt) {
            if (log.isDebugEnabled())
                log.debug("Clearing space: " + name);

            ReadOptions readOptions = new ReadOptions();

            readOptions.fillCache(false); // recommended for bulk operations.

            DBIterator dbIter = keyValDb.iterator(readOptions);

            dbIter.seekToFirst();

            while (dbIter.hasNext() && !spiStopping && (evictCnt > 0)) {
                Map.Entry<byte[], byte[]> entry = dbIter.next();

                byte[] keyWithMeta = entry.getKey();
                byte[] val = entry.getValue();

                evictQ.add(new GridTuple2<byte[], Integer>(keyWithMeta, val.length));

                evictCnt--;
            }

            dbIter.close();

            while(!spiStopping && !evictQ.isEmpty())
                evictSingle();
        }

        /**
         * Evicts single entry from cache. If queue is empty, just returns.
         */
        private void evictSingle() {
            if (spiStopping)
                return;

            GridTuple2<byte[], Integer> tuple = evictQ.poll();

            if (tuple == null)
                return;

            byte[] keyWithMeta = tuple.get1();
            int valLength = tuple.get2();

            if (keyWithMeta == null)
                return;

            byte[] key = Arrays.copyOfRange(keyWithMeta, TSTAMP_BYTES_IN_META + 4, keyWithMeta.length);

            ReadWriteLock lock = lock(key);

            lock.writeLock().lock();

            try {
                if (spiStopping)
                    return;

                if (keyValDb.get(keyWithMeta) == null)
                    return;

                keyMetaDb.delete(key);
                keyValDb.delete(keyWithMeta);
            }
            finally {
                lock.writeLock().unlock();
            }

            updateCnt(-1, -valLength);

            notifySwapManager(EVT_SWAP_SPACE_DATA_EVICTED, name, key);
        }
    }

    /**
     * LevelDB based space with eviction optimized for case when values are small.
     *
     * @author 2012 Copyright (C) GridGain Systems
     * @version 4.0.1c.09042012
     */
    private class GridLevelDbOptimizedSmallSpace extends GridLevelDbEvictSpace {
        /** Folder to store key->meta + value mapping. */
        private static final String KEY_META_VAL_DB_FOLDER = "keymetaval";

        /** Folder to store meta + key -> (null) mapping. */
        private static final String META_KEY_DB_FOLDER = "metakey";

        /** LevelDB to hold real values. */
        @GridToStringExclude
        private DB keyMetaValDb;

        /** LevelDB to store metadata to eviction. */
        @GridToStringExclude
        private DB metaKeyDb;

        /** Eviction queue. */
        private Queue<byte[]> evictQ = new ConcurrentLinkedQueue<byte[]>();

        /**
         * @param name Space name.
         */
        GridLevelDbOptimizedSmallSpace(String name) {
            super(name);
        }

        /** {@inheritDoc} */
        @Override void init(boolean spiStart) throws GridSpiException {
            if (keyMetaValDb != null || metaKeyDb != null)
                return;

            spaceFolder = new File(root(), name);

            File keyMetaValDbFolder  = new File(spaceFolder, KEY_META_VAL_DB_FOLDER);
            File metaKeyDbFolder = new File(spaceFolder, META_KEY_DB_FOLDER);

            if (!U.mkdirs(spaceFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + spaceFolder + ']');

            if (!U.mkdirs(keyMetaValDbFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + keyMetaValDbFolder + ']');

            if (!U.mkdirs(metaKeyDbFolder))
                throw new GridSpiException("Failed to create folder for space [space=" + name +
                    ", folder=" + metaKeyDbFolder + ']');

            try {
                keyMetaValDb = JniDBFactory.factory.open(keyMetaValDbFolder, writeOptions());
                metaKeyDb = JniDBFactory.factory.open(metaKeyDbFolder, writeOptions());
            }
            catch (IOException e) {
                throw new GridSpiException("Failed to create a LevelDB database for space [space=" + name +
                    ", folder=" + spaceFolder + ']', e);
            }

            if (isPersistent()) { // Need to init data from database.
                DBIterator dbIter = keyMetaValDb.iterator();

                dbIter.seekToFirst();

                while (dbIter.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = dbIter.next();

                    byte[] key = entry.getKey();
                    byte[] metaVal = entry.getValue();

                    int valSize = metaVal.length - TSTAMP_BYTES_IN_META;

                    updateCnt(1, valSize);

                    int part = U.bytesToInt(key, 0);

                    parts.add(part);
                }

                dbIter.close();
            }

            if (log.isDebugEnabled())
                log.debug("Space has been initialized: " + this);
        }

        /**
         * Stores key-value pair to space. If value is null, key is in fact deleted.
         *
         * @param key Data key.
         * @param val Data value.
         */
        @Override public void put(byte[] key, byte[] val) {
            if (spiStopping)
                return;

            ReadWriteLock lock = lock(key);

            lock.writeLock().lock();

            try {
                if (spiStopping) // Double-check in case thread was waiting on lock too long.
                    return;

                byte[] existingMetaVal = keyMetaValDb.get(key);

                int addedCnt = 1;
                int addedSize = 0;

                if (existingMetaVal != null) {
                    int existingValSize = existingMetaVal.length - TSTAMP_BYTES_IN_META;

                    addedCnt -= 1;
                    addedSize -= existingValSize;

                    byte[] existingMeta = Arrays.copyOfRange(existingMetaVal, 0, 4);

                    metaKeyDb.delete(U.join(existingMeta, key));
                }

                if (val != null) {
                    byte[] meta = U.intToBytes(timestamp());

                    byte[] newMetaVal = U.join(meta, val);
                    byte[] metaKey = U.join(meta, key);

                    keyMetaValDb.put(key, newMetaVal);
                    metaKeyDb.put(metaKey, U.intToBytes(val.length));

                    addedSize += val.length;
                }
                else
                    keyMetaValDb.delete(key);

                parts.add(U.bytesToInt(key, 0));

                updateCnt(addedCnt, addedSize);
            }
            finally {
                lock.writeLock().unlock();
            }

            evictSingle();
        }

        /**
         * Similar to LevelDB get.
         * @param key Key bytes.
         * @return Fetched value or null if entry was not found.
         */
        @Override public byte[] get(byte[] key) {
            if (spiStopping)
                return null;

            ReadWriteLock lock = lock(key);

            lock.readLock().lock();

            try {
                if (spiStopping) // Double-check in case thread was waiting on lock too long.
                    return null;

                byte[] meta = keyMetaValDb.get(key);

                if (meta == null)
                    return null;

                return Arrays.copyOfRange(meta, TSTAMP_BYTES_IN_META, meta.length);
            }
            finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Similar to LevelDB delete, but also performs some statistics accounting.
         * @param key Key bytes.
         */
        @Override public void delete(byte[] key) {
            if (spiStopping)
                return;

            ReadWriteLock lock = lock(key);

            lock.writeLock().lock();

            try {
                if (spiStopping) // Double-check in case thread was waiting on lock too long.
                    return;

                byte[] metaVal = keyMetaValDb.get(key);

                if (metaVal != null) {
                    int valSize = metaVal.length - TSTAMP_BYTES_IN_META;

                    updateCnt(-1, -valSize);

                    byte[] meta = Arrays.copyOfRange(metaVal, 0, TSTAMP_BYTES_IN_META);

                    metaKeyDb.delete(U.join(meta, key));
                    keyMetaValDb.delete(key);
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Closes database and let resources go.
         */
        @Override void close() {
            keyMetaValDb.close();
            metaKeyDb.close();

            // Init checks for null.
            keyMetaValDb = null;
            metaKeyDb = null;

            if (!isPersistent())
                U.delete(spaceFolder);

            updateCnt(-cnt.get(), -size.get());
        }

        /**
         * Performs main eviction operations, other methods are wrappers.
         * @param evictCnt Number of entries to evict.
         */
        @Override protected void evict(long evictCnt) {
            if (log.isDebugEnabled())
                log.debug("Clearing space: " + name);

            ReadOptions readOptions = new ReadOptions();
            readOptions.fillCache(false); // recommended for bulk operations.

            DBIterator dbIter = metaKeyDb.iterator(readOptions);

            dbIter.seekToFirst();

            while (dbIter.hasNext() && !spiStopping && (evictCnt > 0)) {
                Map.Entry<byte[], byte[]> entry = dbIter.next();

                byte[] metaKey = entry.getKey();

                evictQ.add(metaKey);

                evictCnt--;
            }

            dbIter.close();

            while(!spiStopping && !evictQ.isEmpty())
                evictSingle();
        }

        /**
         * Evicts single entry from cache. If queue is empty, just returns.
         */
        private void evictSingle() {
            if (spiStopping)
                return;

            byte[] metaKey = evictQ.poll();

            if (metaKey == null)
                return;

            byte[] key = Arrays.copyOfRange(metaKey, TSTAMP_BYTES_IN_META, metaKey.length);

            ReadWriteLock lock = lock(key);

            lock.writeLock().lock();

            try {
                if (spiStopping)
                    return;

                byte[] meta = metaKeyDb.get(metaKey);

                if (meta == null)
                    return;

                int valLength = U.bytesToInt(meta, 0);

                keyMetaValDb.delete(key);
                metaKeyDb.delete(metaKey);

                updateCnt(-1, -valLength);
            }
            finally {
                lock.writeLock().unlock();
            }

            notifySwapManager(EVT_SWAP_SPACE_DATA_EVICTED, name, key);
        }
    }
}
