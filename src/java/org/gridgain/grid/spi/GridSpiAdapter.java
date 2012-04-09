// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.json.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.io.*;
import java.text.*;
import java.util.*;

import static org.gridgain.grid.GridEventType.*;

/**
 * This class provides convenient adapter for SPI implementations.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public abstract class GridSpiAdapter implements GridSpi, GridSpiManagementMBean, GridSpiJsonConfigurable {
    /** System line separator. */
    private static final String NL = System.getProperty("line.separator");

    /** Instance of SPI annotation. */
    private GridSpiInfo spiAnn;

    /** */
    private ObjectName spiMBean;

    /** SPI start timestamp. */
    private long startTstamp;

    /** */
    @GridLoggerResource
    private GridLogger log;

    /** */
    @GridMBeanServerResource
    private MBeanServer jmx;

    /** */
    @GridHomeResource
    private String ggHome;

    /** */
    @GridLocalNodeIdResource
    private UUID nodeId;

    /** SPI name. */
    private String name;

    /** Authenticator. */
    private volatile Authenticator auth = new Authenticator() {
        @Override public boolean authenticateNode(UUID nodeId, Map<String, Object> attrs) {
            return false;
        }
    };

    /** Grid SPI context. */
    private GridSpiContext spiCtx = new GridDummySpiContext(null, auth);

    /** Discovery listener. */
    private GridLocalEventListener paramsLsnr;

    /**
     * Creates new adapter and initializes it from the current (this) class.
     * SPI name will be initialized to the simple name of the class
     * (see {@link Class#getSimpleName()}).
     */
    protected GridSpiAdapter() {
        for (Class<?> cls = getClass(); cls != null; cls = cls.getSuperclass())
            if ((spiAnn = cls.getAnnotation(GridSpiInfo.class)) != null)
                break;

        assert spiAnn != null : "Every SPI must have @GridSpiInfo annotation.";

        name = U.getSimpleName(getClass());
    }

    /**
     * Starts startup stopwatch.
     */
    protected void startStopwatch() {
        startTstamp = System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public final String getAuthor() {
        return spiAnn.author();
    }

    /** {@inheritDoc} */
    @Override public final String getVendorUrl() {
        return spiAnn.url();
    }

    /** {@inheritDoc} */
    @Override public final String getVendorEmail() {
        return spiAnn.email();
    }

    /** {@inheritDoc} */
    @Override public final String getVersion() {
        return spiAnn.version();
    }

    /** {@inheritDoc} */
    @Override public final String getStartTimestampFormatted() {
        return DateFormat.getDateTimeInstance().format(new Date(startTstamp));
    }

    /** {@inheritDoc} */
    @Override public final String getUpTimeFormatted() {
        return X.timeSpan2HMSM(getUpTime());
    }

    /** {@inheritDoc} */
    @Override public final long getStartTimestamp() {
        return startTstamp;
    }

    /** {@inheritDoc} */
    @Override public final long getUpTime() {
        return startTstamp == 0 ? 0 : System.currentTimeMillis() - startTstamp;
    }

    /** {@inheritDoc} */
    @Override public UUID getLocalNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public final String getGridGainHome() {
        return ggHome;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /**
     * Sets SPI name.
     *
     * @param name SPI name.
     */
    @GridSpiConfiguration(optional = true)
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public final void onContextInitialized(final GridSpiContext spiCtx) throws GridSpiException {
        assert spiCtx != null;

        auth = new Authenticator() {
            @Override public boolean authenticateNode(UUID nodeId, Map<String, Object> attrs) throws GridException {
                return spiCtx.authenticateNode(nodeId, attrs);
            }
        };

        this.spiCtx = spiCtx;

        spiCtx.addLocalEventListener(paramsLsnr = new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                assert evt instanceof GridDiscoveryEvent : "Invalid event [expected=" + EVT_NODE_JOINED +
                    ", actual=" + evt.type() + ", evt=" + evt + ']';

                GridNode node = spiCtx.node(((GridDiscoveryEvent)evt).eventNodeId());

                if (node != null)
                    try {
                        checkConfigurationConsistency(spiCtx, node, false);
                        checkConfigurationConsistency0(spiCtx, node, false);
                    }
                    catch(GridSpiException e) {
                        U.error(log, "Spi consistency check failed [node=" + node.id() + ", spi=" + getName() + ']', e);
                    }
            }
        }, EVT_NODE_JOINED);

        for (GridNode node : spiCtx.remoteNodes()) {
            checkConfigurationConsistency(spiCtx, node, true);
            checkConfigurationConsistency0(spiCtx, node, true);
        }

        onContextInitialized0(spiCtx);
    }

    /**
     * Method to be called in the end of onContextInitialized method.
     *
     * @param spiCtx SPI context.
     * @throws GridSpiException In case of errors.
     */
    protected void onContextInitialized0(final GridSpiContext spiCtx) throws GridSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void onContextDestroyed() {
        onContextDestroyed0();

        if (spiCtx != null && paramsLsnr != null)
            spiCtx.removeLocalEventListener(paramsLsnr);

        GridNode locNode = spiCtx == null ? null : spiCtx.localNode();

        // Set dummy no-op context.
        spiCtx = new GridDummySpiContext(locNode, auth);
    }

    /**
     * Method to be called in the beginning of onContextDestroyed() method.
     */
    protected void onContextDestroyed0() {
        // No-op.
    }

    /**
     * This method returns SPI internal instances that need to be injected as well.
     * Usually these will be instances provided to SPI externally by user, e.g. during
     * SPI configuration.
     *
     * @return Internal SPI objects that also need to be injected.
     */
    public Collection<Object> injectables() {
        return Collections.emptyList();
    }

    /**
     * Gets SPI context.
     *
     * @return SPI context.
     */
    protected GridSpiContext getSpiContext() {
        return spiCtx;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
        return Collections.emptyMap();
    }

    /**
     * Throws exception with uniform error message if given parameter's assertion condition
     * is {@code false}.
     *
     * @param cond Assertion condition to check.
     * @param condDesc Description of failed condition. Note that this description should include
     *      JavaBean name of the property (<b>not</b> a variable name) as well condition in
     *      Java syntax like, for example:
     *      <pre name="code" class="java">
     *      ...
     *      assertParameter(dirPath != null, "dirPath != null");
     *      ...
     *      </pre>
     *      Note that in case when variable name is the same as JavaBean property you
     *      can just copy Java condition expression into description as a string.
     * @throws GridSpiException Thrown if given condition is {@code false}
     */
    protected final void assertParameter(boolean cond, String condDesc) throws GridSpiException {
        if (!cond)
            throw new GridSpiException("SPI parameter failed condition check: " + condDesc);
    }

    /**
     * Gets uniformly formatted message for SPI start.
     *
     * @return Uniformly formatted message for SPI start.
     * @throws GridSpiException If SPI is missing {@link GridSpiInfo} annotation.
     */
    protected final String startInfo() throws GridSpiException {
        GridSpiInfo ann = getClass().getAnnotation(GridSpiInfo.class);

        if (ann == null)
            throw new GridSpiException("@GridSpiInfo annotation is missing for the SPI.");

        return "SPI started ok [startMs=" + getUpTime() + ", spiMBean=" + spiMBean + ']';
    }

    /**
     * Gets uniformly format message for SPI stop.
     *
     * @return Uniformly format message for SPI stop.
     */
    protected final String stopInfo() {
        return "SPI stopped ok.";
    }

    /**
     * Gets uniformed string for configuration parameter.
     *
     * @param name Parameter name.
     * @param val Parameter value.
     * @return Uniformed string for configuration parameter.
     */
    protected final String configInfo(String name, Object val) {
        assert name != null;

        return "Using parameter [" + name + '=' + val + ']';
    }

    /**
     * @param msg Error message.
     * @param locVal Local node value.
     * @param rmtVal Remote node value.
     * @return Error text.
     */
    private static String format(String msg, Object locVal, Object rmtVal) {
        return msg + NL +
            ">>> => Local node:  " + locVal + NL +
            ">>> => Remote node: " + rmtVal + NL;
    }

    /**
     * Registers SPI MBean. Note that SPI can only register one MBean.
     *
     * @param gridName Grid name. If null, then name will be empty.
     * @param impl MBean implementation.
     * @param mbeanItf MBean interface (if {@code null}, then standard JMX
     *    naming conventions are used.
     * @param <T> Type of the MBean
     * @throws GridSpiException If registration failed.
     */
    protected final <T extends GridSpiManagementMBean> void registerMBean(String gridName, T impl, Class<T> mbeanItf)
        throws GridSpiException {
        assert mbeanItf == null || mbeanItf.isInterface();
        assert jmx != null;

        try {
            spiMBean = U.registerMBean(jmx, gridName, "SPIs", getName(), impl, mbeanItf);

            if (log.isDebugEnabled())
                log.debug("Registered SPI MBean: " + spiMBean);
        }
        catch (JMException e) {
            throw new GridSpiException("Failed to register SPI MBean: " + spiMBean, e);
        }
    }

    /**
     * Unregisters MBean.
     *
     * @throws GridSpiException If bean could not be unregistered.
     */
    protected final void unregisterMBean() throws GridSpiException {
        // Unregister SPI MBean.
        if (spiMBean != null) {
            assert jmx != null;

            try {
                jmx.unregisterMBean(spiMBean);

                if (log.isDebugEnabled())
                    log.debug("Unregistered SPI MBean: " + spiMBean);
            }
            catch (JMException e) {
                throw new GridSpiException("Failed to unregister SPI MBean: " + spiMBean, e);
            }
        }
    }

    /**
     * @return {@code true} if this check is optional.
     */
    private boolean checkOptional() {
        GridSpiConsistencyChecked ann = U.getAnnotation(getClass(), GridSpiConsistencyChecked.class);

        return ann != null && ann.optional();
    }

    /**
     * @return {@code true} if this check is enabled.
     */
    private boolean checkEnabled() {
        return U.getAnnotation(getClass(), GridSpiConsistencyChecked.class) != null;
    }

    /**
     * @return {@code true} if this check is enforced.
     */
    private boolean checkEnforced() {
        return U.getAnnotation(getClass(), GridSpiConsistencyEnforced.class) != null;
    }

    /**
     * Method which is called in the end of checkConfigurationConsistency() method. May be overriden in SPIs.
     *
     * @param spiCtx SPI context.
     * @param node Remote node.
     * @param starting If this node is starting or not.
     * @throws GridSpiException in case of errors.
     */
    protected void checkConfigurationConsistency0(GridSpiContext spiCtx, GridNode node, boolean starting)
        throws GridSpiException {
        // No-op.
    }

    /**
     * Checks remote node SPI configuration and prints warnings if necessary.
     *
     * @param spiCtx SPI context.
     * @param node Remote node.
     * @param starting Flag indicating whether this method is called during SPI start or not.
     * @throws GridSpiException If check fatally failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    private void checkConfigurationConsistency(GridSpiContext spiCtx, GridNode node, boolean starting)
        throws GridSpiException {
        assert spiCtx != null;
        assert node != null;

        /*
         * Optional SPI means that we should not print warning if SPIs are different but
         * still need to compare attributes if SPIs are the same.
         */
        boolean optional = checkOptional();
        boolean enabled = checkEnabled();
        boolean enforced = checkEnforced();

        if (!enabled && !enforced)
            return;

        /*** Don't compare SPIs from different virtual grids unless explicitly requested. ***/

        String locGridName = spiCtx.localNode().attribute(GridNodeAttributes.ATTR_GRID_NAME);
        String rmtGridName = node.attribute(GridNodeAttributes.ATTR_GRID_NAME);

        if (!F.eq(locGridName, rmtGridName) && !enforced) {
            if (log.isDebugEnabled())
                log.debug("Skip consistency check for SPIs from different grids [locGridName=" + locGridName +
                    ", rmtGridName=" + rmtGridName + ']');

            return;
        }

        String clsAttr = createSpiAttributeName(GridNodeAttributes.ATTR_SPI_CLASS);
        String verAttr = createSpiAttributeName(GridNodeAttributes.ATTR_SPI_VER);

        String name = getName();

        SB sb = new SB();

        /*
         * If there are any attributes do compare class and version
         * (do not print warning for the optional SPIs).
         */
        /* Check SPI class and version. */
        String locCls = spiCtx.localNode().attribute(clsAttr);
        String rmtCls = node.attribute(clsAttr);

        String locVer = spiCtx.localNode().attribute(verAttr);
        String rmtVer = node.attribute(verAttr);

        assert locCls != null: "Local SPI class name attribute not found: " + clsAttr;
        assert locVer != null: "Local SPI version attribute not found: " + verAttr;

        boolean isSpiConsistent = false;

        if (rmtCls == null) {
            if (enforced && starting)
                throw new GridSpiException("Remote SPI is not configured [name=" + name + ", loc=" + locCls + ']');

            sb.a(format(">>> Remote SPI is not configured: " + name, locCls, rmtCls));
        }
        else if (!locCls.equals(rmtCls)) {
            if (enforced && starting)
                throw new GridSpiException("Remote SPI is of different type [name=" + name + ", loc=" + locCls +
                    ", rmt=" + rmtCls + ']');

            sb.a(format(">>> Remote SPI is of different type: " + name, locCls, rmtCls));
        }
        else if (!F.eq(rmtVer, locVer)) {
            if (rmtVer.contains("x.x") || locVer.contains("x.x")) {
                if (log.isDebugEnabled())
                    log.debug("Skip SPI version check for 'x.x' development version in: " + name);
            }
            else {
                if (enforced && starting)
                    throw new GridSpiException("Remote SPI is of different version [name=" + name + ", locVer=" +
                        locVer + ", rmtVer=" + rmtVer + ']');

                sb.a(format(">>> Remote SPI is of different version: " + name, locVer, rmtVer));
            }
        }
        else
            isSpiConsistent = true;

        if (optional && !isSpiConsistent)
            return;

        // It makes no sense to compare inconsistent SPIs attributes.
        if (isSpiConsistent) {
            List<String> attrs = getConsistentAttributeNames();

            // Process all SPI specific attributes.
            for (String attr: attrs) {
                // Ignore class and version attributes processed above.
                if (!attr.equals(clsAttr) && !attr.equals(verAttr)) {
                    // This check is considered as optional if no attributes
                    Object rmtVal = node.attribute(attr);
                    Object locVal = spiCtx.localNode().attribute(attr);

                    if (locVal == null && rmtVal == null)
                        continue;

                    if (locVal == null || rmtVal == null || !locVal.equals(rmtVal))
                        sb.a(format(">>> Remote node has different " + getName() + " SPI attribute " +
                            attr, locVal, rmtVal));
                }
            }
        }

        if (sb.length() > 0) {
            String msg;

            if (starting)
                msg = NL + NL +
                    ">>> +--------------------------------------------------------------------+" + NL +
                    ">>> + Courtesy notice that starting node has inconsistent configuration. +" + NL +
                    ">>> + Ignore this message if you are sure that this is done on purpose.  +" + NL +
                    ">>> +--------------------------------------------------------------------+" + NL +
                    ">>> Remote Node ID: " + node.id().toString().toUpperCase() + NL + sb;
            else
                msg = NL + NL +
                    ">>> +-------------------------------------------------------------------+" + NL +
                    ">>> + Courtesy notice that joining node has inconsistent configuration. +" + NL +
                    ">>> + Ignore this message if you are sure that this is done on purpose. +" + NL +
                    ">>> +-------------------------------------------------------------------+" + NL +
                    ">>> Remote Node ID: " + node.id().toString().toUpperCase() + NL + sb;

            U.courtesy(log, msg);
        }
    }

    /**
     * Returns back a list of attributes that should be consistent
     * for this SPI. Consistency means that remote node has to
     * have the same attribute with the same value.
     *
     * @return List or attribute names.
     */
    protected List<String> getConsistentAttributeNames() {
        return Collections.emptyList();
    }

    /**
     * Creates new name for the given attribute. Name contains
     * SPI name prefix.
     *
     * @param attrName SPI attribute name.
     * @return New name with SPI name prefix.
     */
    protected String createSpiAttributeName(String attrName) {
        return U.spiAttribute(this, attrName);
    }

    /** {@inheritDoc} */
    @GridSpiConfiguration(optional = true)
    @Override public void setJson(String json) {
        assert json != null;

        try {
            GridJsonDeserializer.inject(this, json);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * Temporarily SPI context.
     *
     * @author 2012 Copyright (C) GridGain Systems
     * @version 4.0.1c.09042012
     */
    private static class GridDummySpiContext implements GridSpiContext {
        /** */
        private final GridNode locNode;

        /** */
        private final Authenticator auth;

        /**
         * Create temp SPI context.
         *
         * @param locNode Local node.
         * @param auth Authenticator.
         */
        GridDummySpiContext(GridNode locNode, Authenticator auth) {
            this.locNode = locNode;
            this.auth = auth;
        }

        /** {@inheritDoc} */
        @Override public void addLocalEventListener(GridLocalEventListener lsnr, int... types) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public void addMessageListener(GridMessageListener lsnr, String topic) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void recordEvent(GridEvent evt) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void registerPort(int port, GridPortProtocol proto) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void deregisterPort(int port, GridPortProtocol proto) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public boolean isEnterprise() {
            return U.isEnterprise();
        }

        /** {@inheritDoc} */
        @Override public void deregisterPorts() {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public <K, V> V get(String cacheName, K key) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V put(String cacheName, K key, V val, long ttl) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> V remove(String cacheName, K key) throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K> boolean containsKey(String cacheName, K key) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void writeToSwap(String spaceName, Object key, @Nullable Object val,
            @Nullable ClassLoader ldr) throws GridException {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public <T> T readFromSwap(String spaceName, Object key, @Nullable ClassLoader ldr)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void removeFromSwap(String spaceName, Object key, @Nullable ClassLoader ldr)
            throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Collection<GridNode> nodes() {
            return  locNode == null  ? Collections.<GridNode>emptyList() : Collections.singletonList(locNode);
        }

        /** {@inheritDoc} */
        @Override public GridNode localNode() {
            return locNode;
        }

        /** {@inheritDoc} */
        @Override @Nullable
        public GridNode node(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridNode> remoteNodes() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public Collection<GridNode> topology(GridTaskSession taskSes, Collection<? extends GridNode> grid) {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public boolean pingNode(UUID nodeId) {
            return locNode != null && nodeId.equals(locNode.id());
        }

        /** {@inheritDoc} */
        @Override public boolean removeLocalEventListener(GridLocalEventListener lsnr) {
            return false;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public boolean removeMessageListener(GridMessageListener lsnr, String topic) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void send(GridNode node, Serializable msg, String topic) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void send(Collection<? extends GridNode> nodes, Serializable msg, String topic) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public boolean authenticateNode(UUID nodeId, Map<String, Object> attrs) throws GridException {
            return auth.authenticateNode(nodeId, attrs);
        }
    }

    /**
     *
     */
    private interface Authenticator {
        /**
         * Delegates to real implementation whenever possible.
         *
         * @param nodeId Node ID.
         * @param attrs Attributes.
         * @return {@code True} if passed authentication.
         * @throws GridException If failed.
         */
        public boolean authenticateNode(UUID nodeId, Map<String, Object> attrs) throws GridException;
    }
}
