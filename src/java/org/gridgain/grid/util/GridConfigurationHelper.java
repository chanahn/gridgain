// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.commons.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Utility for grid configuration properties values overriding.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridConfigurationHelper {
    /** */
    private static final String PROPERTY_PREFIX = "gg.";

    /** */
    private static final String CONFIG_PREFIX = "config.";

    /** */
    private static final String SPI_PREFIX = "spi.";

    /** */
    private static final String CACHE_PREFIX = "cache.";

    /** */
    private static final String LOG_PREFIX = "log.";

    /**
     * Enforces singleton.
     */
    private GridConfigurationHelper() {
        /* No-op. */
    }

    /**
     * Overrides configuration.
     *
     * @param cfg Configuration.
     * @param props Properties.
     * @param gridName Grid name.
     * @param log Logger.
     * @throws GridException If overrides failed.
     */
    public static void overrideConfiguration(GridConfiguration cfg, Map<Object, Object> props, String gridName, GridLogger log)
        throws GridException {
        assert cfg != null;
        assert props != null;
        assert log != null;

        Map<String, String> nameMap = new HashMap<String, String>();

        // Properties for all grids.
        String prefix = PROPERTY_PREFIX + "*.";

        for (Map.Entry<Object, Object> entry : props.entrySet())
            if (entry.getKey() instanceof String) {
                String key = (String)entry.getKey();

                if (key.startsWith(prefix))
                    nameMap.put(key.substring(prefix.length()), key);
            }

        // Properties for grids with specified name.
        prefix = PROPERTY_PREFIX + (gridName != null ? gridName : "") + '.';

        for (Map.Entry<Object, Object> entry : props.entrySet())
            if (entry.getKey() instanceof String) {
                String key = (String)entry.getKey();

                if (key.startsWith(prefix))
                    nameMap.put(key.substring(prefix.length()), key);
            }

        for (Map.Entry<String, String> entry : nameMap.entrySet()) {
            String key = entry.getKey();
            String fullKey = entry.getValue();

            Object objVal = props.get(fullKey);

            if (!(objVal instanceof String)) {
                U.warn(log, "Failed to override configuration property (value must be a String) [key=" + fullKey +
                    ", value=" + objVal + ']');

                break;
            }

            String val = (String)objVal;

            Object target;
            String targetPropName;

            if (key.startsWith(CONFIG_PREFIX)) {
                targetPropName = key.substring(CONFIG_PREFIX.length());

                target = cfg;
            }
            else if (key.startsWith(SPI_PREFIX)) {
                String prop = key.substring(SPI_PREFIX.length());

                String[] s = prop.split("\\.");

                if (s == null || s.length != 2) {
                    U.warn(log, "Wrong SPI parameter name format (will ignore): " + fullKey);

                    continue;
                }

                target = findSpi(cfg, s[0]);

                if (target == null) {
                    U.warn(log, "Failed to find target spi for property (will ignore): " + fullKey);

                    continue;
                }

                targetPropName = s[1];
            }
            else if (key.startsWith(CACHE_PREFIX)) {
                String prop = key.substring(CACHE_PREFIX.length());

                String[] s = prop.split("\\.");

                if (s == null || s.length != 2) {
                    U.warn(log, "Wrong Cache parameter name format (will ignore): " + fullKey);

                    continue;
                }

                target = findCache(cfg, s[0]);

                if (target == null) {
                    U.warn(log, "Failed to find target cache configuration for property (will ignore): " + fullKey);

                    continue;
                }

                targetPropName = s[1];
            }
            else if (key.startsWith(LOG_PREFIX)) {
                boolean isLog4jUsed = GridConfigurationHelper.class.getClassLoader()
                    .getResource("org/apache/log4j/Appender.class") != null;

                if (isLog4jUsed) {
                    try {
                        Class logCls = Class.forName("org.apache.log4j.Logger");

                        Object log4j = logCls.getMethod("getLogger", String.class).invoke(logCls, val);

                        Class levelCls = Class.forName("org.apache.log4j.Level");

                        log4j.getClass().getMethod("setLevel", levelCls).invoke(log4j,
                            levelCls.getField("DEBUG").get(levelCls));
                    }
                    catch (Exception ignore) {
                        U.warn(log, "Failed to change log level for property (will ignore): " + fullKey);
                    }
                }

                continue;
            }
            else {
                U.warn(log, "Wrong Configuration parameter name format (will ignore): " + fullKey);

                continue;
            }

            assert target != null;

            if (F.isEmpty(targetPropName)) {
                U.warn(log, "Wrong configuration parameter name format (will ignore): " + fullKey);

                continue;
            }

            Method mtd = findSetterMethod(target.getClass(), targetPropName);

            if (mtd == null) {
                U.warn(log, "Failed to find target setter method for property (will ignore): " + fullKey);

                continue;
            }

            Object param = convert(val, mtd.getParameterTypes()[0], fullKey);

            try {
                mtd.invoke(target, param);
            }
            catch (IllegalArgumentException e) {
                throw new GridException("Failed to invoke setter to override configuration property [name=" + fullKey +
                    ", value=" + val + ']', e);
            }
            catch (IllegalAccessException e) {
                throw new GridException("Failed to invoke getter or setter to override configuration property [name=" +
                    fullKey + ", value=" + val + ']', e);
            }
            catch (InvocationTargetException e) {
                throw new GridException("Failed to invoke getter or setter to override configuration property [name=" +
                    fullKey + ", value=" + val + ']', e);
            }

            if (log.isInfoEnabled())
                log.info("Override configuration property [name=" + fullKey + ", value=" + val + ']');
        }
    }

    /**
     * Finds SPI in configuration by name.
     *
     * @param cfg Configuration.
     * @param spiName Name.
     * @return SPI.
     */
    @Nullable
    private static GridSpi findSpi(GridConfiguration cfg, String spiName) {
        if (checkSpiName(spiName, cfg.getDiscoverySpi()))
            return cfg.getDiscoverySpi();

        if (checkSpiName(spiName, cfg.getCommunicationSpi()))
            return cfg.getCommunicationSpi();

        if (checkSpiName(spiName, cfg.getDeploymentSpi()))
            return cfg.getDeploymentSpi();

        if (checkSpiName(spiName, cfg.getEventStorageSpi()))
            return cfg.getEventStorageSpi();

        if (cfg.getCheckpointSpi() != null)
            for (GridSpi spi : cfg.getCheckpointSpi())
                if (checkSpiName(spiName, spi))
                    return spi;

        if (checkSpiName(spiName, cfg.getCollisionSpi()))
            return cfg.getCollisionSpi();

        if (cfg.getFailoverSpi() != null)
            for (GridSpi spi : cfg.getFailoverSpi())
                if (checkSpiName(spiName, spi))
                    return spi;

        if (cfg.getTopologySpi() != null)
            for (GridSpi spi : cfg.getTopologySpi())
                if (checkSpiName(spiName, spi))
                    return spi;

        if (checkSpiName(spiName, cfg.getMetricsSpi()))
            return cfg.getMetricsSpi();

        if (checkSpiName(spiName, cfg.getAuthenticationSpi()))
            return cfg.getAuthenticationSpi();

        if (checkSpiName(spiName, cfg.getSecureSessionSpi()))
            return cfg.getSecureSessionSpi();

        if (cfg.getLoadBalancingSpi() != null)
            for (GridSpi spi : cfg.getLoadBalancingSpi())
                if (checkSpiName(spiName, spi))
                    return spi;

        if (cfg.getSwapSpaceSpi() != null)
            for (GridSpi spi : cfg.getSwapSpaceSpi())
                if (checkSpiName(spiName, spi))
                    return spi;

        return null;
    }

    /**
     * Finds cache configuration in grid configuration by name.
     *
     * @param cfg Grid configuration.
     * @param cacheName Name.
     * @return Cache configuration.
     */
    @Nullable
    private static GridCacheConfiguration findCache(GridConfiguration cfg, String cacheName) {
        if (cfg.getCacheConfiguration() != null)
            for (GridCacheConfiguration cacheCfg : cfg.getCacheConfiguration())
                if (cacheCfg.getName().equals(cacheName))
                    return cacheCfg;

        return null;
    }

    /**
     * Checks if passed name is name of passed SPI.
     *
     * @param name Name.
     * @param spi SPI.
     * @return {@code true} - if passed name is name of passed SPI.
     */
    private static boolean checkSpiName(String name, GridSpi spi) {
        return spi != null && spi.getName().equals(name);
    }

    /**
     * Get setter method for field.
     *
     * @param field Field.
     * @param cls Class.
     * @return Setter method.
     */
    @Nullable
    private static Method findSetterMethod(Class<?> cls, String field) {
        assert cls != null;
        assert field != null;

        String mtdName = "set" + StringUtils.capitalize(field);

        for (Method mtd : cls.getMethods())
            if (mtdName.equals(mtd.getName()) && mtd.getParameterTypes().length == 1)
                return mtd;

        return null;
    }

    /**
     * Converts value from {@code String} to simple type.
     *
     * @param val Value.
     * @param cls Result type.
     * @param name Name.
     * @return Converted value.
     * @throws GridException If convert fails.
     */
    private static Object convert(String val, Class<?> cls, String name) throws GridException {
        if (cls == String.class)
            return val;

        if (cls == Boolean.class || cls == boolean.class)
            return Boolean.parseBoolean(val);

        if (cls == Character.class || cls == char.class)
            return val.charAt(0);

        try {
            if (cls == Byte.class || cls == byte.class)
                return Byte.parseByte(val);

            if (cls == Short.class || cls == short.class)
                return Short.parseShort(val);

            if (cls == Integer.class || cls == int.class)
                return Integer.parseInt(val);

            if (cls == Long.class || cls == long.class)
                return Long.parseLong(val);

            if (cls == Float.class || cls == float.class)
                return Float.parseFloat(val);

            if (cls == Double.class || cls == double.class)
                return Double.parseDouble(val);
        }
        catch (NumberFormatException ignore) {
            throw new GridException("Failed to parse configuration property value [name=" + name + ", value=" + val
                + ']');
        }

        throw new GridException("Unexpected configuration property type (must be primitive or String) [name=" + name +
            ", value=" + val + ", cls=" + cls + ']');
    }
}
