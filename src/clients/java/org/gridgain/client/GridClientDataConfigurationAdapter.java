// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client;

import org.gridgain.client.balancer.*;

/**
 * Data configuration bean.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridClientDataConfigurationAdapter implements GridClientDataConfiguration {
    /** Grid cache name. */
    private String name;

    /** Affinity. */
    private GridClientDataAffinity affinity;

    /** Balancer for pinned mode. */
    private GridClientLoadBalancer balancer = new GridClientRandomBalancer();

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /**
     * Creates empty configuration.
     */
    public GridClientDataConfigurationAdapter() {
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridClientDataConfigurationAdapter(GridClientDataConfiguration cfg) {
        // Preserve alphabetic order for maintenance.
        affinity = cfg.getAffinity();
        balancer = cfg.getPinnedBalancer();
        name = cfg.getName();
    }

    /**
     * Sets grid cache name for this configuration.
     *
     * @param name Cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public GridClientDataAffinity getAffinity() {
        return affinity;
    }

    /**
     * Sets client data affinity for this configuration.
     *
     * @param affinity Client data affinity.
     */
    public void setAffinity(GridClientDataAffinity affinity) {
        this.affinity = affinity;
    }

    /** {@inheritDoc} */
    @Override public GridClientLoadBalancer getPinnedBalancer() {
        return balancer;
    }

    /**
     * Sets balancer for pinned mode for this configuration.
     *
     * @param balancer Balancer that will be used in pinned mode.
     */
    public void setBalancer(GridClientLoadBalancer balancer) {
        this.balancer = balancer;
    }
}
