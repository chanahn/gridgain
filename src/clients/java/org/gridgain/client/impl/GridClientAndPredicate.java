// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

/**
 * AND predicate. Passes if and only if both provided filters accept the node.
 * This filter uses short-term condition evaluation, i.e. second filter would not
 * be invoked if first filter returned {@code false}.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public class GridClientAndPredicate<T> implements GridClientPredicate<T> {
    /** First filter to check. */
    private GridClientPredicate<T> first;

    /** Second filter to check. */
    private GridClientPredicate<T> second;

    /**
     * Creates AND filter.
     *
     * @param first First filter to check.
     * @param second Second filter to check.
     */
    public GridClientAndPredicate(GridClientPredicate<T> first, GridClientPredicate<T> second) {
        assert first != null;
        assert second != null;
        
        this.first = first;
        this.second = second;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(T element) {
        return first.apply(element) && second.apply(element);
    }
}
