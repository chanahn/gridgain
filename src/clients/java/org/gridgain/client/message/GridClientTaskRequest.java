// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.message;

import java.util.*;

/**
 * {@code Task} command request.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridClientTaskRequest extends GridClientAbstractMessage {
    /** Task name. */
    private String taskName;

    /** Task parameters */
    private Object[] args;

    /**
     * @return Task name.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @param taskName Task name.
     */
    public void taskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * @return Arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @param args Arguments.
     */
    public void arguments(Object[] args) {
        this.args = args;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridClientTaskRequest other = (GridClientTaskRequest)o;

        return taskName != null ? taskName.equals(other.taskName) : other.taskName == null &&
            Arrays.equals(args, other.args);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (taskName != null ? taskName.hashCode() : 0) +
            (args != null ? Arrays.hashCode(args) : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new StringBuilder().
            append("GridClientTaskRequest [taskName=").
            append(taskName).
            append(", args=").
            append(args == null ? "null" : Arrays.asList(args).toString()).
            append("]").
            toString();
    }
}
