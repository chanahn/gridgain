// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.util;

import org.gridgain.client.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Java client utils.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public abstract class GridClientUtils {
    /**
     * Closes resource without reporting any error.
     *
     * @param closeable Resource to close.
     */
    public static void closeQuiet(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException ignored) {
            }
        }
    }

    /**
     * Closes socket without reporting any error.
     *
     * @param sock Resource to close.
     */
    public static void closeQuiet(Socket sock) {
        if (sock != null) {
            try {
                sock.close();
            }
            catch (IOException ignored) {
            }
        }
    }

    /**
     * Creates a predicates that checks if given value is contained in collection c.
     *
     * @param c Collection to check.
     * @param <T> Type of elements in collection.
     * @return Predicate.
     */
    public static <T> GridClientPredicate<T> contains(final Collection<T> c) {
        return new GridClientPredicate<T>() {
            @Override public boolean apply(T t) {
                return (!(c == null || c.isEmpty())) && c.contains(t);
            }
        };
    }

    /**
     * Gets first element from given collection or returns {@code null} if the collection is empty.
     *
     * @param c A collection.
     * @param <T> Type of the collection.
     * @return Collections' first element or {@code null} in case if the collection is empty.
     */
    public static <T> T first(Iterable<? extends T> c) {
        if (c == null)
            return null;

        Iterator<? extends T> it = c.iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Applies filter and returns filtered collection of nodes.
     *
     * @param elements Nodes to be filtered.
     * @param filter Filter to apply
     * @return Filtered collection.
     */
    public static <T> Collection<T> applyFilter(Iterable<? extends T> elements,
        GridClientPredicate<T> filter) {
        assert filter != null;

        Collection<T> res = new LinkedList<T>();

        for (T el : elements) {
            if (filter.apply(el))
                res.add(el);
        }

        return res;
    }
}
