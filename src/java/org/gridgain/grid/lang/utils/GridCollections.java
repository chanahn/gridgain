// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Provides locked wrappers around given maps and collections. Since {@link ReentrantLock}
 * performs a lot better than standard Java {@code synchronization}, these locked wrappers
 * should perform better as their analogous methods in {@code java.util.Collections} class.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public final class GridCollections {
    /**
     * Ensure singleton.
     */
    private GridCollections() {
        // No-op.
    }

    /**
     * Gets locked map wrapping given map.
     *
     * @param m Map to wrap into locked implementation.
     * @return Locked map.
     */
    public static <K, V> Map<K, V> lockedMap(Map<K, V> m) {
        return new LockedMap<K, V>(m);
    }

    /**
     * Gets locked set wrapping given set.
     *
     * @param s Set to wrap into locked implementation.
     * @return Locked set.
     */
    public static <E> Set<E> lockedSet(Set<E> s) {
        return new LockedSet<E>(s);
    }

    /**
     * Gets locked collection wrapping given set.
     *
     * @param c Collection to wrap into locked implementation.
     * @return Locked collection.
     */
    public static <E> Collection<E> lockedCollection(Collection<E> c) {
        return new LockedCollection<E>(c);
    }

    /**
     * Synchronized map.
     */
    private static final class LockedMap<K, V> extends ReentrantLock implements Map<K, V> {
        /** Delegate map. */
        private final Map<K, V> m;

        /**
         * @param m Map.
         */
        private LockedMap(Map<K, V> m) {
            this.m = m;
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            lock();

            try {
                m.clear();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int size() {
            lock();

            try {
                return m.size();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            lock();

            try {
                return m.isEmpty();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object key) {
            lock();

            try {
                return m.containsKey(key);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object val) {
            lock();

            try {
                return m.containsValue(val);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public V get(Object key) {
            lock();

            try {
                return m.get(key);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public V put(K key, V val) {
            lock();

            try {
                return m.put(key, val);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public V remove(Object key) {
            lock();

            try {
                return m.remove(key);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void putAll(Map<? extends K, ? extends V> m) {
            lock();

            try {
                this.m.putAll(m);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Set<K> keySet() {
            lock();

            try {
                return new LockedSet<K>(m.keySet());
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Collection<V> values() {
            lock();

            try {
                return new LockedCollection<V>(m.values());
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K, V>> entrySet() {
            lock();

            try {
                return new LockedSet<Entry<K, V>>(m.entrySet());
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            lock();

            try {
                return m.equals(o);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            lock();

            try {
                return m.hashCode();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            lock();

            try {
                return m.toString();
            }
            finally {
                unlock();
            }
        }

        /**
         * Overrides write object.
         *
         * @param s Object output stream.
         * @throws IOException If failed.
         */
        private void writeObject(ObjectOutputStream s) throws IOException {
            lock();

            try {
                s.defaultWriteObject();
            }
            finally {
                unlock();
            }
        }
    }

    /**
     * Synchronized set.
     */
    private static final class LockedSet<E> extends LockedCollection<E> implements Set<E> {
        /**
         * @param s Set to wrap.
         */
        @SuppressWarnings({"TypeMayBeWeakened"})
        private LockedSet(Set<E> s) {
            super(s);
        }
    }

    /**
     * Synchronized list.
     */
    private static final class LockedList<E> extends LockedCollection<E> implements List<E> {
        /** List. */
        private final List<E> l;

        /**
         * @param l List to wrap.
         */
        private LockedList(List<E> l) {
            super(l);

            this.l = l;
        }

        /** {@inheritDoc} */
        @Override public void add(int index, E e) {
            lock();

            try {
                l.add(index, e);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean addAll(int idx, Collection<? extends E> c) {
            lock();

            try {
                return l.addAll(c);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public E get(int idx) {
            lock();

            try {
                return l.get(idx);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public E set(int idx, E e) {
            lock();

            try {
                return l.set(idx, e);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public E remove(int idx) {
            lock();

            try {
                return l.remove(idx);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int indexOf(Object o) {
            lock();

            try {
                return l.indexOf(o);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int lastIndexOf(Object o) {
            lock();

            try {
                return l.lastIndexOf(o);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public ListIterator<E> listIterator() {
            return l.listIterator(); // Must be synchronized manually by user.
        }

        /** {@inheritDoc} */
        @Override public ListIterator<E> listIterator(int idx) {
            return l.listIterator(idx); // Must be synchronized manually by user.
        }

        /** {@inheritDoc} */
        @Override public List<E> subList(int fromIdx, int toIdx) {
            lock();

            try {
                return new LockedList<E>(l.subList(fromIdx, toIdx));
            }
            finally {
                unlock();
            }
        }
    }

    /**
     * Synchronized collection.
     */
    private static class LockedCollection<E> extends ReentrantLock implements Collection<E> {
        /** Delegating collection. */
        protected final Collection<E> c;

        /**
         * @param c Delegating collection.
         */
        private LockedCollection(Collection<E> c) {
            this.c = c;
        }

        /** {@inheritDoc} */
        @Override public boolean add(E e) {
            lock();

            try {
                return c.add(e);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int size() {
            lock();

            try {
                return c.size();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            lock();

            try {
                return c.isEmpty();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            lock();

            try {
                return c.contains(o);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Iterator<E> iterator() {
            return c.iterator(); // Must be manually synced by user.
        }

        /** {@inheritDoc} */
        @Override public Object[] toArray() {
            lock();

            try {
                return c.toArray();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"SuspiciousToArrayCall"})
        @Override public <T> T[] toArray(T[] a) {
            lock();

            try {
                return c.toArray(a);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            lock();

            try {
                return c.remove(o);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean containsAll(Collection<?> c) {
            lock();

            try {
                return this.c.containsAll(c);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean addAll(Collection<? extends E> c) {
            lock();

            try {
                return this.c.addAll(c);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean removeAll(Collection<?> c) {
            lock();

            try {
                return this.c.removeAll(c);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean retainAll(Collection<?> c) {
            lock();

            try {
                return this.c.retainAll(c);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            lock();

            try {
                c.clear();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            lock();

            try {
                return c.hashCode();
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            lock();

            try {
                return c.equals(o);
            }
            finally {
                unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            lock();

            try {
                return c.toString();
            }
            finally {
                unlock();
            }
        }

        /**
         * Overrides write object.
         *
         * @param s Object output stream.
         * @throws IOException If failed.
         */
        private void writeObject(ObjectOutputStream s) throws IOException {
            lock();

            try {
                s.defaultWriteObject();
            }
            finally {
                unlock();
            }
        }
    }
}
