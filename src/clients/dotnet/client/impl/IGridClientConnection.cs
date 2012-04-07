// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Net;
    using System.Collections.Generic;
    using GridGain.Client.Util;

    /**
     * <summary>
     * Facade for all possible network communications between client and server. Introduced to hide
     * protocol implementation (TCP, HTTP) from client code.</summary>
     */
    internal interface IGridClientConnection {
        /**
         * <summary>
         * Closes connection facade.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> this method will wait until all pending requests are handled.</param>
         */
        void Close(bool waitCompletion);

        /**
         * <summary>
         * Closes connection facade if no requests are in progress.</summary>
         *
         * <returns>Idle timeout.</returns>
         * <returns><c>True</c> if no requests were in progress and client was closed, <c>True</c> otherwise.</returns>
         */
        bool CloseIfIdle(TimeSpan timeout);

        /** <summary>Server address this connection linked to.</summary> */
        IPEndPoint ServerAddress {
            get;
        }

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>If value was actually put.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CachePut<K, V>(String cacheName, K key, V val);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <returns>Value.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<V> CacheGet<K, V>(String cacheName, K key);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <returns>Whether entry was actually removed.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheRemove<K>(String cacheName, K key);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="entries">Entries.</param>
         * <returns><c>True</c> if map contained more then one entry or if put succeeded in case of one entry,</returns>
         *      <c>false</c> otherwise
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CachePutAll<K, V>(String cacheName, IDictionary<K, V> entries);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="keys">Keys.</param>
         * <returns>Entries.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IDictionary<K, V>> CacheGetAll<K, V>(String cacheName, IEnumerable<K> keys);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="keys">Keys.</param>
         * <returns>Whether operation finishes.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture CacheRemoveAll<K>(String cacheName, IEnumerable<K> keys);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether entry was added.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheAdd<K, V>(String cacheName, K key, V val);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether value was actually replaced.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheReplace<K, V>(String cacheName, K key, V val);
        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <param name="val1">Value 1.</param>
         * <param name="val2">Value 2.</param>
         * <returns>Whether new value was actually set.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheCompareAndSet<K, V>(String cacheName, K key, V val1, V val2);

        /**
         * <summary>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="key">Key.</param>
         * <returns>Metrics.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IGridClientDataMetrics> CacheMetrics<K>(String cacheName, K key);

        /**
         * <summary>
         * Requests task execution with specified name.</summary>
         *
         * <param name="taskName">Task name.</param>
         * <param name="taskArg">Task argument.</param>
         * <returns>Task execution result.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<T> Execute<T>(String taskName, Object taskArg);

        /**
         * <summary>
         * Requests node by its ID.</summary>
         *
         * <param name="id">Node ID.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Node.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Requests node by its IP address.</summary>
         *
         * <param name="ipAddr">IP address.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Node.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IGridClientNode> Node(String ipAddr, bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Requests actual grid topology.</summary>
         *
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <returns>Nodes.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Requests server log entries in specified scope.</summary>
         *
         * <param name="path">Log file path.</param>
         * <param name="fromLine">Index of start line that should be retrieved.</param>
         * <param name="toLine">Index of end line that should be retrieved.</param>
         * <returns>Log file contents.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine);
    }
}
