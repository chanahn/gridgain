// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;

    /**
     * <summary>
     * A data projection of grid client. Contains various methods for cache operations
     * and metrics retrieval.</summary>
     */
    public interface IGridClientData {
        /**
         * <summary>
         * Gets name of the remote cache.</summary>
         *
         * <returns>Name of the remote cache.</returns>
         */
        String CacheName {
            get;
        }

        /**
         * <summary>
         * Gets client data which will only contact specified remote grid node. By default, remote node
         * is determined based on <see ctype="GridClientDataAffinity"/> provided - this method allows
         * to override default behavior and use only specified server for all cache operations.
         * <para/>
         * Use this method when there are other than <c>key-affinity</c> reasons why a certain
         * node should be contacted.</summary>
         *
         * <param name="node">Node to be contacted.</param>
         * <param name="nodes">Optional additional nodes.</param>
         * <returns>Client data which will only contact server with given node ID.</returns>
         * <exception cref="GridClientException">If resulting projection is empty.</exception>
         */
        IGridClientData PinNodes(IGridClientNode node, params IGridClientNode[] nodes);

        /**
         * <summary>
         * Gets pinned node or <c>null</c> if no node was pinned.</summary>
         *
         * <returns>Pinned node.</returns>
         */
        ICollection<IGridClientNode> PinnedNodes();

        /**
         * <summary>
         * Puts value to default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether value was actually put to cache.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Put<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Asynchronously puts value to default cache.</summary>
         *
         * <param name="key">key.</param>
         * <param name="val">Value.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<Boolean> PutAsync<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Puts entries to default cache.</summary>
         *
         * <param name="entries">Entries.</param>
         * <returns><c>True</c> if map contained more then one entry or if put succeeded in case of one entry,</returns>
         *      <c>false</c> otherwise
         * <exception cref="GridClientException">In case of error.</exception>
         */
        Boolean PutAll<TKey, TVal>(IDictionary<TKey, TVal> entries);

        /**
         * <summary>
         * Asynchronously puts entries to default cache.</summary>
         *
         * <param name="entries">Entries.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<Boolean> PutAllAsync<TKey, TVal>(IDictionary<TKey, TVal> entries);

        /**
         * <summary>
         * Gets value from default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Value.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        TVal GetItem<TKey, TVal>(TKey key);

        /**
         * <summary>
         * Asynchronously gets value from default cache.</summary>
         *
         * <param name="key">key.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<TVal> GetAsync<TKey, TVal>(TKey key);

        /**
         * <summary>
         * Gets entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <exception cref="GridClientException">In case of error.</exception>
         * <returns>Entries.</returns>
         */
        IDictionary<TKey, TVal> GetAll<TKey, TVal>(ICollection<TKey> keys);

        /**
         * <summary>
         * Asynchronously gets entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<IDictionary<TKey, TVal>> GetAllAsync<TKey, TVal>(ICollection<TKey> keys);

        /**
         * <summary>
         * Removes value from default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Whether value was actually removed.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Remove<TKey>(TKey key);

        /**
         * <summary>
         * Asynchronously removes value from default cache.</summary>
         *
         * <param name="key">key.</param>
         * <returns>Future, whether entry was actually removed.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<Boolean> RemoveAsync<TKey>(TKey key);

        /**
         * <summary>
         * Removes entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        void RemoveAll<TKey>(ICollection<TKey> keys);

        /**
         * <summary>
         * Asynchronously removes entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <returns>Future, whether operation finishes.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture RemoveAllAsync<TKey>(ICollection<TKey> keys);

        /**
         * <summary>
         * Replaces value in default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether value was actually replaced.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Replace<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Asynchronously replaces value in default cache.</summary>
         *
         * <param name="key">key.</param>
         * <param name="val">Value.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<Boolean> ReplaceAsync<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Sets entry value to <c>val1</c> if current value is <c>val1</c>.
         * <para/>
         * If <c>val1</c> is <c>val2</c> and <c>val2</c> is equal to current value,
         * entry is removed from cache.
         * <para/>
         * If <c>val2</c> is <c>val1</c>, entry is created if it doesn't exist.
         * <para/>
         * If both <c>val1</c> and <c>val1</c> are <c>val1</c>, entry is removed.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val1">Value to set.</param>
         * <param name="val2">Check value.</param>
         * <returns>Whether value of entry was changed.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Cas(String key, String val1, String val2);

        /**
         * <summary>
         * Asynchronously sets entry value to <c>val1</c> if current value is <c>val1</c>.
         * <para/>
         * If <c>val1</c> is <c>val2</c> and <c>val2</c> is equal to current value,
         * entry is removed from cache.
         * <para/>
         * If <c>val2</c> is <c>val1</c>, entry is created if it doesn't exist.
         * <para/>
         * If both <c>val1</c> and <c>null</c> are <c>null</c>, entry is removed.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val1">Value to set.</param>
         * <param name="val2">Check value.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<Boolean> CasAsync(String key, String val1, String val2);

        /**
         * <summary>
         * Gets affinity node ID for provided key. This method will return <c>null</c> if no
         * affinity was configured for the given cache or there are no nodes in topology with
         * cache enabled.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Node ID.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        Guid Affinity<TKey>(TKey key);

        /**
         * <summary>
         * Gets metrics for default cache.</summary>
         *
         * <returns>Cache metrics.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientDataMetrics Metrics();

        /**
         * <summary>
         * Asynchronously gets metrics for default cache.</summary>
         *
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<IGridClientDataMetrics> MetricsAsync();

        /**
         * <summary>
         * Gets metrics for entry.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Entry metrics.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientDataMetrics Metrics<TKey>(TKey key);

        /**
         * <summary>
         * Asynchronously gets metrics for entry.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        IGridClientFuture<IGridClientDataMetrics> MetricsAsync<TKey>(TKey key);
    }
}
