// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    /** <summary>Future for asynchronous operations.</summary> */
    public interface IGridClientFuture {
        /**
         * <summary>
         * Synchronously waits for task completion.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        void WaitDone();

        /** <summary>Future is done flag.</summary> */
        bool IsDone {
            get;
        }
    }

    /** <summary>Generic future with result for asynchronous operations.</summary> */
    public interface IGridClientFuture<T> : IGridClientFuture {
        /**
         * <summary>
         * Synchronously waits for task completion and returns execution result.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        T Result {
            get;
        }
    }
}
