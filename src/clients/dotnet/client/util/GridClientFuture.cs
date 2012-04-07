// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;
    using System.Threading;
    using GridGain.Client;

    using Dbg = System.Diagnostics.Debug;

    /** <summary>Future base implementation.</summary> */
    internal abstract class GridClientFuture : IGridClientFuture {
        /**
         * <summary>
         * Synchronously waits for completion.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public abstract void WaitDone();

        /**
         * <summary>
         * Checks if future is done.</summary>
         *
         * <returns>Whether future is done.</returns>
         */
        public abstract bool IsDone {
            get;
        }

        /**
         * <summary>
         * Callback to notify that future is finished successfully.</summary>
         *
         * <param name="res">Result (can be <c>null</c>).</param>
         */
        public abstract void Done(Object res);

        /**
         * <summary>
         * Callback to notify that future is finished with error. 
         * Note! Should always throw kind of GridClientException.</summary>
         *
         * <param name="err">Error (can't be <c>null</c>).</param>
         */
        public abstract void Fail(Action err);
    }

    /** <summary>Future generic implementation.</summary> */
    internal class GridClientFuture<T> : GridClientFuture, IGridClientFuture<T> {
        /** <summary>Latch.</summary> */
        private readonly EventWaitHandle doneLatch = new ManualResetEvent(false);

        /** <summary>Done flag.</summary> */
        private volatile bool done = false;

        /** <summary>Future task execution result.</summary> */
        private T res;

        /** <summary>Error callback, should always throw GridClientException.</summary> */
        private volatile Action err;

        /** <summary>Callback delegate to call on future finishes.</summary> */
        public Action DoneCallback {
            get;
            set;
        }

        /** <summary>Successfull done converter from object to expected type.</summary> */
        public Func<Object, T> DoneConverter {
            get;
            set;
        }

        /**
         * <summary>
         * Synchronously waits for completion.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public override void WaitDone() {
            doneLatch.WaitOne();

            if (err == null)
                return;
            
            // Throw an exception with correct stacktrace.
            err();

            // Throw an exception if exception's callback works incorrectly.
            throw new InvalidOperationException("Error callback is set, but doesn't throws an exception: " + err);
        }

        /**
         * <summary>
         * Synchronously waits for task completion and returns execution result.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public T Result {
            get {
                WaitDone();

                lock (this) {
                    return res;
                }
            }
        }

        /**
         * <summary>
         * Checks if future is done.</summary>
         *
         * <returns>Whether future is done.</returns>
         */
        public override bool IsDone {
            get {
                return done;
            }
        }

        /**
         * <summary>
         * Callback to notify that future is finished successfully.</summary>
         *
         * <param name="res">Result (can be <c>null</c>).</param>
         */
        public override void Done(Object res) {
            try {
                Done(DoneConverter == null ? (T)res : DoneConverter(res));
            }
            catch (Exception e) {
                Fail(() => {
                    throw new GridClientException(e.Message, e);
                });
            }
        }

        /**
         * <summary>
         * Callback to notify that future is finished successfully.</summary>
         *
         * <param name="res">Result (can be <c>null</c>).</param>
         */
        public void Done(T res) {
            Done(() => this.res = res);
        }

        /**
         * <summary>
         * Callback to notify that future is finished with error.
         * Note! Pass in new exception instance to preserve exception stack trace.</summary>
         *
         * <param name="err">Error (can't be <c>null</c>).</param>
         */
        public override void Fail(Action err) {
            Dbg.Assert(err != null);

            Done(() => this.err = err);
        }

        /**
         * <summary>
         * Update future state on complete.</summary>
         *
         * <param name="updateCallback">Update delegate to set future result in thread-safe environment.</param>
         */
        private void Done(Action updateCallback) {
            lock (this) {
                if (done)
                    return;

                updateCallback();

                // Mark done AFTER update performed due to exception can happens.
                done = true;

                doneLatch.Set();
            }

            if (DoneCallback != null)
                DoneCallback();
        }
    }
}
