// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    /**
     * <summary>
     * This class provides implementation for commit message fields
     * and cannot be used directly.</summary>
     */
    internal abstract class GridClientRequest {
        /** <summary>Request id.</summary> */
        public long RequestId {
            get;
            set;
        }

        /** <summary>Client id.</summary> */
        public Guid ClientId {
            get;
            set;
        }

        /** <summary>Client session token.</summary> */
        public byte[] SessionToken {
            get;
            set;
        }
    }
}
