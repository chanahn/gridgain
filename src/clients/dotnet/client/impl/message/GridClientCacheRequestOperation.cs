// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    /** <summary>Available cache operations.</summary> */
    internal enum GridClientCacheRequestOperation {
        /** <summary>Cache put.</summary> */
        Put = 0x01,

        /** <summary>Cache put all.</summary> */
        PutAll = 0x02,

        /** <summary>Cache get.</summary> */
        Get = 0x03,

        /** <summary>Cache get all.</summary> */
        GetAll = 0x04,

        /** <summary>Cache remove.</summary> */
        Rmv = 0x05,

        /** <summary>Cache remove all.</summary> */
        RmvAll = 0x06,

        /** <summary>Cache add (put only if not exists).</summary> */
        Add = 0x07,

        /** <summary>Cache replace (put only if exists).</summary> */
        Replace = 0x08,

        /** <summary>Cache compare and set.</summary> */
        Cas = 0x09,

        /** <summary>Cache metrics request.</summary> */
        Metrics = 0x0A
    }
}
