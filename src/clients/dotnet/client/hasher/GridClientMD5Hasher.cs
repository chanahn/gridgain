// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    using System;
    using System.Security.Cryptography;

    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary>MD5 based hasher function.</summary> */
    public class GridClientMD5Hasher : IGridClientHasher {
        /** <inheritdoc /> */
        public int Hash(byte[] data) {
            return U.BytesToInt32(MD5.Create().ComputeHash(data), 0);
        }

        /** <inheritdoc /> */
        override public String ToString() {
            return "MD5 Hasher.";
        }
    }
}
