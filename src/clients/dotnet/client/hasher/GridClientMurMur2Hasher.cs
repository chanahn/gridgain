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

    /**
     * <summary>
     * Murmur2 hasher function based on endian-neutral implementation.
     * For more information refer to <a href="http://sites.google.com/site/murmurhash/">MurmurHash</a> website.
     * </summary>
     */
    public class GridClientMurMur2Hasher : IGridClientHasher {
        /** <summary>M.</summary> */
        private const long M = 0x5bd1e995L;

        /** <summary>R.</summary> */
        private const int R = 24;

        /** <summary>Seed.</summary> */
        private readonly int seed = GridClientConsistentHash.Prime;

        /** <inheritdoc /> */
        public int Hash(byte[] data) {
            int len = data.Length;

            int off = 0;

            ulong h = (uint)(seed ^ len);

            while (len >= 4) {
                ulong k = data[off++];

                k |= (uint)data[off++] << 8;
                k |= (uint)data[off++] << 16;
                k |= (uint)data[off++] << 24;

                k *= M;
                k ^= k >> R;
                k *= M;

                h *= M;
                h ^= k;

                len -= 4;
            }

            // Fall through.
            switch (len) {
                case 3: {
                    h ^= (uint) data[2] << 16;
                    break;
                }

                case 2: {
                    h ^= (uint) data[1] << 8;
                    break;
                }

                case 1: {
                    h ^= data[0];
                    h *= M;
                    break;
                }
            }

            h ^= h >> 13;
            h *= M;
            h ^= h >> 15;

            return (int)h;
        }

        /** <inheritdoc /> */
        override public String ToString() {
            return "Murmur Hasher.";
        }
    }
}
