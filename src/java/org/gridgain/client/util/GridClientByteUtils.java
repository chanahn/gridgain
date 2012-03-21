// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.util;

/**
 * Primitive to byte array and backward conversions.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public abstract class GridClientByteUtils {
    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        byte[] bytes = new byte[(Long.SIZE >> 3)];

        longToBytes(l, bytes, 0);

        return bytes;
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        int bytesCnt = Long.SIZE >> 3;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            bytes[off++] = (byte)(l >>> shift & 0xff);
        }

        return off;
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        byte[] bytes = new byte[(Integer.SIZE >> 3)];

        intToBytes(i, bytes, 0);

        return bytes;
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        int bytesCnt = Integer.SIZE >> 3;

        for (int j = 0; j < bytesCnt; j++) {
            int shift = bytesCnt - j - 1 << 3;

            bytes[off++] = (byte)(i >>> shift & 0xff);
        }

        return off;
    }

    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        byte[] bytes = new byte[(Short.SIZE >> 3)];

        shortToBytes(s, bytes, 0);

        return bytes;
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        int bytesCnt = Short.SIZE >> 3;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            bytes[off++] = (byte)(s >>> shift & 0xff);
        }

        return off;
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Short.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            // Just use the remainder.
            bytesCnt = bytes.length - off;

        short res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Integer.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            // Just use the remainder.
            bytesCnt = bytes.length - off;

        int res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Long.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            bytesCnt = bytes.length - off;

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }
}
