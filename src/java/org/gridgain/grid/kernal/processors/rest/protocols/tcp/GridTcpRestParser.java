// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.gridgain.client.marshaller.*;
import org.gridgain.client.marshaller.protobuf.*;
import org.gridgain.client.message.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.jdk.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridTcpRestPacket.*;

/**
 * Parser for extended memcache protocol. Handles parsing and encoding activity.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridTcpRestParser implements GridNioParser<GridClientMessage> {
    /** Ping message. */
    public static final GridClientMessage PING_MESSAGE = new GridClientPingPacket();

    /** Ping packet. */
    private static final byte[] PING_PACKET = new byte[] {(byte)0x90, 0x00, 0x00, 0x00, 0x00};

    /** Meta name for parser state. */
    private static final String PARSER_STATE_META_NAME = UUID.randomUUID().toString();

    /** JDK marshaller. */
    private final GridMarshaller jdkMarshaller = new GridJdkMarshaller();

    /** Hessian marshaller. */
    @GridToStringExclude
    private final GridClientMarshaller marshaller = new GridClientProtobufMarshaller();

    /** {@inheritDoc} */
    @Nullable @Override public GridClientMessage decode(GridNioSession ses, ByteBuffer buf) throws IOException,
        GridException {
        ParserState state = ses.removeMeta(PARSER_STATE_META_NAME);

        if (state == null)
            state = new ParserState();

        PacketType type = state.packetType();

        if (type == null) {
            byte hdr = buf.get(buf.position());

            switch (hdr) {
                case MEMCACHE_REQ_FLAG:
                    state.packet(new GridTcpRestPacket());
                    state.packetType(PacketType.MEMCACHE);

                    break;
                case GRIDGAIN_REQ_FLAG:
                    // Skip header.
                    buf.get();

                    state.packetType(PacketType.GRIDGAIN);

                    break;

                default:
                    throw new IOException("Failed to parse incoming packet (invalid packet start) [ses=" + ses +
                        ", b=" + Integer.toHexString(hdr & 0xFF) + ']');
            }
        }

        GridClientMessage result = null;

        switch (state.packetType()) {
            case MEMCACHE:
                result = parseMemcachePacket(ses, buf, state);

                break;
            case GRIDGAIN:
                result = parseCustomPacket(ses, buf, state);

                break;
        }

        if (result == null)
            // Packet was not fully parsed yet.
            ses.addMeta(PARSER_STATE_META_NAME, state);

        return result;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, GridClientMessage msg) throws IOException, GridException {
        assert msg != null;

        if (msg instanceof GridTcpRestPacket)
            return encodeMemcache((GridTcpRestPacket)msg);
        else if (msg == PING_MESSAGE)
            return ByteBuffer.wrap(PING_PACKET);
        else {
            byte[] data = marshaller.marshal(msg);

            assert data.length > 0;

            ByteBuffer res = ByteBuffer.allocate(data.length + 5);

            res.put(GRIDGAIN_REQ_FLAG);
            res.put(U.intToBytes(data.length));
            res.put(data);

            res.flip();

            return res;
        }
    }

    /**
     * Parses memcache protocol message.
     *
     * @param ses Session.
     * @param buf Buffer containing not parsed bytes.
     * @param state Current parser state.
     * @return Parsed packet.s
     * @throws IOException If packet cannot be parsed.
     * @throws GridException If deserialization error occurred.
     */
    @Nullable private GridClientMessage parseMemcachePacket(GridNioSession ses, ByteBuffer buf, ParserState state)
        throws IOException, GridException {
        assert state.packetType() == PacketType.MEMCACHE;
        assert state.packet() != null;
        assert state.packet() instanceof GridTcpRestPacket;

        GridTcpRestPacket req = (GridTcpRestPacket)state.packet();
        ByteArrayOutputStream tmp = state.buffer();
        int i = state.index();

        while (buf.remaining() > 0) {
            byte b = buf.get();

            if (i == 0)
                req.requestFlag(b);
            else if (i == 1)
                req.operationCode(b);
            else if (i == 2 || i == 3) {
                tmp.write(b);

                if (i == 3) {
                    req.keyLength(U.bytesToShort(tmp.toByteArray(), 0));

                    tmp.reset();
                }
            }
            else if (i == 4)
                req.extrasLength(b);
            else if (i >= 8 && i <= 11) {
                tmp.write(b);

                if (i == 11) {
                    req.totalLength(U.bytesToInt(tmp.toByteArray(), 0));

                    tmp.reset();
                }
            }
            else if (i >= 12 && i <= 15) {
                tmp.write(b);

                if (i == 15) {
                    req.opaque(tmp.toByteArray());

                    tmp.reset();
                }
            }
            else if (i >= HDR_LEN && i < HDR_LEN + req.extrasLength()) {
                tmp.write(b);

                if (i == HDR_LEN + req.extrasLength() - 1) {
                    req.extras(tmp.toByteArray());

                    tmp.reset();
                }
            }
            else if (i >= HDR_LEN + req.extrasLength() &&
                i < HDR_LEN + req.extrasLength() + req.keyLength()) {
                tmp.write(b);

                if (i == HDR_LEN + req.extrasLength() + req.keyLength() - 1) {
                    req.key(tmp.toByteArray());

                    tmp.reset();
                }
            }
            else if (i >= HDR_LEN + req.extrasLength() + req.keyLength() &&
                i < HDR_LEN + req.totalLength()) {
                tmp.write(b);

                if (i == HDR_LEN + req.totalLength() - 1) {
                    req.value(tmp.toByteArray());

                    tmp.reset();
                }
            }

            if (i == HDR_LEN + req.totalLength() - 1)
                // Assembled the packet.
                return assemble(ses, req);

            i++;
        }

        state.index(i);

        return null;
    }

    /**
     * Parses custom packet serialized by hessian marshaller.
     *
     * @param ses Session.
     * @param buf Buffer containing not parsed bytes.
     * @param state Parser state.
     * @return Parsed message.
     * @throws IOException If packet parsing or deserialization failed.
     */
    @Nullable private GridClientMessage parseCustomPacket(GridNioSession ses, ByteBuffer buf, ParserState state)
        throws IOException {
        assert state.packetType() == PacketType.GRIDGAIN;
        assert state.packet() == null;

        ByteArrayOutputStream tmp = state.buffer();

        int len = state.index();

        while (buf.remaining() > 0) {
            byte b = buf.get();

            if (len == 0) {
                tmp.write(b);

                if (tmp.size() == 4) {
                    len = U.bytesToInt(tmp.toByteArray(), 0);

                    tmp.reset();

                    if (len == 0)
                        return PING_MESSAGE;
                    else if (len < 0)
                        throw new IOException("Failed to parse incoming packet (invalid packet length) [ses=" + ses +
                            ", len=" + len + ']');

                    state.index(len);
                }
            }
            else {
                tmp.write(b);

                if (tmp.size() == len)
                    return marshaller.unmarshal(tmp.toByteArray());
            }
        }

        return null;
    }

    /**
     * Encodes memcache message to a raw byte array.
     *
     * @param msg Message being serialized.
     * @return Serialized message.
     * @throws GridException If serialization failed.
     */
    private ByteBuffer encodeMemcache(GridTcpRestPacket msg) throws GridException {
        GridByteArrayList res = new GridByteArrayList(HDR_LEN);

        int keyLength = 0;

        int keyFlags = 0;

        if (msg.key() != null) {
            ByteArrayOutputStream rawKey = new ByteArrayOutputStream();

            keyFlags = encodeObj(msg.key(), rawKey);

            msg.key(rawKey.toByteArray());

            keyLength = rawKey.size();
        }

        int dataLength = 0;

        int valFlags = 0;

        if (msg.value() != null) {
            ByteArrayOutputStream rawVal = new ByteArrayOutputStream();

            valFlags = encodeObj(msg.value(), rawVal);

            msg.value(rawVal.toByteArray());

            dataLength = rawVal.size();
        }

        int flagsLength = 0;

        if (msg.addFlags())// || keyFlags > 0 || valFlags > 0)
            flagsLength = FLAGS_LENGTH;

        res.add(MEMCACHE_RES_FLAG);

        res.add(msg.operationCode());

        // Cast is required due to packet layout.
        res.add((short)keyLength);

        // Cast is required due to packet layout.
        res.add((byte)flagsLength);

        // Data type is always 0x00.
        res.add((byte)0x00);

        res.add((short)msg.status());

        res.add(keyLength + flagsLength + dataLength);

        res.add(msg.opaque(), 0, msg.opaque().length);

        // CAS, unused.
        res.add(0L);

        assert res.size() == HDR_LEN;

        if (flagsLength > 0) {
            res.add((short) keyFlags);
            res.add((short) valFlags);
        }

        assert msg.key() == null || msg.key() instanceof byte[];
        assert msg.value() == null || msg.value() instanceof byte[];

        if (keyLength > 0)
            res.add((byte[])msg.key(), 0, ((byte[])msg.key()).length);

        if (dataLength > 0)
            res.add((byte[])msg.value(), 0, ((byte[])msg.value()).length);

        return ByteBuffer.wrap(res.entireArray());
    }

    /**
     * Validates incoming packet and deserializes all fields that need to be deserialized.
     *
     * @param ses Session on which packet is being parsed.
     * @param req Raw packet.
     * @return Same packet with fields deserialized.
     * @throws IOException If parsing failed.
     * @throws GridException If deserialization failed.
     */
    private GridClientMessage assemble(GridNioSession ses, GridTcpRestPacket req) throws IOException, GridException {
        byte[] extras = req.extras();

        // First, decode key and value, if any
        if (req.key() != null || req.value() != null) {
            short keyFlags = 0;
            short valFlags = 0;

            if (req.hasFlags()) {
                if (extras == null || extras.length < FLAGS_LENGTH)
                    throw new IOException("Failed to parse incoming packet (flags required for command) [ses=" +
                        ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

                keyFlags = U.bytesToShort(extras, 0);
                valFlags = U.bytesToShort(extras, 2);
            }

            if (req.key() != null) {
                assert req.key() instanceof byte[];

                byte[] rawKey = (byte[])req.key();

                // Only values can be hessian-encoded.
                req.key(decodeObj(keyFlags, rawKey));
            }

            if (req.value() != null) {
                assert req.value() instanceof byte[];

                byte[] rawVal = (byte[])req.value();

                req.value(decodeObj(valFlags, rawVal));
            }
        }

        if (req.hasExpiration()) {
            if (extras == null || extras.length < 8)
                throw new IOException("Failed to parse incoming packet (expiration value required for command) [ses=" +
                    ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

            req.expiration(U.bytesToInt(extras, 4) & 0xFFFFFFFFL);
        }

        if (req.hasInitial()) {
            if (extras == null || extras.length < 16)
                throw new IOException("Failed to parse incoming packet (initial value required for command) [ses=" +
                    ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

            req.initial(U.bytesToLong(extras, 8));
        }

        if (req.hasDelta()) {
            if (extras == null || extras.length < 8)
                throw new IOException("Failed to parse incoming packet (delta value required for command) [ses=" +
                    ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

            req.delta(U.bytesToLong(extras, 0));
        }

        if (extras != null) {
            // Clients that include cache name must always include flags.
            int length = 4;

            if (req.hasExpiration())
                length += 4;

            if (req.hasDelta())
                length += 8;

            if (req.hasInitial())
                length += 8;

            if (extras.length - length > 0) {
                byte[] cacheName = new byte[extras.length - length];

                System.arraycopy(extras, length, cacheName, 0, extras.length - length);

                req.cacheName(new String(cacheName));
            }
        }

        return req;
    }

    /**
     * Decodes value from a given byte array to the object according to the flags given.
     *
     * @param flags Flags.
     * @param bytes Byte array to decode.
     * @return Decoded value.
     * @throws GridException If deserialization failed.
     */
    private Object decodeObj(short flags, byte[] bytes) throws GridException {
        assert bytes != null;

        if ((flags & SERIALIZED_FLAG) != 0)
            return jdkMarshaller.unmarshal(new ByteArrayInputStream(bytes), null);

        int masked = flags & 0xff00;

        switch (masked) {
            case BOOLEAN_FLAG:
                return bytes[0] == '1';
            case INT_FLAG:
                return U.bytesToInt(bytes, 0);
            case LONG_FLAG:
                return U.bytesToLong(bytes, 0);
            case DATE_FLAG:
                return new Date(U.bytesToLong(bytes, 0));
            case BYTE_FLAG:
                return bytes[0];
            case FLOAT_FLAG:
                return Float.intBitsToFloat(U.bytesToInt(bytes, 0));
            case DOUBLE_FLAG:
                return Double.longBitsToDouble(U.bytesToLong(bytes, 0));
            case BYTE_ARR_FLAG:
                return bytes;
            default:
                return new String(bytes);
        }
    }

    /**
     * Encodes given object to a byte array and returns flags that describe the type of serialized object.
     *
     * @param obj Object to serialize.
     * @param out Output stream to which object should be written.
     * @return Serialization flags.
     * @throws GridException If JDK serialization failed.
     */
    private int encodeObj(Object obj, ByteArrayOutputStream out) throws GridException {
        int flags = 0;

        byte[] data = null;

        if (obj instanceof String) {
            data = ((String)obj).getBytes();
        }
        else if (obj instanceof Boolean) {
            data = new byte[] {(byte)((Boolean)obj ? '1' : '0')};

            flags |= BOOLEAN_FLAG;
        }
        else if (obj instanceof Integer) {
            data = U.intToBytes((Integer)obj);

            flags |= INT_FLAG;
        }
        else if (obj instanceof Long) {
            data = U.longToBytes((Long)obj);

            flags |= LONG_FLAG;
        }
        else if (obj instanceof Date) {
            data = U.longToBytes(((Date)obj).getTime());

            flags |= DATE_FLAG;
        }
        else if (obj instanceof Byte) {
            data = new byte[] {(Byte)obj};

            flags |= BYTE_FLAG;
        }
        else if (obj instanceof Float) {
            data = U.intToBytes(Float.floatToIntBits((Float)obj));

            flags |= FLOAT_FLAG;
        }
        else if (obj instanceof Double) {
            data = U.longToBytes(Double.doubleToLongBits((Double)obj));

            flags |= DOUBLE_FLAG;
        }
        else if (obj instanceof byte[]) {
            data = (byte[])obj;

            flags |= BYTE_ARR_FLAG;
        }
        else {
            jdkMarshaller.marshal(obj, out);

            flags |= SERIALIZED_FLAG;
        }

        if (data != null)
            out.write(data, 0, data.length);

        return flags;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridTcpRestParser.class, this, "clientMarshaller", marshaller.getClass().getSimpleName());
    }

    /**
     * Type of message being parsed.
     */
    private enum PacketType {
        /** Memcache protocol message. */
        MEMCACHE,

        /** Custom hessian-serialized message. */
        GRIDGAIN
    }

    /**
     * Holder for parser state and temporary buffer.
     */
    private static class ParserState {
        /** Parser index. */
        private int idx;

        /** Temporary data buffer. */
        private ByteArrayOutputStream buf = new ByteArrayOutputStream();

        /** Packet being assembled. */
        private GridClientMessage packet;

        /** Packet type. */
        private PacketType packetType;

        /**
         * @return Stored parser index.
         */
        private int index() {
            return idx;
        }

        /**
         * @param idx Index to store.
         */
        private void index(int idx) {
            this.idx = idx;
        }

        /**
         * @return Temporary data buffer.
         */
        private ByteArrayOutputStream buffer() {
            return buf;
        }

        /**
         * @return Pending packet.
         */
        private GridClientMessage packet() {
            return packet;
        }

        /**
         * @param packet Pending packet.
         */
        private void packet(GridClientMessage packet) {
            assert this.packet == null;

            this.packet = packet;
        }

        /**
         * @return Pending packet type.
         */
        private PacketType packetType() {
            return packetType;
        }

        /**
         * @param packetType Pending packet type.
         */
        private void packetType(PacketType packetType) {
            this.packetType = packetType;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ParserState.class, this);
        }
    }
}
