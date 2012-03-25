// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Wrapper for all grid messages.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.25032012
 */
public class GridIoMessage implements Externalizable {
    /** Sender ID. */
    private UUID sndId;

    /** Destination IDs. */
    @GridToStringInclude
    private List<UUID> destIds;

    /** Message topic. */
    private String topic;

    /** Topic ordinal. */
    private int topicOrd = -1;

    /** Message order. */
    private long msgId = -1;

    /** Message timeout. */
    private long timeout;

    /** Message body. */
    private GridByteArrayList msg;

    /** Message processing policy. */
    private GridIoPolicy plc;

    /** Message receive time. */
    private final long rcvTime = System.currentTimeMillis();

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public GridIoMessage() {
        // No-op.
    }

    /**
     * @param sndId Node ID.
     * @param destId Destination ID.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Communication message.
     * @param plc Thread policy.
     */
    public GridIoMessage(UUID sndId, UUID destId, String topic, int topicOrd, GridByteArrayList msg,
        GridIoPolicy plc) {
        this(sndId, Collections.singletonList(destId), topic, topicOrd, msg, plc);
    }

    /**
     * @param sndId Node ID.
     * @param destIds Destination IDs.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Communication message.
     * @param plc Thread policy.
     */
    public GridIoMessage(UUID sndId, List<UUID> destIds, String topic, int topicOrd,
        GridByteArrayList msg, GridIoPolicy plc) {
        assert sndId != null;
        assert destIds != null;
        assert topic != null;
        assert topicOrd <= Byte.MAX_VALUE;
        assert plc != null;
        assert msg != null;

        this.sndId = sndId;
        this.destIds = destIds;
        this.msg = msg;
        this.topic = topic;
        this.topicOrd = topicOrd;
        this.plc = plc;
    }

    /**
     * @param sndId Node ID.
     * @param destId Destination node ID.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Communication message.
     * @param plc Thread policy.
     * @param msgId Message ID.
     * @param timeout Timeout.
     */
    public GridIoMessage(UUID sndId, UUID destId, String topic, int topicOrd, GridByteArrayList msg,
        GridIoPolicy plc, long msgId, long timeout) {
        this(sndId, Collections.singletonList(destId), topic, topicOrd, msg, plc);

        this.msgId = msgId;
        this.timeout = timeout;
    }

    /**
     * @param sndId Node ID.
     * @param destIds Destination node IDs.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Communication message.
     * @param plc Thread policy.
     * @param msgId Message ID.
     * @param timeout Timeout.
     */
    public GridIoMessage(UUID sndId, List<UUID> destIds, String topic, int topicOrd,
        GridByteArrayList msg, GridIoPolicy plc, long msgId, long timeout) {
        this(sndId, destIds, topic, topicOrd, msg, plc);

        this.msgId = msgId;
        this.timeout = timeout;
    }

    /**
     * @return Topic.
     */
    String topic() {
        return topic;
    }

    /**
     * @return Topic ordinal.
     */
    int topicOrdinal() {
        return topicOrd;
    }

    /**
     * @return Message.
     */
    public GridByteArrayList message() {
        return msg;
    }

    /**
     * @return Policy.
     */
    GridIoPolicy policy() {
        return plc;
    }

    /**
     * @return Message ID.
     */
    long messageId() {
        return msgId;
    }

    /**
     * @return Message timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return {@code True} if message is ordered, {@code false} otherwise.
     */
    boolean isOrdered() {
        return msgId > 0;
    }

    /**
     * @return Sender node ID.
     */
    UUID senderId() {
        return sndId;
    }

    /**
     * @param sndId Sender ID.
     */
    void senderId(UUID sndId) {
        this.sndId = sndId;
    }

    /**
     * @return Gets destination node IDs.
     */
    Collection<UUID> destinationIds() {
        return destIds;
    }

    /**
     * @return Message receive time.
     */
    long receiveTime() {
        return rcvTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(msg);
        out.writeLong(msgId);
        out.writeLong(timeout);

        // Special enum handling.
        out.writeByte(plc.ordinal());

        U.writeUuid(out, sndId);

        out.writeByte(topicOrd);

        if (topicOrd < 0)
            U.writeString(out, topic);

        // Write destination IDs.
        out.writeInt(destIds.size());

        // Purposely don't use foreach loop for
        // better performance.
        for (UUID destId : destIds)
            U.writeUuid(out, destId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        msg = (GridByteArrayList)in.readObject();
        msgId = in.readLong();
        timeout = in.readLong();

        byte ord = in.readByte();

        // Account for incorrect message and check for positive enum ordinal.
        plc = ord >= 0 ? GridIoPolicy.fromOrdinal(ord) : null;

        sndId = U.readUuid(in);

        topicOrd = in.readByte();

        if (topicOrd < 0)
            topic = U.readString(in);
        else {
            GridTopic topic = GridTopic.fromOrdinal(topicOrd);

            if (topic == null)
                throw new IOException("Failed to deserialize grid topic from ordinal: " + topicOrd);

            this.topic = topic.name();
        }

        int size = in.readInt();

        if (size == 1)
            destIds = Collections.singletonList(U.readUuid(in));
        else {
            destIds = new ArrayList<UUID>(size);

            for (int i = 0; i < size; i++)
                destIds.add(U.readUuid(in));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof GridIoMessage))
            return false;

        GridIoMessage other = (GridIoMessage)obj;

        return plc == other.plc && topic.equals(other.topic) && msgId == other.msgId &&
            sndId.equals(other.sndId) && destIds.equals(other.destIds);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = topic.hashCode();

        res = 31 * res + (int)(msgId ^ (msgId >>> 32));
        res = 31 * res + msg.hashCode();
        res = 31 * res + plc.hashCode();
        res = 31 * res + sndId.hashCode();
        res = 31 * res + topic.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIoMessage.class, this);
    }
}
