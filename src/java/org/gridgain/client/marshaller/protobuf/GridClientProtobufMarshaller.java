// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.marshaller.protobuf;

import com.google.protobuf.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.client.message.*;
import org.gridgain.client.message.protobuf.*;
import org.gridgain.client.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.client.message.protobuf.ClientMessagesProtocols.*;
import static org.gridgain.client.message.protobuf.ClientMessagesProtocols.ObjectWrapperType.*;

/**
 * Client messages marshaller based on protocol buffers compiled code.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
@SuppressWarnings({"unchecked", "UnnecessaryFullyQualifiedName"})
public class GridClientProtobufMarshaller implements GridClientMarshaller {
    /** Empty byte array. */
    private static final byte[] EMPTY = new byte[0];

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object obj) throws IOException {
        if (!(obj instanceof GridClientMessage))
            throw new IOException("Message serialization of given type is not supported: " + obj.getClass().getName());

        GridClientMessage msg = (GridClientMessage)obj;

        ObjectWrapperType type = NULL;

        ByteString wrapped;

        if (obj instanceof GridClientResultBean) {
            GridClientResultBean bean = (GridClientResultBean)obj;

            ProtoResponse.Builder builder = ProtoResponse.newBuilder();

            builder.setRequestId(bean.requestId());

            if (bean.sessionToken() != null)
                builder.setSessionToken(ByteString.copyFrom(bean.sessionToken()));

            builder.setClientId(ByteString.copyFrom(wrapUUID(bean.clientId())));

            builder.setStatus(bean.successStatus());

            if (bean.errorMessage() != null)
                builder.setErrorMessage(bean.errorMessage());

            if (bean.result() != null)
                builder.setResult(wrapObject(bean.result()));

            wrapped = builder.build().toByteString();

            type = RESPONSE;
        }
        else {
            ProtoRequest.Builder reqBuilder = ProtoRequest.newBuilder();

            reqBuilder.setClientId(ByteString.copyFrom(wrapUUID(msg.clientId())));

            reqBuilder.setRequestId(msg.requestId());

            if (msg.sessionToken() != null)
                reqBuilder.setSessionToken(ByteString.copyFrom(msg.sessionToken()));

            if (obj instanceof GridClientAuthenticationRequest) {
                GridClientAuthenticationRequest req = (GridClientAuthenticationRequest)obj;

                ProtoAuthenticationRequest.Builder builder = ProtoAuthenticationRequest.newBuilder();

                builder.setCredentials(wrapObject(req.credentials()));

                reqBuilder.setBody(builder.build().toByteString());

                type = AUTH_REQUEST;
            }
            else if (obj instanceof GridClientCacheRequest) {
                GridClientCacheRequest req = (GridClientCacheRequest)obj;

                ProtoCacheRequest.Builder builder = ProtoCacheRequest.newBuilder();

                builder.setOperation(ProtoCacheRequest.GridCacheOperation.valueOf(req.operation().opCode()));

                if (req.cacheName() != null)
                    builder.setCacheName(req.cacheName());

                if (req.key() != null)
                    builder.setKey(wrapObject(req.key()));

                if (req.value() != null)
                    builder.setValue(wrapObject(req.value()));

                if (req.value2() != null)
                    builder.setValue2(wrapObject(req.value2()));

                if (req.values() != null && !req.values().isEmpty())
                    builder.setValues(wrapMap(req.values()));

                reqBuilder.setBody(builder.build().toByteString());

                type = CACHE_REQUEST;
            }
            else if (obj instanceof GridClientLogRequest) {
                GridClientLogRequest req = (GridClientLogRequest)obj;

                ProtoLogRequest.Builder builder = ProtoLogRequest.newBuilder();

                if (req.path() != null)
                    builder.setPath(req.path());

                builder.setFrom(req.from());
                builder.setTo(req.to());

                reqBuilder.setBody(builder.build().toByteString());

                type = LOG_REQUEST;
            }
            else if (obj instanceof GridClientTaskRequest) {
                GridClientTaskRequest req = (GridClientTaskRequest)obj;

                ProtoTaskRequest.Builder builder = ProtoTaskRequest.newBuilder();

                builder.setTaskName(req.taskName());

                builder.setArguments(wrapCollection(Arrays.asList(req.arguments())));

                reqBuilder.setBody(builder.build().toByteString());

                type = TASK_REQUEST;
            }
            else if (obj instanceof GridClientTopologyRequest) {
                GridClientTopologyRequest req = (GridClientTopologyRequest)obj;

                ProtoTopologyRequest.Builder builder = ProtoTopologyRequest.newBuilder();

                builder.setIncludeAttributes(req.includeAttributes());
                builder.setIncludeMetrics(req.includeMetrics());

                if (req.nodeId() != null)
                    builder.setNodeId(req.nodeId());

                if (req.nodeIp() != null)
                    builder.setNodeIp(req.nodeIp());

                reqBuilder.setBody(builder.build().toByteString());

                type = TOPOLOGY_REQUEST;
            }

            wrapped = reqBuilder.build().toByteString();
        }

        ObjectWrapper.Builder res = ObjectWrapper.newBuilder();

        res.setType(type);
        res.setBinary(wrapped);

        return res.build().toByteArray();
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        ObjectWrapper msg = ObjectWrapper.parseFrom(bytes);

        switch (msg.getType()) {
            case CACHE_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoCacheRequest reqBean = ProtoCacheRequest.parseFrom(req.getBody());

                GridClientCacheRequest res = new GridClientCacheRequest(GridClientCacheRequest.GridCacheOperation.
                    findByOperationCode(reqBean.getOperation().getNumber()));

                fillClientMessage(res, req);

                if (reqBean.hasCacheName())
                    res.cacheName(reqBean.getCacheName());

                if (reqBean.hasKey())
                    res.key(unwrapObject(reqBean.getKey()));

                if (reqBean.hasValue())
                    res.value(unwrapObject(reqBean.getValue()));

                if (reqBean.hasValue2())
                    res.value2(unwrapObject(reqBean.getValue2()));

                res.values(unwrapMap(reqBean.getValues()));

                return (T)res;
            }

            case TASK_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoTaskRequest reqBean = ProtoTaskRequest.parseFrom(req.getBody());

                GridClientTaskRequest res = new GridClientTaskRequest();

                fillClientMessage(res, req);

                res.taskName(reqBean.getTaskName());

                res.arguments(unwrapCollection(reqBean.getArguments()).toArray());

                return (T)res;
            }

            case LOG_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoLogRequest reqBean = ProtoLogRequest.parseFrom(req.getBody());

                GridClientLogRequest res = new GridClientLogRequest();

                fillClientMessage(res, req);

                if (reqBean.hasPath())
                    res.path(reqBean.getPath());

                res.from(reqBean.getFrom());

                res.to(reqBean.getTo());

                return (T)res;
            }

            case TOPOLOGY_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoTopologyRequest reqBean = ProtoTopologyRequest.parseFrom(req.getBody());

                GridClientTopologyRequest res = new GridClientTopologyRequest();

                fillClientMessage(res, req);

                if (reqBean.hasNodeId())
                    res.nodeId(reqBean.getNodeId());

                if (reqBean.hasNodeIp())
                    res.nodeIp(reqBean.getNodeIp());

                res.includeAttributes(reqBean.getIncludeAttributes());

                res.includeMetrics(reqBean.getIncludeMetrics());

                return (T)res;
            }

            case AUTH_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoAuthenticationRequest reqBean = ProtoAuthenticationRequest.parseFrom(req.getBody());

                GridClientAuthenticationRequest res = new GridClientAuthenticationRequest();

                fillClientMessage(res, req);

                res.credentials(unwrapObject(reqBean.getCredentials()));

                return (T)res;
            }

            case RESPONSE: {
                ProtoResponse resBean = ProtoResponse.parseFrom(msg.getBinary());

                GridClientResultBean res = new GridClientResultBean();

                res.requestId(resBean.getRequestId());

                if (resBean.hasSessionToken())
                    res.sessionToken(resBean.getSessionToken().toByteArray());

                res.successStatus(resBean.getStatus());

                res.clientId(unwrapUUID(resBean.getClientId().toByteArray()));

                if (resBean.hasErrorMessage())
                    res.errorMessage(resBean.getErrorMessage());

                if (resBean.hasResult())
                    res.result(unwrapObject(resBean.getResult()));

                return (T)res;
            }

            default:
                throw new IOException("Failed to unmarshall message (invalid message type was received): " +
                    msg.getType());
        }
    }

    /**
     * Fills given client message with values from packet.
     *
     * @param res Resulting message to fill.
     * @param req Packet to get values from.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void fillClientMessage(GridClientMessage res, ProtoRequest req) {
        res.requestId(req.getRequestId());
        res.clientId(unwrapUUID(req.getClientId().toByteArray()));

        if (req.hasSessionToken())
            res.sessionToken(req.getSessionToken().toByteArray());
    }

    /**
     * Converts node bean to protocol message.
     *
     * @param node Node bean to convert.
     * @return Converted message.
     * @throws IOException If node attribute cannot be converted.
     */
    private ProtoNodeBean wrapNode(GridClientNodeBean node) throws IOException {
        ProtoNodeBean.Builder builder = ProtoNodeBean.newBuilder();

        builder.setNodeId(node.getNodeId());

        builder.setTcpPort(node.getTcpPort());
        builder.setJettyPort(node.getJettyPort());

        builder.addAllExternalAddress(node.getExternalAddresses());
        builder.addAllInternalAddress(node.getInternalAddresses());

        if (node.getDefaultCacheMode() != null || node.getCaches() != null) {
            java.util.Map<String, String> caches = new HashMap<String, String>();

            if (node.getDefaultCacheMode() != null)
                caches.put(null, node.getDefaultCacheMode());

            if (node.getCaches() != null)
                caches.putAll(node.getCaches());

            builder.setCaches(wrapMap(caches));
        }

        if (node.getAttributes() != null && !node.getAttributes().isEmpty())
            builder.setAttributes(wrapMap(node.getAttributes()));

        if (node.getMetrics() != null) {
            ProtoNodeMetricsBean.Builder metricsBean = ProtoNodeMetricsBean.newBuilder();

            GridClientNodeMetricsBean metrics = node.getMetrics();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setFileSystemFreeSpace(metrics.getFileSystemFreeSpace());
            metricsBean.setFileSystemTotalSpace(metrics.getFileSystemTotalSpace());
            metricsBean.setFileSystemUsableSpace(metrics.getFileSystemUsableSpace());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setUpTime(metrics.getUpTime());

            builder.setMetrics(metricsBean.build());
        }

        return builder.build();
    }

    /**
     * Converts protocol message to a node bean.
     *
     * @param bean Protocol message.
     * @return Converted node bean.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private GridClientNodeBean unwrapNode(ProtoNodeBean bean) throws IOException {
        GridClientNodeBean res = new GridClientNodeBean();

        res.setNodeId(bean.getNodeId());

        res.setTcpPort(bean.getTcpPort());
        res.setJettyPort(bean.getJettyPort());

        res.setExternalAddresses(bean.getExternalAddressList());
        res.setInternalAddresses(bean.getInternalAddressList());

        if (bean.hasCaches()) {
            java.util.Map<String, String> caches = (java.util.Map<String, String>)unwrapMap(bean.getCaches());

            if (caches.containsKey(null))
                res.setDefaultCacheMode(caches.remove(null));

            if (!caches.isEmpty())
                res.setCaches(caches);
        }

        res.setAttributes((java.util.Map<String, Object>)unwrapMap(bean.getAttributes()));

        if (bean.hasMetrics()) {
            ProtoNodeMetricsBean metrics = bean.getMetrics();

            GridClientNodeMetricsBean metricsBean = new GridClientNodeMetricsBean();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setFileSystemFreeSpace(metrics.getFileSystemFreeSpace());
            metricsBean.setFileSystemTotalSpace(metrics.getFileSystemTotalSpace());
            metricsBean.setFileSystemUsableSpace(metrics.getFileSystemUsableSpace());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setUpTime(metrics.getUpTime());

            res.setMetrics(metricsBean);
        }

        return res;
    }

    /**
     * Wraps task result bean into a protocol message.
     *
     * @param bean Task result that need to be wrapped.
     * @return Wrapped message.
     * @throws IOException If result object cannot be serialized.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private ProtoTaskBean wrapTaskResult(GridClientTaskResultBean bean) throws IOException {
        ProtoTaskBean.Builder builder = ProtoTaskBean.newBuilder();

        builder.setId(bean.getId());
        builder.setFinished(bean.isFinished());

        if (bean.getError() != null)
            builder.setError(bean.getError());

        if (bean.getResult() != null)
            builder.setResult(wrapObject(bean.getResult()));

        return builder.build();
    }

    /**
     * Unwraps protocol message to a task result bean.
     *
     * @param bean Protocol message to unwrap.
     * @return Unwrapped message.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private GridClientTaskResultBean unwrapTaskResult(ProtoTaskBean bean) throws IOException {
        GridClientTaskResultBean res = new GridClientTaskResultBean();

        res.setId(bean.getId());
        res.setFinished(bean.getFinished());

        if (bean.hasError())
            res.setError(bean.getError());

        if (bean.hasResult())
            res.setResult(unwrapObject(bean.getResult()));

        return res;
    }

    /**
     * Wraps a collection of nodes to a sequence of messages.
     *
     * @param beans Beans collection to convert.
     * @return Collection of converted messages.
     * @throws IOException If some node attribute cannot be converted.
     */
    private Iterable<ProtoNodeBean> wrapNodes(java.util.Collection<GridClientNodeBean> beans) throws IOException {
        java.util.Collection<ProtoNodeBean> res = new ArrayList<ProtoNodeBean>(beans.size());

        for (GridClientNodeBean bean : beans)
            res.add(wrapNode(bean));

        return res;
    }

    /**
     * Unwraps a sequence of messages to a collection of nodes.
     *
     * @param beans Messages to convert.
     * @return Converted collection.
     * @throws IOException If message parsing failed.
     */
    private List<GridClientNodeBean> unwrapNodes(java.util.Collection<ProtoNodeBean> beans) throws IOException {
        List<GridClientNodeBean> res = new ArrayList<GridClientNodeBean>(beans.size());

        for (ProtoNodeBean bean : beans)
            res.add(unwrapNode(bean));

        return res;
    }

    /**
     * Converts java object to a protocol-understandable format.
     *
     * @param obj Object to convert.
     * @return Wrapped protocol message object.
     * @throws IOException If object of given type cannot be converted.
     */
    private ObjectWrapper wrapObject(Object obj) throws IOException {
        ObjectWrapper.Builder builder = ObjectWrapper.newBuilder();

        ObjectWrapperType type = NULL;
        ByteString data;

        if (obj == null) {
            builder.setType(type);

            builder.setBinary(ByteString.copyFrom(EMPTY));

            return builder.build();
        }
        else if (obj instanceof Boolean) {
            data = ByteString.copyFrom((Boolean)obj ? new byte[] {0x01} : new byte[] {0x00});

            type = BOOL;
        }
        else if (obj instanceof Byte) {
            data = ByteString.copyFrom(new byte[] {(Byte)obj});

            type = BYTE;
        }
        else if (obj instanceof Short) {
            data = ByteString.copyFrom(GridClientByteUtils.shortToBytes((Short)obj));

            type = SHORT;
        }
        else if (obj instanceof Integer) {
            data = ByteString.copyFrom(GridClientByteUtils.intToBytes((Integer)obj));

            type = INT32;
        }
        else if (obj instanceof Long) {
            data = ByteString.copyFrom(GridClientByteUtils.longToBytes((Long)obj));

            type = INT64;
        }
        else if (obj instanceof Float) {
            data = ByteString.copyFrom(GridClientByteUtils.intToBytes(Float.floatToIntBits((Float)obj)));

            type = FLOAT;
        }
        else if (obj instanceof Double) {
            data = ByteString.copyFrom(GridClientByteUtils.longToBytes(Double.doubleToLongBits((Double)obj)));

            type = DOUBLE;
        }
        else if (obj instanceof String) {
            data = ByteString.copyFrom((String)obj, "UTF-8");

            type = STRING;
        }
        else if (obj instanceof byte[]) {
            data = ByteString.copyFrom((byte[])obj);

            type = BYTES;
        }
        else if (obj instanceof java.util.Collection) {
            data = wrapCollection((java.util.Collection)obj).toByteString();

            type = COLLECTION;
        }
        else if (obj instanceof java.util.Map) {
            data = wrapMap((java.util.Map)obj).toByteString();

            type = MAP;
        }
        else if (obj instanceof GridClientNodeBean) {
            data = wrapNode((GridClientNodeBean)obj).toByteString();

            type = NODE_BEAN;
        }
        else if (obj instanceof GridClientTaskResultBean) {
            data = wrapTaskResult((GridClientTaskResultBean)obj).toByteString();

            type = TASK_BEAN;
        }
        // To-string conversion for special cases
        else if (obj instanceof Enum || obj instanceof InetAddress) {
            data = ByteString.copyFrom(obj.toString(), "UTF-8");
        }
        else
            throw new IOException("Failed to serialize object (object serialization of given type is not supported): "
                + obj.getClass().getName());

        builder.setType(type);
        builder.setBinary(data);

        return builder.build();
    }

    /**
     * Unwraps protocol value to a Java type.
     *
     * @param wrapper Wrapped value.
     * @return Corresponding Java object.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private Object unwrapObject(ObjectWrapper wrapper) throws IOException {
        ObjectWrapperType type = wrapper.getType();

        ByteString data = wrapper.getBinary();

        switch (type) {
            case NULL: {
                return null;
            }

            case BOOL: {
                return data.byteAt(0) != 0x00;
            }

            case BYTE: {
                return data.byteAt(0);
            }

            case SHORT: {
                return GridClientByteUtils.bytesToShort(data.toByteArray(), 0);
            }

            case INT32: {
                return GridClientByteUtils.bytesToInt(data.toByteArray(), 0);
            }

            case INT64: {
                return GridClientByteUtils.bytesToLong(data.toByteArray(), 0);
            }

            case FLOAT: {
                return Float.intBitsToFloat(GridClientByteUtils.bytesToInt(data.toByteArray(), 0));
            }

            case DOUBLE: {
                return Double.longBitsToDouble(GridClientByteUtils.bytesToLong(data.toByteArray(), 0));
            }

            case BYTES: {
                return data.toByteArray();
            }

            case STRING: {
                return data.toStringUtf8();
            }

            case COLLECTION: {
                return unwrapCollection(ClientMessagesProtocols.Collection.parseFrom(data));
            }

            case MAP: {
                return unwrapMap(ClientMessagesProtocols.Map.parseFrom(data));
            }

            case NODE_BEAN: {
                return unwrapNode(ProtoNodeBean.parseFrom(data));
            }

            case TASK_BEAN: {
                return unwrapTaskResult(ProtoTaskBean.parseFrom(data));
            }

            default:
                throw new IOException("Failed to unmarshall object (unsupported type): " + type);
        }
    }

    /**
     * Converts map to a sequence of key-value pairs.
     *
     * @param map Map to convert.
     * @return Sequence of key-value pairs.
     * @throws IOException If some key or value in map cannot be converted.
     */
    private ClientMessagesProtocols.Map wrapMap(java.util.Map<?, ?> map) throws IOException {
        ClientMessagesProtocols.Map.Builder builder = ClientMessagesProtocols.Map.newBuilder();

        if (map != null && !map.isEmpty()) {
            java.util.Collection<KeyValue> res = new ArrayList<KeyValue>(map.size());

            for (java.util.Map.Entry o : map.entrySet()) {
                KeyValue.Builder entryBuilder = KeyValue.newBuilder();

                entryBuilder.setKey(wrapObject(o.getKey())).setValue(wrapObject(o.getValue()));

                res.add(entryBuilder.build());
            }

            builder.addAllEntry(res);
        }

        return builder.build();
    }

    /**
     * Converts collection to a sequence of wrapped objects.
     *
     * @param col Collection to wrap.
     * @return Collection of wrapped objects.
     * @throws IOException If some element of collection cannot be converted.
     */
    private ClientMessagesProtocols.Collection wrapCollection(java.util.Collection<?> col) throws IOException {
        ClientMessagesProtocols.Collection.Builder  builder = ClientMessagesProtocols.Collection.newBuilder();

        java.util.Collection<ObjectWrapper> res = new ArrayList<ObjectWrapper>(col.size());

        for (Object o : col)
            res.add(wrapObject(o));

        builder.addAllItem(res);

        return builder.build();
    }

    /**
     * Converts collection of object wrappers to a sequence of java objects.
     *
     * @param col Collection of object wrappers.
     * @return Collection of java objects.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private java.util.Collection<?> unwrapCollection(ClientMessagesProtocols.Collection col) throws IOException {
        java.util.Collection<Object> res = new ArrayList<Object>(col.getItemCount());

        for (ObjectWrapper o : col.getItemList())
            res.add(unwrapObject(o));

        return res;
    }

    /**
     * Constructs a Java map from given key-value sequence.
     *
     * @param vals Key-value sequence.
     * @return Constructed map.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private java.util.Map<?, ?> unwrapMap(ClientMessagesProtocols.Map vals) throws IOException {
        java.util.Map<Object, Object> res = new HashMap<Object, Object>();

        for (KeyValue val : vals.getEntryList())
            res.put(unwrapObject(val.getKey()), unwrapObject(val.getValue()));

        return res;
    }

    /**
     * Converts UUID to a byte array.
     *
     * @param uuid UUID to convert.
     * @return Converted bytes.
     */
    private byte[] wrapUUID(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        byte[] buf = new byte[16];

        for (int i = 0; i < 8; i++)
            buf[i] = (byte) (msb >>> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            buf[i] = (byte) (lsb >>> 8 * (7 - i));

        return buf;
    }

    /**
     * Converts byte array to a UUID.
     *
     * @param byteArr UUID bytes.
     * @return Corresponding UUID.
     */
    private UUID unwrapUUID(byte[] byteArr) {
        long msb = 0;

        for (int i = 0; i < 8; i++)
            msb = (msb << 8) | (byteArr[i] & 0xff);

        long lsb = 0;

        for (int i = 8; i < 16; i++)
            lsb = (lsb << 8) | (byteArr[i] & 0xff);

        return new UUID(msb, lsb);
    }
}
