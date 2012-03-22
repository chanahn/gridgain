// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.task;

import org.gridgain.client.message.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestResponse.*;

/**
 * Command handler for API requests.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridTaskCommandHandler extends GridRestCommandHandlerAdapter {
    /** Default maximum number of task results. */
    private static final int DFLT_MAX_TASK_RESULTS = 10240;

    /** Maximum number of task results. */
    private final int maxTaskResults = Integer.getInteger(GridSystemProperties.GG_REST_MAX_TASK_RESULTS,
        DFLT_MAX_TASK_RESULTS);

    /** Task results. */
    private final Map<GridUuid, TaskDescriptor> taskDescs = new GridConcurrentLinkedHashMap<GridUuid, TaskDescriptor>(16, 0.75f, 4, false,
        new P2<GridConcurrentLinkedHashMap<GridUuid, TaskDescriptor>,
            GridConcurrentLinkedHashMap.HashEntry<GridUuid, TaskDescriptor>>() {
            @Override public boolean apply(
                GridConcurrentLinkedHashMap<GridUuid, TaskDescriptor> map,
                GridConcurrentLinkedHashMap.HashEntry<GridUuid, TaskDescriptor> e) {
                return map.sizex() > maxTaskResults;
            }
        });

    /** Topic ID generator. */
    private final AtomicLong topicIdGen = new AtomicLong();

    /**
     * @param ctx Context.
     */
    @SuppressWarnings("deprecation")
    public GridTaskCommandHandler(final GridKernalContext ctx) {
        super(ctx);

        ctx.io().addMessageListener(TOPIC_REST, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (!(msg instanceof TaskResultRequest)) {
                    U.warn(log, "Received unexpected message instead of task result request: " + msg);

                    return;
                }

                TaskResultRequest req = (TaskResultRequest)msg;

                TaskResultResponse res = new TaskResultResponse();

                GridUuid taskId = req.taskId();

                TaskDescriptor desc = taskDescs.get(taskId);

                if (desc != null) {
                    res.found(true);
                    res.finished(desc.finished());

                    Throwable err = desc.error();

                    if (err != null)
                        res.error(err.getMessage());
                    else
                        res.result(desc.result());
                }
                else
                    res.found(false);

                try {
                    ctx.io().send(nodeId, req.topic(), res, SYSTEM_POOL);
                }
                catch (GridException e) {
                    U.error(log, "Failed to send job task result response.", e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridRestCommand cmd) {
        switch (cmd) {
            case EXE:
            case RESULT:
            case NOOP:
                return true;

            default:
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(final GridRestRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling task REST request: " + req);

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<GridRestResponse>(ctx);

        final GridRestResponse res = new GridRestResponse();

        final GridClientTaskResultBean taskRestRes = new GridClientTaskResultBean();

        switch (req.getCommand()) {
            case EXE: {
                final boolean async = Boolean.parseBoolean((String)value("async", req));

                final String name = value("name", req);

                if (F.isEmpty(name)) {
                    res.setError(missingParameter("name"));
                    res.setSuccessStatus(STATUS_FAILED);

                    return new GridFinishedFuture<GridRestResponse>(ctx, res);
                }

                List<Object> params = values("p", req);

                String s = value("timeout", req);

                long timeout = 0;

                if (s != null) {
                    try {
                        timeout = Long.parseLong(s);
                    }
                    catch (NumberFormatException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to parse timeout parameter: " + e.getMessage());
                    }
                }

                final GridTaskFuture<Object> taskFut = ctx.grid().execute(name,
                    !params.isEmpty() ? params.size() == 1 ? params.get(0) : params.toArray() : null, timeout);

                final GridUuid tid = taskFut.getTaskSession().getId();

                taskDescs.put(tid, new TaskDescriptor(false, null, null));

                taskRestRes.setId(tid.toString() + '~' + ctx.localNodeId().toString());

                taskFut.listenAsync(new GridInClosure<GridFuture<Object>>() {
                    @Override public void apply(GridFuture<Object> f) {
                        GridUuid tid = taskFut.getTaskSession().getId();

                        TaskDescriptor desc;

                        try {
                            desc = new TaskDescriptor(true, f.get(), null);
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to execute task [name=" + name + ", clientId=" + req.getClientId() +
                                ']', e);

                            desc = new TaskDescriptor(true, null, e);
                        }

                        taskDescs.put(tid, desc);

                        if (!async) {
                            if (desc.error() == null) {
                                taskRestRes.setFinished(true);
                                taskRestRes.setResult(desc.result());

                                res.setSuccessStatus(STATUS_SUCCESS);
                                res.setResponse(taskRestRes);
                            }
                            else {
                                res.setSuccessStatus(STATUS_FAILED);
                                res.setError(desc.error().getMessage());
                            }

                            fut.onDone(res);
                        }
                    }
                });

                if (async) {
                    res.setSuccessStatus(STATUS_SUCCESS);
                    res.setResponse(taskRestRes);

                    fut.onDone(res);
                }

                break;
            }

            case RESULT:
                String id = value("id", req);

                if (F.isEmpty(id)) {
                    res.setSuccessStatus(STATUS_FAILED);
                    res.setError(missingParameter("id"));

                    fut.onDone(res);

                    break;
                }

                StringTokenizer st = new StringTokenizer(id, "~");

                if (st.countTokens() != 2) {
                    res.setSuccessStatus(STATUS_FAILED);
                    res.setError("Failed to parse id parameter: " + id);

                    fut.onDone(res);

                    break;
                }

                String tidParam = st.nextToken();
                String resHolderIdParam = st.nextToken();

                taskRestRes.setId(id);

                try {
                    GridUuid tid = !F.isEmpty(tidParam) ? GridUuid.fromString(tidParam) : null;

                    UUID resHolderId = !F.isEmpty(resHolderIdParam) ? UUID.fromString(resHolderIdParam) : null;

                    if (tid == null || resHolderId == null) {
                        res.setSuccessStatus(STATUS_FAILED);
                        res.setError("Failed to parse id parameter: " + id);

                        fut.onDone(res);

                        break;
                    }

                    if (ctx.localNodeId().equals(resHolderId)) {
                        TaskDescriptor desc = taskDescs.get(tid);

                        if (desc != null) {
                            taskRestRes.setFinished(desc.finished());

                            if (desc.error() != null)
                                taskRestRes.setError(desc.error().getMessage());
                            else
                                taskRestRes.setResult(desc.result());

                            res.setSuccessStatus(STATUS_SUCCESS);
                            res.setResponse(taskRestRes);
                        }
                        else {
                            res.setSuccessStatus(STATUS_FAILED);
                            res.setError("Task with provided id has never been started on provided node [taskId=" +
                                tidParam + ", taskResHolderId=" + resHolderIdParam + ']');
                        }
                    }
                    else {
                        GridTuple2<String, TaskResultResponse> t = requestTaskResult(resHolderId, tid);

                        if (t.get1() != null) {
                            res.setSuccessStatus(STATUS_FAILED);
                            res.setError(t.get1());
                        }
                        else {
                            TaskResultResponse taskRes = t.get2();

                            assert taskRes != null;

                            if (taskRes.found()) {
                                taskRestRes.setFinished(taskRes.finished());

                                if (taskRes.error() != null)
                                    taskRestRes.setError(taskRes.error());
                                else
                                    taskRestRes.setResult(taskRes.result());

                                res.setSuccessStatus(STATUS_SUCCESS);
                                res.setResponse(taskRestRes);
                            }
                            else {
                                res.setSuccessStatus(STATUS_FAILED);
                                res.setError("Task with provided id has never been started on provided node " +
                                    "[taskId=" + tidParam + ", taskResHolderId=" + resHolderIdParam + ']');
                            }
                        }
                    }
                }
                catch (IllegalArgumentException e) {
                    String msg = "Failed to parse parameters [taskId=" + tidParam + ", taskResHolderId="
                        + resHolderIdParam + ", err=" + e.getMessage() + ']';

                    if (log.isDebugEnabled())
                        log.debug(msg);

                    res.setSuccessStatus(STATUS_FAILED);
                    res.setError(msg);
                }

                fut.onDone(res);

                break;

            case NOOP:
                final GridRestResponse emptyRes = new GridRestResponse();

                emptyRes.setSuccessStatus(STATUS_SUCCESS);

                fut.onDone(emptyRes);

                break;

            default:
                assert false : "Invalid command for task handler: " + req;
        }

        if (log.isDebugEnabled())
            log.debug("Handled task REST request [res=" + res + ", req=" + req + ']');

        return fut;
    }

    /**
     * @param resHolderId Result holder.
     * @param taskId Task ID.
     * @return Response from task holder.
     */
    @SuppressWarnings("deprecation")
    private GridTuple2<String, TaskResultResponse> requestTaskResult(final UUID resHolderId, GridUuid taskId) {
        GridNode taskNode = ctx.discovery().node(resHolderId);

        if (taskNode == null)
            return F.t("Task result holder has left grid: " + resHolderId, null);

        // Tuple: error message-response.
        final GridTuple2<String, TaskResultResponse> t = F.t2();

        final Lock lock = new ReentrantLock();
        final Condition cond = lock.newCondition();

        GridMessageListener msgLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                String err = null;
                TaskResultResponse res = null;

                if (!(msg instanceof TaskResultResponse))
                    err = "Received unexpected message: " + msg;
                else if (!nodeId.equals(resHolderId))
                    err = "Received task result response from unexpected node [resHolderId=" + resHolderId +
                        ", nodeId=" + nodeId + ']';
                else
                    // Sender and message type are fine.
                    res = (TaskResultResponse)msg;

                lock.lock();

                try {
                    if (t.isEmpty()) {
                        t.set(err, res);

                        cond.signalAll();
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        };

        GridLocalEventListener discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                assert evt instanceof GridDiscoveryEvent &&
                    (evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) : "Unexpected event: " + evt;

                GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

                if (resHolderId.equals(discoEvt.eventNodeId())) {
                    lock.lock();

                    try {
                        if (t.isEmpty()) {
                            t.set("Node that originated task execution has left grid: " + resHolderId, null);

                            cond.signalAll();
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        };

        // 1. Create unique topic name and register listener.
        String topic = TOPIC_REST.name("task-result", "idx#" + Long.toString(topicIdGen.getAndIncrement()));

        try {
            ctx.io().addMessageListener(topic, msgLsnr);

            // 2. Send message.
            try {
                ctx.io().send(taskNode, TOPIC_REST, new TaskResultRequest(taskId, topic), SYSTEM_POOL);
            }
            catch (GridException e) {
                String errMsg = "Failed to send task result request [resHolderId=" + resHolderId +
                    ", err=" + e.getMessage() + ']';

                if (log.isDebugEnabled())
                    log.debug(errMsg);

                return F.t(errMsg, null);
            }

            // 3. Listen to discovery events.
            ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

            // 4. Check whether node has left before disco listener has been installed.
            taskNode = ctx.discovery().node(resHolderId);

            if (taskNode == null)
                return F.t("Task result holder has left grid: " + resHolderId, null);

            // 5. Wait for result.
            lock.lock();

            try {
                long netTimeout = ctx.config().getNetworkTimeout();

                if (t.isEmpty())
                    cond.await(netTimeout, MILLISECONDS);

                if (t.isEmpty())
                    t.set1("Timed out waiting for task result (consider increasing 'networkTimeout' " +
                        "configuration property) [resHolderId=" + resHolderId + ", netTimeout=" + netTimeout + ']');

                // Return result
                return t;
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();

                return F.t("Interrupted while waiting for task result.", null);
            }
            finally {
                lock.unlock();
            }
        }
        finally {
            ctx.io().removeMessageListener(topic, msgLsnr);
            ctx.event().removeLocalEventListener(discoLsnr);
        }
    }
    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskCommandHandler.class, this);
    }

    /**
     * Immutable task execution state descriptor.
     */
    private static class TaskDescriptor {
        /** */
        private final boolean finished;

        /** */
        private final Object res;

        /** */
        private final Throwable err;

        /**
         * @param finished Finished flag.
         * @param res Result.
         * @param err Error.
         */
        private TaskDescriptor(boolean finished, @Nullable Object res, @Nullable Throwable err) {
            this.finished = finished;
            this.res = res;
            this.err = err;
        }

        /**
         * @return {@code true} if finished.
         */
        public boolean finished() {
            return finished;
        }

        /**
         * @return Task result.
         */
        @Nullable public Object result() {
            return res;
        }

        /**
         * @return Error.
         */
        @Nullable public Throwable error() {
            return err;
        }
    }

    /**
     *
     */
    private static class TaskResultRequest implements Externalizable {
        /** Task ID. */
        private GridUuid taskId;

        /** Topic. */
        private String topic;

        /**
         * Public no-arg constructor for {@link Externalizable} support.
         */
        public TaskResultRequest() {
            // No-op.
        }

        /**
         * @param taskId Task ID.
         * @param topic Topic.
         */
        private TaskResultRequest(GridUuid taskId, String topic) {
            this.taskId = taskId;
            this.topic = topic;
        }

        /**
         * @return Task ID.
         */
        public GridUuid taskId() {
            return taskId;
        }

        /**
         * @param taskId Task ID.
         */
        public void taskId(GridUuid taskId) {
            assert taskId != null;

            this.taskId = taskId;
        }

        /**
         * @return Topic.
         */
        public String topic() {
            return topic;
        }

        /**
         * @param topic Topic.
         */
        public void topic(String topic) {
            assert topic != null;

            this.topic = topic;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, taskId);
            U.writeString(out, topic);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            taskId = U.readGridUuid(in);
            topic = U.readString(in);
        }
    }

    /**
     *
     */
    private static class TaskResultResponse implements Externalizable {
        /** Result. */
        private Object res;

        /** Finished flag. */
        private boolean finished;

        /** Flag indicating that task has ever been launched on node. */
        private boolean found;

        /** Error. */
        private String err;

        /**
         * Public no-arg constructor for {@link Externalizable} support.
         */
        public TaskResultResponse() {
            // No-op.
        }

        /**
         * @return Task result.
         */
        @Nullable public Object result() {
            return res;
        }

        /**
         * @param res Task result.
         */
        public void result(@Nullable Object res) {
            this.res = res;
        }

        /**
         * @return {@code true} if finished.
         */
        public boolean finished() {
            return finished;
        }

        /**
         * @param finished {@code true} if finished.
         */
        public void finished(boolean finished) {
            this.finished = finished;
        }

        /**
         * @return {@code true} if found.
         */
        public boolean found() {
            return found;
        }

        /**
         * @param found {@code true} if found.
         */
        public void found(boolean found) {
            this.found = found;
        }

        /**
         * @return Error.
         */
        public String error() {
            return err;
        }

        /**
         * @param err Error.
         */
        public void error(String err) {
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(res);
            out.writeBoolean(finished);
            out.writeBoolean(found);

            U.writeString(out, err);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            res = in.readObject();
            finished = in.readBoolean();
            found = in.readBoolean();

            err = U.readString(in);
        }
    }
}
