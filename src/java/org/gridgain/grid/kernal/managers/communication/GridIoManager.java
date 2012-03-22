// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

/**
 * Grid communication manager.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridIoManager extends GridManagerAdapter<GridCommunicationSpi> {
    /** Max closed topics to store. */
    public static final int MAX_CLOSED_TOPICS = 10240;

    /** Listeners by topic. */
    private final ConcurrentMap<String, GridConcurrentHashSet<GridFilteredMessageListener>> lsnrMap =
        new ConcurrentHashMap<String, GridConcurrentHashSet<GridFilteredMessageListener>>();

    /** Internal worker pool. */
    private GridWorkerPool workerPool;

    /** Internal P2P pool. */
    private GridWorkerPool p2pPool;

    /** Internal system pool. */
    private GridWorkerPool sysPool;

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /** */
    private final ConcurrentMap<String, ConcurrentMap<UUID, GridCommunicationMessageSet>> msgSetMap =
        new ConcurrentHashMap<String, ConcurrentMap<UUID,GridCommunicationMessageSet>>();

    /** Messages ID generator (per topic). */
    private final ConcurrentMap<String, ConcurrentMap<UUID, AtomicLong>> msgIdMap =
        new ConcurrentHashMap<String, ConcurrentMap<UUID, AtomicLong>>();

    /** Closed topic names queue with the fixed size. */
    private final GridConcurrentLinkedHashMap<String, Integer> closedTopics =
        new GridConcurrentLinkedHashMap<String, Integer>(
            128,
            0.75f,
            16,
            false,
            new GridPredicate2<GridConcurrentLinkedHashMap<String, Integer>,
                GridConcurrentLinkedHashMap.HashEntry<String, Integer>>() {
                @Override public boolean apply(GridConcurrentLinkedHashMap<String, Integer> map,
                    GridConcurrentLinkedHashMap.HashEntry<String, Integer> entry) {
                    return map.sizex() > MAX_CLOSED_TOPICS;
                }
            });

    /** Local node ID. */
    private final UUID locNodeId;

    /** Discovery delay. */
    private final long discoDelay;

    /** Cache for messages that were received prior to discovery. */
    private final ConcurrentMap<UUID, GridConcurrentLinkedDeque<GridIoMessage>> discoWaitMap =
        new ConcurrentHashMap<UUID, GridConcurrentLinkedDeque<GridIoMessage>>();

    /** Disco wait map processing flag. */
    private final AtomicBoolean discoWaitMapProc = new AtomicBoolean();

    /** Communication message listener. */
    @SuppressWarnings("deprecation")
    private GridMessageListener msgLsnr;

    /** Grid marshaller. */
    private final GridMarshaller marsh;

    /** Busy lock. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Lock to sync maps access. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Message cache. */
    private ThreadLocal<GridTuple2<Object, GridByteArrayList>> cacheMsg =
        new GridThreadLocal<GridTuple2<Object, GridByteArrayList>>() {
            @Nullable @Override protected GridTuple2<Object, GridByteArrayList> initialValue() {
                return null;
            }
        };

    /** Default class loader. */
    private final ClassLoader dfltClsLdr = getClass().getClassLoader();

    /**
     * @param ctx Grid kernal context.
     */
    public GridIoManager(GridKernalContext ctx) {
        super(GridCommunicationSpi.class, ctx, ctx.config().getCommunicationSpi());

        locNodeId = ctx.config().getNodeId();

        discoDelay = ctx.config().getDiscoveryStartupDelay();

        marsh = ctx.config().getMarshaller();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void start() throws GridException {
        assertParameter(discoDelay > 0, "discoveryStartupDelay > 0");

        startSpi();

        workerPool = new GridWorkerPool(ctx.config().getExecutorService(), log);
        p2pPool = new GridWorkerPool(ctx.config().getPeerClassLoadingExecutorService(), log);
        sysPool = new GridWorkerPool(ctx.config().getSystemExecutorService(), log);

        getSpi().setListener(msgLsnr = new GridMessageListener() {
            @SuppressWarnings("deprecation")
            @Override public void onMessage(UUID nodeId, Object msg) {
                assert nodeId != null;
                assert msg != null;

                if (log.isDebugEnabled())
                    log.debug("Received communication message: " + msg);

                if (!(msg instanceof GridIoMessage)) {
                    U.error(log, "Communication manager received message of unknown type (will ignore): " +
                        msg.getClass().getName() + ". Most likely GridCommunicationSpi is being used directly, " +
                        "which is illegal - make sure to send messages only via GridProjection API.");

                    return;
                }

                GridIoMessage commMsg = (GridIoMessage)msg;

                // If discovery was not started, then it means that we got bound to the
                // same port as a previous node that was started on this IP and got a message
                // destined for a previous node.
                if (!commMsg.destinationIds().contains(locNodeId)) {
                    U.error(log, "Received message whose destination does not match this node (will ignore): " + msg);

                    return;
                }

                if (!nodeId.equals(commMsg.senderId())) {
                    U.error(log, "Expected node ID does not match the one in message " +
                        "[expected=" + nodeId + ", msgNodeId=" + commMsg.senderId() + ']');

                    return;
                }

                if (!busyLock.enterBusy()) {
                    if (log.isDebugEnabled())
                        log.debug("Received communication message while stopping grid: " + msg);

                    return;
                }

                try {
                    GridNode node = ctx.discovery().node(nodeId);

                    // Get the same ID instance as the node.
                    commMsg.senderId(nodeId = node == null ? nodeId : node.id());

                    // Although we check closed topics prior to processing
                    // every message, we still check it here to avoid redundant
                    // placement of messages on wait list whenever possible.
                    if (closedTopics.containsKey(commMsg.topic())) {
                        if (log.isDebugEnabled())
                            log.debug("Message is ignored as it came for the closed topic: " + msg);

                        return;
                    }

                    // Remove expired messages from wait list.
                    processWaitList();

                    if (node == null) {
                        // Received message before a node got discovered or after it left.
                        if (log.isDebugEnabled())
                            log.debug("Adding message to waiting list [senderId=" + nodeId + ", msg=" + msg + ']');

                        addToWaitList(commMsg);

                        return;
                    }

                    // If message is P2P, then process in P2P service.
                    // This is done to avoid extra waiting and potential deadlocks
                    // as thread pool may not have any available threads to give.
                    switch (commMsg.policy()) {
                        case P2P_POOL: {
                            processP2PMessage(nodeId, commMsg);

                            break;
                        }

                        case PUBLIC_POOL:
                        case SYSTEM_POOL: {
                            if (!commMsg.isOrdered())
                                processRegularMessage(nodeId, commMsg, commMsg.policy());
                            else
                                processOrderedMessage(nodeId, commMsg, commMsg.policy());

                            break;
                        }
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void onKernalStart0() throws GridException {
        discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                assert evt instanceof GridDiscoveryEvent : "Invalid event: " + evt;

                GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

                UUID nodeId = discoEvt.eventNodeId();

                switch (evt.type()) {
                    case EVT_NODE_JOINED:
                        Collection<GridIoMessage> delayedMsgs = new LinkedList<GridIoMessage>();

                        lock.writeLock().lock();

                        try {
                            GridConcurrentLinkedDeque<GridIoMessage> waitList = discoWaitMap.remove(nodeId);

                            if (log.isDebugEnabled())
                                log.debug("Processing messages from discovery startup delay list " +
                                    "(sender node joined topology): " + waitList);

                            if (waitList != null)
                                for (GridIoMessage msg : waitList)
                                    delayedMsgs.add(msg);
                        }
                        finally {
                            lock.writeLock().unlock();
                        }

                        // After write lock released.
                        if (!delayedMsgs.isEmpty())
                            for (GridIoMessage msg : delayedMsgs)
                                msgLsnr.onMessage(msg.senderId(), msg);

                        break;

                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        // Clean up delayed and ordered messages (need exclusive lock).
                        lock.writeLock().lock();

                        try {
                            GridConcurrentLinkedDeque<GridIoMessage> waitList = discoWaitMap.remove(nodeId);

                            if (log.isDebugEnabled())
                                log.debug("Removed messages from discovery startup delay list " +
                                    "(sender node left topology): " + waitList);

                            for (Map.Entry<String, ConcurrentMap<UUID, GridCommunicationMessageSet>> e :
                                msgSetMap.entrySet()) {
                                ConcurrentMap<UUID, GridCommunicationMessageSet> map = e.getValue();

                                GridCommunicationMessageSet set = map.remove(nodeId);

                                if (set != null) {
                                    if (log.isDebugEnabled())
                                        log.debug("Removed message set due to node leaving grid: " + set);

                                    // Unregister timeout listener.
                                    ctx.timeout().removeTimeoutObject(set);

                                    // Node may still send stale messages for this topic
                                    // even after discovery notification is done.
                                    closedTopics.put(set.topic(), 0);
                                }

                                if (map.isEmpty())
                                    msgSetMap.remove(e.getKey(), map);
                            }
                        }
                        finally {
                            lock.writeLock().unlock();
                        }

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }
            }
        };

        ctx.event().addLocalEventListener(discoLsnr,
            EVT_NODE_JOINED,
            EVT_NODE_LEFT,
            EVT_NODE_FAILED);

        // Make sure that there are no stale nodes due to window between communication
        // manager start and kernal start.
        // Need exclusive lock for that.
        Collection<GridIoMessage> delayedMsgs = new LinkedList<GridIoMessage>();

        lock.writeLock().lock();

        try {
            // 1. Process wait list.
            for (Map.Entry<UUID, GridConcurrentLinkedDeque<GridIoMessage>> e : discoWaitMap.entrySet()) {
                if (ctx.discovery().node(e.getKey()) != null) {
                    GridConcurrentLinkedDeque<GridIoMessage> waitList = discoWaitMap.remove(e.getKey());

                    if (log.isDebugEnabled())
                        log.debug("Processing messages from discovery startup delay list: " + waitList);

                    if (waitList != null)
                        for (GridIoMessage msg : waitList)
                            delayedMsgs.add(msg);
                }
            }

            // 2. Process messages sets.
            for (Map.Entry<String, ConcurrentMap<UUID, GridCommunicationMessageSet>> e :
                msgSetMap.entrySet()) {
                ConcurrentMap<UUID, GridCommunicationMessageSet> map = e.getValue();

                for (GridCommunicationMessageSet set : map.values()) {
                    if (ctx.discovery().node(set.nodeId()) == null) {
                        boolean b = map.remove(set.nodeId(), set);

                        assert b;

                        if (log.isDebugEnabled())
                            log.debug("Removed message set due to node leaving grid: " + set);

                        // Unregister timeout listener.
                        ctx.timeout().removeTimeoutObject(set);

                        // Node may still send stale messages for this topic
                        // even after discovery notification is done.
                        closedTopics.put(set.topic(), 0);
                    }
                }

                if (map.isEmpty())
                    msgSetMap.remove(e.getKey(), map);
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        // After write lock released.
        if (!delayedMsgs.isEmpty())
            for (GridIoMessage msg : delayedMsgs)
                msgLsnr.onMessage(msg.senderId(), msg);
    }

    /**
     * Adds new message to discovery wait list.
     *
     * @param msg Message to add.
     */
    private void addToWaitList(GridIoMessage msg) {
        lock.readLock().lock();

        try {
            GridConcurrentLinkedDeque<GridIoMessage> list =
                F.addIfAbsent(discoWaitMap, msg.senderId(), F.<GridIoMessage>newDeque());

            assert list != null;

            list.add(msg);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes expired messages from wait list.
     */
    private void processWaitList() {
        if (discoWaitMap.isEmpty())
            return;

        if (discoWaitMapProc.compareAndSet(false, true)) {
            lock.writeLock().lock();

            try {
                long now = System.currentTimeMillis();

                for (Iterator<GridConcurrentLinkedDeque<GridIoMessage>> iter1 = discoWaitMap.values().iterator();
                    iter1.hasNext();) {
                    GridConcurrentLinkedDeque<GridIoMessage> msgs = iter1.next();

                    assert msgs != null && !msgs.isEmpty();

                    for (Iterator<GridIoMessage> iter2 = msgs.iterator(); iter2.hasNext();) {
                        GridIoMessage msg = iter2.next();

                        if (now - msg.receiveTime() > discoDelay) {
                            if (log.isDebugEnabled())
                                log.debug("Removing expired message from discovery wait list. " +
                                    "This is normal when received a message after sender node has left the grid. " +
                                    "It also may happen (although rarely) if sender node has not been " +
                                    "discovered yet and 'GridConfiguration.getDiscoveryStartupDelay()' value is " +
                                    "too small. Make sure to increase this parameter " +
                                    "if you believe that message should have been processed. Removed message: " + msg);

                            iter2.remove();
                        }
                    }

                    if (msgs.isEmptyx())
                        iter1.remove();
                }
            }
            finally {
                discoWaitMapProc.set(false);

                lock.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel, boolean wait) {
        // No more communication messages.
        getSpi().setListener(null);

        busyLock.block();

        GridEventStorageManager evtMgr = ctx.event();

        if (evtMgr != null && discoLsnr != null)
            evtMgr.removeLocalEventListener(discoLsnr);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel, boolean wait) throws GridException {
        stopSpi();

        // Stop should be done in proper order.
        // First we stop regular workers,
        // Then ordered and urgent at the end.
        if (workerPool != null)
            workerPool.join(true);

        if (sysPool != null)
            sysPool.join(true);

        if (p2pPool != null)
            p2pPool.join(true);

        // Clear cache.
        cacheMsg.set(null);

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Gets execution pool for policy.
     *
     * @param plc Policy.
     * @return Execution pool.
     */
    private GridWorkerPool getPool(GridIoPolicy plc) {
        switch (plc) {
            case P2P_POOL:
                return p2pPool;
            case SYSTEM_POOL:
                return sysPool;
            case PUBLIC_POOL:
                return workerPool;

            default: {
                assert false : "Invalid communication policy: " + plc;

                // Never reached.
                return null;
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Urgent message.
     */
    @SuppressWarnings("deprecation")
    private void processP2PMessage(final UUID nodeId, final GridIoMessage msg) {
        assert msg.policy() == P2P_POOL;

        if (closedTopics.containsKey(msg.topic())) {
            if (log.isDebugEnabled())
                log.debug("Message is ignored because it came for the closed topic: " + msg);

            return;
        }

        final Set<GridFilteredMessageListener> lsnrs = lsnrMap.get(msg.topic());

        if (!F.isEmpty(lsnrs)) {
            final Runnable c = new Runnable() {
                @Override public void run() {
                    try {
                        Object obj = unmarshal(msg);

                        for (GridMessageListener lsnr : lsnrs)
                            lsnr.onMessage(nodeId, obj);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to deserialize P2P communication message:" + msg, e);
                    }
                }
            };

            try {
                p2pPool.execute(new GridWorker(ctx.config().getGridName(), "comm-mgr-urgent-worker", log) {
                    @Override protected void body() {
                        c.run();
                    }
                });
            }
            catch (GridExecutionRejectedException e) {
                U.error(log, "Failed to process P2P message due to execution rejection. Increase the upper bound " +
                    "on 'ExecutorService' provided by 'GridConfiguration.getPeerClassLoadingExecutorService()'. " +
                    "Will attempt to process message in the listener thread instead.", e);

                c.run();
            }
            catch (GridException e) {
                U.error(log, "Failed to process P2P message due to system error.", e);
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Regular message.
     * @param plc Execution policy.
     */
    @SuppressWarnings("deprecation")
    private void processRegularMessage(final UUID nodeId, final GridIoMessage msg, GridIoPolicy plc) {
        assert !msg.isOrdered();

        if (closedTopics.containsKey(msg.topic())) {
            if (log.isDebugEnabled())
                log.debug("Message is ignored because it came for the closed topic: " + msg);

            return;
        }

        final Set<GridFilteredMessageListener> lsnrs = lsnrMap.get(msg.topic());

        if (!F.isEmpty(lsnrs)) {
            final Runnable c = new Runnable() {
                @Override public void run() {
                    try {
                        Object obj = unmarshal(msg);

                        for (GridMessageListener lsnr : lsnrs)
                            lsnr.onMessage(nodeId, obj);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to deserialize regular communication message: " + msg, e);
                    }
                }
            };

            try {
                getPool(plc).execute(new GridWorker(ctx.config().getGridName(), "comm-mgr-unordered-worker", log) {
                    @Override protected void body() {
                        c.run();
                    }
                });
            }
            catch (GridExecutionRejectedException e) {
                U.error(log, "Failed to process regular message due to execution rejection. Increase the upper bound " +
                    "on 'ExecutorService' provided by 'GridConfiguration.getExecutorService()'. " +
                    "Will attempt to process message in the listener thread instead.", e);

                c.run();
            }
            catch (GridException e) {
                U.error(log, "Failed to process regular message due to system error.", e);
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Ordered message.
     * @param plc Execution policy.
     */
    @SuppressWarnings("TooBroadScope")
    private void processOrderedMessage(UUID nodeId, GridIoMessage msg, GridIoPolicy plc) {
        assert msg.isOrdered();

        assert msg.timeout() > 0 : "Message timeout of 0 should never be sent: " + msg;

        if (closedTopics.containsKey(msg.topic())) {
            if (log.isDebugEnabled())
                log.debug("Message is ignored as it came for the closed topic: " + msg);

            return;
        }

        long endTime = msg.timeout() + System.currentTimeMillis();

        // Account for overflow.
        if (endTime < 0)
            endTime = Long.MAX_VALUE;

        boolean isNew = false;

        GridCommunicationMessageSet set;

        lock.readLock().lock();

        try {
            ConcurrentMap<UUID, GridCommunicationMessageSet> map = msgSetMap.get(msg.topic());

            if (map == null) {
                ConcurrentMap<UUID, GridCommunicationMessageSet> old = msgSetMap.putIfAbsent(msg.topic(),
                    map = new ConcurrentHashMap<UUID, GridCommunicationMessageSet>(16, 0.75f, 2));

                if (old != null)
                    map = old;
            }

            set = map.get(nodeId);

            if (set == null) {
                GridCommunicationMessageSet old = map.putIfAbsent(nodeId,
                    set = new GridCommunicationMessageSet(plc, msg.topic(), nodeId, endTime));

                if (old != null)
                    set = old;
                else
                    isNew = true;
            }

            set.add(msg);
        }
        finally {
            lock.readLock().unlock();
        }

        final GridCommunicationMessageSet msgSet = set;

        if (isNew && endTime != Long.MAX_VALUE)
            ctx.timeout().addTimeoutObject(msgSet);

        final Set<GridFilteredMessageListener> lsnrs = lsnrMap.get(msg.topic());

        if (!F.isEmpty(lsnrs)) {
            try {
                getPool(plc).execute(new GridWorker(ctx.config().getGridName(), "comm-mgr-ordered-worker", log) {
                    @Override protected void body() {
                        unwindMessageSet(msgSet, lsnrs);
                    }
                });
            }
            catch (GridExecutionRejectedException e) {
                U.error(log, "Failed to process ordered message due to execution rejection. Increase the upper bound " +
                    "on system executor service provided by 'GridConfiguration.getSystemExecutorService()'). Will " +
                    "attempt to process message in the listener thread instead.", e);

                unwindMessageSet(msgSet, lsnrs);
            }
            catch (GridException e) {
                U.error(log, "Failed to process ordered message due to system error.", e);
            }
        }
        else {
            // Note that we simply keep messages if listener is not
            // registered yet, until one will be registered.
            if (log.isDebugEnabled())
                log.debug("Received message for unknown listener " +
                    "(messages will be kept until a listener is registered): " + msg);
        }
    }

    /**
     * @param msgSet Message set to unwind.
     * @param lsnrs Listeners to notify.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "deprecation"})
    private void unwindMessageSet(GridCommunicationMessageSet msgSet, Iterable<GridFilteredMessageListener> lsnrs) {
        // Loop until message set is empty or
        // another thread owns the reservation.
        while (true) {
            if (msgSet.reserve()) {
                try {
                    Collection<GridIoMessage> orderedMsgs = msgSet.unwind();

                    if (!orderedMsgs.isEmpty()) {
                        for (GridIoMessage msg : orderedMsgs) {
                            try {
                                Object obj = unmarshal(msg);

                                for (GridMessageListener lsnr : lsnrs)
                                    lsnr.onMessage(msgSet.nodeId(), obj);
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to deserialize ordered communication message:" + msg, e);
                            }
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("No messages were unwound: " + msgSet);
                }
                finally {
                    msgSet.release();
                }

                // Check outside of reservation block.
                if (!msgSet.changed()) {
                    if (log.isDebugEnabled())
                        log.debug("Message set has not been changed: " + msgSet);

                    break;
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Another thread owns reservation: " + msgSet);

                return;
            }
        }
    }

    /**
     * Unmarshal given message with appropriate class loader.
     *
     * @param msg communication message
     * @return Unmarshalled message.
     * @throws GridException If deserialization failed.
     */
    private Object unmarshal(GridIoMessage msg) throws GridException {
        return U.unmarshal(marsh, msg.message(), dfltClsLdr);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param topicOrd GridTopic enumeration ordinal.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param msgId Message ID.
     * @param timeout Timeout.
     * @throws GridException Thrown in case of any errors.
     */
    private void send(GridNode node, String topic, int topicOrd, Object msg, GridIoPolicy plc,
        long msgId, long timeout) throws GridException {
        assert node != null;
        assert topic != null;
        assert msg != null;
        assert plc != null;

        GridNode node0 = ctx.discovery().node(node.id());

        if (node0 == null)
            throw new GridException("Failed to send message to node (has node left grid?): " + node.id());

        GridByteArrayList serMsg = marshalSendingMessage(msg);

        try {
            getSpi().sendMessage(node, new GridIoMessage(locNodeId, node.id(), topic, topicOrd,
                serMsg, plc, msgId, timeout));
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to send message [node=" + node + ", topic=" + topic +
                ", msg=" + msg + ", policy=" + plc + ']', e);
        }
    }

    /**
     * @param nodeId Id of destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    public void send(UUID nodeId, String topic, Object msg, GridIoPolicy plc) throws GridException {
        GridNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new GridException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic, msg, plc);
    }

    /**
     * @param nodeId Id of destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void send(UUID nodeId, GridTopic topic, Object msg, GridIoPolicy plc) throws GridException {
        GridNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new GridException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic.name(), topic.ordinal(), msg, plc, -1, 0);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    public void send(GridNode node, String topic, Object msg, GridIoPolicy plc) throws GridException {
        send(node, topic, -1, msg, plc, -1, 0);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void send(GridNode node, GridTopic topic, Object msg, GridIoPolicy plc) throws GridException {
        send(node, topic.name(), topic.ordinal(), msg, plc, -1, 0);
    }

    /**
     * @param topic Message topic.
     * @param nodeId Node ID.
     * @return Next ordered message ID.
     */
    public long nextMessageId(String topic, UUID nodeId) {
        ConcurrentMap<UUID, AtomicLong> map = msgIdMap.get(topic);

        if (map == null) {
            ConcurrentMap<UUID, AtomicLong> lastMap = msgIdMap.putIfAbsent(topic,
                map = new ConcurrentHashMap<UUID, AtomicLong>(1, 1.0f, 1));

            if (lastMap != null)
                map = lastMap;
        }

        AtomicLong msgId = map.get(nodeId);

        if (msgId == null) {
            AtomicLong lastMsgId = map.putIfAbsent(nodeId, msgId = new AtomicLong(0));

            if (lastMsgId != null)
                msgId = lastMsgId;
        }

        long id = msgId.incrementAndGet();

        if (log.isDebugEnabled())
            log.debug("Got next message ID [topic=" + topic + ", nodeId=" + nodeId + ", id=" + id + ']');

        return id;
    }

    /**
     * @param topic Message topic.
     */
    public void removeMessageId(String topic) {
        if (log.isDebugEnabled())
            log.debug("Remove message ID for topic: " + topic);

        msgIdMap.remove(topic);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msgId Ordered message ID.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @throws GridException Thrown in case of any errors.
     */
    public void sendOrderedMessage(GridNode node, String topic, long msgId, Object msg,
        GridIoPolicy plc, long timeout) throws GridException {
        send(node, topic, (byte)-1, msg, plc, msgId, timeout);
    }

    /**
     * @param nodes Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    public void send(Collection<? extends GridNode> nodes, String topic, Object msg,
        GridIoPolicy plc) throws GridException {
        send(nodes, topic, -1, msg, plc);
    }

    /**
     * @param nodes Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    public void send(Collection<? extends GridNode> nodes, GridTopic topic, Object msg,
        GridIoPolicy plc) throws GridException {
        send(nodes, topic.name(), topic.ordinal(), msg, plc);
    }

    /**
     * Sends a peer deployable user message.
     *
     * @param nodes Destination nodes.
     * @param msg Message to send.
     * @throws GridException Thrown in case of any errors.
     */
    public void sendUserMessage(Collection<? extends GridNode> nodes, Object msg) throws GridException {
        GridByteArrayList serSrc = U.marshal(ctx.config().getMarshaller(), msg);

        GridDeployment dep = ctx.deploy().deploy(msg.getClass(), U.detectClassLoader(msg.getClass()));

        if (dep == null)
            throw new GridDeploymentException("Failed to deploy user message: " + msg);

        Serializable serMsg = new GridIoUserMessage(
            serSrc,
            msg.getClass().getName(),
            dep.classLoaderId(),
            dep.deployMode(),
            dep.sequenceNumber(),
            dep.userVersion(),
            dep.participants());

        send(nodes, TOPIC_COMM_USER, serMsg, PUBLIC_POOL);
    }

    /**
     *
     * @param nodes Collection of nodes to listen from.
     * @param ps Message predicate.
     */
    @SuppressWarnings("deprecation")
    public <T> void listenAsync(Collection<GridRichNode> nodes, @Nullable final GridPredicate2<UUID, T>[] ps) {
        if (!F.isEmpty(ps)) {
            final UUID[] ids = F.nodeIds(nodes).toArray(new UUID[nodes.size()]);

            // Optimize search over IDs.
            Arrays.sort(ids);

            assert ps != null;

            // Set local context.
            for (GridPredicate2<UUID, T> p : ps)
                if (p instanceof GridListenActor)
                    ((GridListenActor)p).setContext(ctx.grid(), nodes);

            addMessageListener(TOPIC_COMM_USER, new GridMessageListener() {
                @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "unchecked", "deprecated"})
                @Override public void onMessage(UUID nodeId, Object msg) {
                    if (Arrays.binarySearch(ids, nodeId) >= 0) {
                        if (!(msg instanceof GridIoUserMessage)) {
                            U.error(log, "Received unknown message (potentially fatal problem): " + msg);

                            return;
                        }

                        GridIoUserMessage req = (GridIoUserMessage)msg;

                        GridNode node = ctx.discovery().node(nodeId);

                        if (node == null) {
                            U.warn(log, "Failed to resolve sender node that does not exist: " + nodeId);

                            return;
                        }

                        Object srcMsg = null;

                        try {
                            GridDeployment dep = ctx.deploy().getGlobalDeployment(
                                req.getDeploymentMode(),
                                req.getSourceClassName(),
                                req.getSourceClassName(),
                                req.getSequenceNumber(),
                                req.getUserVersion(),
                                nodeId,
                                req.getClassLoaderId(),
                                req.getLoaderParticipants(),
                                null);

                            if (dep == null)
                                throw new GridDeploymentException("Failed to obtain deployment for user message " +
                                    "(is peer class loading turned on?): " + req);

                            srcMsg = U.unmarshal(ctx.config().getMarshaller(), req.getSource(), dep.classLoader());

                            // Resource injection.
                            ctx.resource().inject(dep, dep.deployedClass(req.getSourceClassName()), srcMsg);
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to send user message [node=" + nodeId + ", message=" + msg + ']', e);
                        }

                        if (srcMsg != null) {
                            for (GridPredicate2<UUID, T> p : ps) {
                                synchronized (p) {
                                    if (!p.apply(nodeId, (T)srcMsg)) {
                                        removeMessageListener(TOPIC_COMM_USER, this);

                                        // Short-circuit.
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }, F.<Object>alwaysTrue());
        }
    }

    /**
     * @param nodes Destination nodes.
     * @param topic Topic to send the message to.
     * @param topicOrd Topic ordinal value.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws GridException Thrown in case of any errors.
     */
    private void send(Collection<? extends GridNode> nodes, String topic, int topicOrd, Object msg,
        GridIoPolicy plc) throws GridException {
        assert nodes != null;
        assert topic != null;
        assert msg != null;
        assert plc != null;

        try {
            // Small optimization, as communication SPIs may have lighter implementation for sending
            // messages to one node vs. many.
            if (nodes.size() == 1)
                send(F.first(nodes), topic, topicOrd, msg, plc, -1, 0);
            else if (nodes.size() > 1) {
                GridByteArrayList serMsg = marshalSendingMessage(msg);

                List<UUID> destIds = new ArrayList<UUID>(nodes.size());

                for (GridNode node : nodes)
                    destIds.add(node.id());

                getSpi().sendMessage(nodes, new GridIoMessage(locNodeId, destIds, topic, topicOrd, serMsg, plc));
            }
            else
                U.warn(log, "Failed to send message to empty nodes collection [topic=" + topic + ", msg=" +
                    msg + ", policy=" + plc + ']', "Failed to send message to empty nodes collection.");
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to send message [nodes=" + nodes + ", topic=" + topic +
                ", msg=" + msg + ", policy=" + plc + ']', e);
        }
    }

    /**
     * Marshal message.
     *
     * @param msg Message to send.
     * @return Return message serialized in bytes.
     * @throws GridException In case of error.
     */
    private GridByteArrayList marshalSendingMessage(Object msg) throws GridException {
        GridByteArrayList serMsg;

        GridTuple2<Object, GridByteArrayList> cacheEntry = cacheMsg.get();

        // Check cached message in ThreadLocal for optimization.
        if (cacheEntry != null && cacheEntry.get1() == msg)
            serMsg = cacheEntry.get2();
        else {
            serMsg = U.marshal(marsh, msg);

            cacheMsg.set(F.t(msg, serMsg));
        }

        return serMsg;
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     * @param p Predicates to be applied.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "deprecation"})
    public void addMessageListener(GridTopic topic, GridMessageListener lsnr, GridPredicate<Object>... p) {
        addMessageListener(topic.name(), lsnr, p);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     * @param p Predicates to be applied.
     */
    @SuppressWarnings({"deprecation", "TooBroadScope"})
    public void addMessageListener(String topic, GridMessageListener lsnr, GridPredicate<Object>... p) {
        assert lsnr != null;
        assert topic != null;

        Collection<GridCommunicationMessageSet> msgSets = null;

        GridFilteredMessageListener filteredLsnr = new GridFilteredMessageListener(lsnr, p);

        GridConcurrentHashSet<GridFilteredMessageListener> lsnrs;

        lock.readLock().lock();

        try {
            lsnrs = lsnrMap.get(topic);

            if (lsnrs == null) {
                GridConcurrentHashSet<GridFilteredMessageListener> temp =
                    new GridConcurrentHashSet<GridFilteredMessageListener>(1);

                lsnrs = lsnrMap.putIfAbsent(topic, temp);

                if (lsnrs == null)
                    lsnrs = temp;
            }

            lsnrs.add(filteredLsnr);

            Map<UUID, GridCommunicationMessageSet> map = msgSetMap.get(topic);

            msgSets = map != null ? map.values() : null;

            // Make sure that new topic is not in the list of closed topics.
            closedTopics.remove(topic);
        }
        finally {
            lock.readLock().unlock();
        }

        assert lsnrs != null;

        if (msgSets != null) {
            final Collection<GridFilteredMessageListener> lsnrs0 = lsnrs;

            try {
                for (final GridCommunicationMessageSet msgSet : msgSets) {
                    getPool(msgSet.policy()).execute(
                        new GridWorker(ctx.config().getGridName(), "comm-mgr-ordered-worker", log) {
                            @Override protected void body() {
                                unwindMessageSet(msgSet, lsnrs0);
                            }
                        });
                }
            }
            catch (GridExecutionRejectedException e) {
                U.error(log, "Failed to process delayed message due to execution rejection. Increase the upper bound " +
                    "on executor service provided in 'GridConfiguration.getExecutorService()'). Will attempt to " +
                    "process message in the listener thread instead instead.", e);

                for (GridCommunicationMessageSet msgSet : msgSets)
                    unwindMessageSet(msgSet, Collections.singletonList(filteredLsnr));
            }
            catch (GridException e) {
                U.error(log, "Failed to process delayed message due to system error.", e);
            }
        }
    }

    /**
     * @param topic Message topic.
     * @return Whether or not listener was indeed removed.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    public boolean removeMessageListener(GridTopic topic) {
        return removeMessageListener(topic.name());
    }

    /**
     * @param topic Message topic.
     * @return Whether or not listener was indeed removed.
     */
    public boolean removeMessageListener(String topic) {
        return removeMessageListener(topic, null);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "deprecation"})
    public boolean removeMessageListener(GridTopic topic, @Nullable GridMessageListener lsnr) {
        return removeMessageListener(topic.name(), lsnr);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    @SuppressWarnings({"deprecation", "TooBroadScope"})
    public boolean removeMessageListener(String topic, @Nullable GridMessageListener lsnr) {
        assert topic != null;

        boolean rmv = true;

        Collection<GridCommunicationMessageSet> msgSets = null;

        lock.writeLock().lock();

        try {
            // If listener is null, then remove all listeners.
            if (lsnr == null) {
                rmv = lsnrMap.remove(topic) != null;

                Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                if (map != null)
                    msgSets = map.values();

                closedTopics.put(topic, 0);
            }
            else {
                GridConcurrentHashSet<GridFilteredMessageListener> lsnrs = lsnrMap.get(topic);

                // If removing listener before subscription happened.
                if (lsnrs == null) {
                    Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                    if (map != null)
                        msgSets = map.values();

                    closedTopics.put(topic, 0);

                    rmv = false;
                }
                else {
                    GridFilteredMessageListener filteredLsnr = new GridFilteredMessageListener(lsnr);

                    if (lsnrs.remove(filteredLsnr)) {
                        // If removing last subscribed listener.
                        if (lsnrs.isEmpty()) {
                            Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                            if (map != null)
                                msgSets = map.values();

                            lsnrMap.remove(topic);

                            closedTopics.put(topic, 0);
                        }
                    }
                    else
                        // Listener was not found.
                        rmv = false;
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        if (msgSets != null) {
            for (GridCommunicationMessageSet msgSet : msgSets)
                ctx.timeout().removeTimeoutObject(msgSet);
        }

        if (rmv && log.isDebugEnabled())
            log.debug("Removed message listener [topic=" + topic + ", lsnr=" + lsnr + ']');

        return rmv;
    }

    /**
     * Adds a user message listener to support the deprecated method.
     *
     * This is a workaround for implementation of the deprecated
     * {@link Grid#addMessageListener(GridMessageListener, GridPredicate[])} method.
     * In general you should not use it.
     *
     * @param lsnr Listener to add.
     * @param p Predicates to be applied.
     */
    @SuppressWarnings("deprecation")
    public void addUserMessageListener(GridMessageListener lsnr, GridPredicate<Object>... p) {
        // We transform the messages before we send them. Therefore we have to wrap
        // the predicates in the new listener object as well. Then we can apply
        // these predicates to the source message after opposite transformation.
        addMessageListener(TOPIC_COMM_USER, new GridUserMessageListener(lsnr, p));
    }

    /**
     * Removes a user message listener to support the deprecated method.
     *
     * This is a workaround for implementation of the deprecated
     * {@link Grid#removeMessageListener(GridMessageListener)} method.
     * In general you should not use it.
     *
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    @SuppressWarnings("deprecation")
    public boolean removeUserMessageListener(GridMessageListener lsnr) {
        return removeMessageListener(TOPIC_COMM_USER, new GridUserMessageListener(lsnr));
    }

    /**
     * @param topic Communication topic.
     */
    public void removeSyncMessageHandler(GridTopic topic) {
        removeMessageListener(topic.name());
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {

        X.println(">>>");
        X.println(">>> IO manager memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>  lsnrMapSize: " + lsnrMap.size());
        X.println(">>>  msgSetMapSize: " + msgSetMap.size());
        X.println(">>>  msgIdMapSize: " + msgIdMap.size());
        X.println(">>>  closedTopicsSize: " + closedTopics.sizex());
        X.println(">>>  discoWaitMapSize: " + discoWaitMap.size());
    }

    /**
     * This class represents a pair of listener and its corresponding message p.
     */
    @SuppressWarnings("deprecation")
    private class GridFilteredMessageListener implements GridMessageListener {
        /** MessageFilter. */
        private final GridPredicate<Object>[] p;

        /** MessageListener. */
        private final GridMessageListener lsnr;

        /**
         * Constructs an object with the specified listener and message predicates.
         *
         * @param lsnr Listener to bind.
         * @param p Filter to apply.
         */
        GridFilteredMessageListener(GridMessageListener lsnr, GridPredicate<Object>... p) {
            assert lsnr != null;
            assert p != null;

            this.lsnr = lsnr;
            this.p = p;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (F.isAll(msg, p))
                lsnr.onMessage(nodeId, msg);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o ||
                (o instanceof GridFilteredMessageListener && lsnr.equals(((GridFilteredMessageListener)o).lsnr));
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return lsnr.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridFilteredMessageListener.class, this);
        }
    }

    /**
     * This class represents a message listener wrapper that knows about peer deployment.
     */
    @SuppressWarnings("deprecation")
    private class GridUserMessageListener implements GridMessageListener {
        /** MessageFilter. */
        private final GridPredicate<Object>[] p;

        /** MessageListener. */
        private final GridMessageListener lsnr;

        /**
         * Constructs an object with the specified listener.
         *
         * @param lsnr Listener to bind.
         * @param p Filters to apply.
         */
        GridUserMessageListener(GridMessageListener lsnr, GridPredicate<Object>... p) {
            assert lsnr != null;
            assert p != null;

            this.lsnr = lsnr;
            this.p = p;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (!(msg instanceof GridIoUserMessage)) {
                U.error(log, "Received unknown message (potentially fatal error): " + msg);

                return;
            }

            GridIoUserMessage req = (GridIoUserMessage)msg;

            GridNode node = ctx.discovery().node(nodeId);

            if (node == null) {
                U.warn(log, "Failed to resolve sender node (did node leave the grid?): " + nodeId);

                return;
            }

            Object srcMsg = null;

            try {
                GridDeployment dep = ctx.deploy().getGlobalDeployment(
                    req.getDeploymentMode(),
                    req.getSourceClassName(),
                    req.getSourceClassName(),
                    req.getSequenceNumber(),
                    req.getUserVersion(),
                    nodeId,
                    req.getClassLoaderId(),
                    req.getLoaderParticipants(),
                    null);

                if (dep == null)
                    throw new GridDeploymentException("Failed to obtain deployment for user message " +
                        "(is peer class loading turned on?): " + req);

                srcMsg = U.unmarshal(ctx.config().getMarshaller(), req.getSource(), dep.classLoader());

                // Resource injection.
                ctx.resource().inject(dep, dep.deployedClass(req.getSourceClassName()), srcMsg);
            }
            catch (GridException e) {
                U.error(log, "Failed to unmarshal user message [node=" + nodeId + ", message=" + msg + ']', e);
            }

            if (srcMsg != null && F.isAll(srcMsg, p))
                lsnr.onMessage(nodeId, srcMsg);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o ||
                (o instanceof GridUserMessageListener && lsnr.equals(((GridUserMessageListener)o).lsnr));
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return lsnr.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridUserMessageListener.class, this);
        }
    }

    /**
     * Ordered communication message set.
     */
    private class GridCommunicationMessageSet implements GridTimeoutObject {
        /** */
        private final UUID nodeId;

        /** */
        private final long endTime;

        /** */
        private final GridUuid timeoutId;

        /** */
        private final String topic;

        /** */
        private final GridIoPolicy plc;

        /** */
        @GridToStringInclude
        private final List<GridIoMessage> msgs = new ArrayList<GridIoMessage>();

        /** */
        private long nextMsgId = 1;

        /** */
        private final AtomicBoolean reserved = new AtomicBoolean();

        /** */
        private final Lock lock0 = new ReentrantLock();

        /** */
        private volatile boolean changed;

        /**
         * @param plc Communication policy.
         * @param topic Communication topic.
         * @param nodeId Node ID.
         * @param endTime endTime.
         */
        GridCommunicationMessageSet(GridIoPolicy plc, String topic, UUID nodeId, long endTime) {
            assert nodeId != null;
            assert topic != null;
            assert plc != null;

            this.plc = plc;
            this.nodeId = nodeId;
            this.topic = topic;
            this.endTime = endTime;

            timeoutId = GridUuid.randomUuid();
        }

        /** {@inheritDoc} */
        @Override public GridUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Removing message set due to timeout: " + this);

            lock.writeLock().lock();

            try {
                ConcurrentMap<UUID, GridCommunicationMessageSet> map = msgSetMap.get(topic);

                if (map != null && map.remove(nodeId, this) && map.isEmpty())
                    msgSetMap.remove(topic);
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * @return ID of node that sent the messages in the set.
         */
        UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Communication policy.
         */
        GridIoPolicy policy() {
            return plc;
        }

        /**
         * @return Message topic.
         */
        String topic() {
            return topic;
        }

        /**
         * @return {@code True} if successful.
         */
        boolean reserve() {
            return reserved.compareAndSet(false, true);
        }

        /**
         * Releases reservation.
         */
        void release() {
            boolean b = reserved.compareAndSet(true, false);

            assert b : "Message set was not reserved: " + this;
        }

        /**
         * @return Session request.
         */
        Collection<GridIoMessage> unwind() {
            assert reserved.get();

            lock0.lock();

            try {
                changed = false;

                if (msgs.isEmpty())
                    return Collections.emptyList();

                // Sort before unwinding.
                Collections.sort(msgs, new Comparator<GridIoMessage>() {
                    @Override public int compare(GridIoMessage o1, GridIoMessage o2) {
                        return o1.messageId() < o2.messageId() ? -1 : o1.messageId() == o2.messageId() ? 0 : 1;
                    }
                });

                Collection<GridIoMessage> orderedMsgs = new LinkedList<GridIoMessage>();

                for (Iterator<GridIoMessage> iter = msgs.iterator(); iter.hasNext();) {
                    GridIoMessage msg = iter.next();

                    if (msg.messageId() == nextMsgId) {
                        orderedMsgs.add(msg);

                        nextMsgId++;

                        iter.remove();
                    }
                    else
                        break;
                }

                return orderedMsgs;
            }
            finally {
                lock0.unlock();
            }
        }

        /**
         * @param msg Message to add.
         */
        void add(GridIoMessage msg) {
            lock0.lock();

            try {
                msgs.add(msg);

                changed = true;
            }
            finally {
                lock0.unlock();
            }
        }

        /**
         * @return {@code True} if set has messages to unwind.
         */
        boolean changed() {
            return changed;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            lock0.lock();

            try {
                return S.toString(GridCommunicationMessageSet.class, this);
            }
            finally {
                lock0.unlock();
            }
        }
    }
}
