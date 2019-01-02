package com.kucuk.distributed.leaderelection;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Election Coordinator an asynchronous implementation of Bully leader election algorithm
 * coordinator thread runs on its own scheduled threat pool and checks the health of current leader periodically
 * if current leader misses heartbeat 'maxSkipHeartbeatCount' times (default:3) starts a new leader election
 * each election coordinator must have a unique priorityId
 */
@Slf4j
public class ElectionCoordinator implements Node, Runnable {

    private final Long priorityId;

    private final Map<Long, Node> nodes;
    private final Map<Long, Node> higherPriorityNodes;
    private Node leader;

    private volatile LivelinessState livelinessState;
    private volatile ElectionState electionState;
    private final Lock stateChangeLock = new ReentrantLock();

    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Setter
    private int maxTimeoutForElectionMessagesInMilliseconds = 3000;
    @Setter
    private int maxSkipHeartbeatCount = 3;

    /**
     * Initializes new election coordinator
     * @param priorityId priority id of election coordinator, must be unique
     * @param threadPoolSize 0 means cachedThreadPool, Otherwise fixedThreadPool size
     */
    public ElectionCoordinator(Long priorityId, int threadPoolSize) {
        this.priorityId = priorityId;
        nodes = new HashMap<>();
        higherPriorityNodes = new HashMap<>();
        livelinessState = LivelinessState.Healthy;
        electionState = ElectionState.Init;

        if (threadPoolSize == 0) {
            executorService = Executors.newCachedThreadPool();
        } else {
            executorService = Executors.newFixedThreadPool(threadPoolSize);
        }
    }

    @Override
    public long getPriorityId() {
        return priorityId;
    }

    public void setFaultyState() {
        stateChangeLock.lock();
        this.livelinessState = LivelinessState.Faulty;
        this.electionState = ElectionState.Init;
        this.leader = null;
        stateChangeLock.unlock();
    }

    public void setHealthyState() {
        stateChangeLock.lock();
        this.livelinessState = LivelinessState.Healthy;
        this.electionState = ElectionState.Init;
        this.leader = null;
        stateChangeLock.unlock();
    }

    public void addAll(List<Node> nodes) {
        nodes.forEach(this::add);
    }

    public void add(final Node node) {
        if (node.getPriorityId() != this.priorityId) {
            nodes.put(node.getPriorityId(), node);
            if (node.getPriorityId() > getPriorityId()) {
                higherPriorityNodes.put(node.getPriorityId(), node);
            }
        }
    }

    @Override
    public ElectionResult electionMessage() {
        log.debug("{} received election message", getPriorityId());
        if (livelinessState.equals(LivelinessState.Faulty)) {
            return ElectionResult.FAIL;
        }
        if (!electionState.equals(ElectionState.ElectionInProgress)) {
            executorService.submit(this::startLeaderElection);
        }
        return ElectionResult.OK;
    }

    @Override
    public void coordinateMessage(long leaderPriorityId) {
        log.debug("{} received coordinate message from {}", getPriorityId(), leaderPriorityId);
        stateChangeLock.lock();
        this.electionState = ElectionState.Elected;
        if (nodes.containsKey(leaderPriorityId)) {
            leader = nodes.get(leaderPriorityId);
        }
        stateChangeLock.unlock();
    }

    @Override
    public boolean isLeader() {
        return leader != null && leader.getPriorityId() == this.getPriorityId();
    }

    @Override
    public LivelinessState getLivelinessState() {
        //log.debug("{} being asked for liveliness state", getPriorityId());
        return livelinessState;
    }


    private void promoteSelfAsLeader() {
        if (livelinessState.equals(LivelinessState.Healthy)) {
            if (!electionState.equals(ElectionState.ElectionInProgress)) {
                log.error("PromoteSelfAsLeader is called while Election is not in progress");
            } else {
                leader = this;
                nodes.values().parallelStream().forEach(node ->
                {
                    try {
                        //implement retry in the node object
                        node.coordinateMessage(getPriorityId());
                    } catch (CommunicationException e) {
                        log.warn("Communication Exception with Node {}", e.getTargetPriorityId(), e);
                    }
                });
            }
        }
    }

    private boolean isLeaderHealthy() {
        if (leader == null) {
            return false;
        }
        LivelinessState state = LivelinessState.Faulty;
        try {
            state = leader.getLivelinessState();
        } catch (CommunicationException e) {
            log.warn("Communication Exception with Leader Node {}", e.getTargetPriorityId(), e);
        }
        return state == LivelinessState.Healthy;
    }

    private void startLeaderElection() {
        if (electionState.equals(ElectionState.ElectionInProgress)) {
            log.error("Invalid state, start leader election is called while one is already inProgress");
            return;
        }

        stateChangeLock.lock();
        this.electionState = ElectionState.ElectionInProgress;
        stateChangeLock.unlock();

        if (!livelinessState.equals(LivelinessState.Healthy)) {
            log.info("Quiting due to unhealthy state");
        } else if (higherPriorityNodes.size() == 0) {
            promoteSelfAsLeader();
        } else {

            boolean quit = false;

            //send electionMessage messages
            List<Future<ElectionResult>> results = new ArrayList<>();
            for (Node node : higherPriorityNodes.values()) {
                if (!livelinessState.equals(LivelinessState.Healthy) || !electionState.equals(ElectionState.ElectionInProgress)) {
                    log.info("Quiting leader election due to invalid state while sending election messages, electionState:{}, livelinessState",
                            electionState, livelinessState);
                    quit = true;
                    break;
                }
                results.add(executorService.submit(node::electionMessage));
            }

            if (!quit) {
                boolean highPriorityResponseReceived = false;
                for (Future<ElectionResult> result : results) {
                    try {
                        if (!livelinessState.equals(LivelinessState.Healthy) || !electionState.equals(ElectionState.ElectionInProgress)) {
                            log.info("Quiting leader election due to invalid state while receiving election messages, electionState:{}, livelinessState",
                                    electionState, livelinessState);
                            quit = true;
                            break;
                        }
                        ElectionResult electionResult = result.get(maxTimeoutForElectionMessagesInMilliseconds, TimeUnit.MILLISECONDS);
                        log.debug("{} Received election result {}", getPriorityId(), electionResult);
                        if (electionResult.equals(ElectionResult.OK)) {
                            highPriorityResponseReceived = true;
                            break;
                        }
                    } catch (InterruptedException e) {
                        log.error("Interrupted...", e);
                        return;
                    } catch (ExecutionException e) {
                        log.warn("Execution exception while waiting for election message response", e);
                    } catch (TimeoutException e) {
                        log.warn("Timeout while waiting for an election message response", e);
                    }
                }

                if (!quit && !highPriorityResponseReceived) {
                    promoteSelfAsLeader();
                }
            }
        }
        stateChangeLock.lock();
        this.electionState = ElectionState.Elected;
        stateChangeLock.unlock();
    }

    public void start(long initialDelay, long period, TimeUnit unit) {
        log.info("Starting Leader Election Module for Node {}", getPriorityId());
        scheduler.scheduleAtFixedRate(this, initialDelay, period, unit);
    }

    public void shutdown() {
        log.info("Shutting down {}", getPriorityId());
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(3,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
           log.error("Interrupted {}", getPriorityId());
           scheduler.shutdownNow();
        }
        leader = null;
        log.info("Shutting down completed {}", getPriorityId());
    }

    @Override
    public void run() {

        if (!livelinessState.equals(LivelinessState.Faulty)) {

            if (!electionState.equals(ElectionState.ElectionInProgress)) {

                boolean startElection = true;
                if (leader != null) {
                    int attemptCount = 0;
                    while (startElection && attemptCount++ < maxSkipHeartbeatCount) {
                        startElection = !isLeaderHealthy();
                        log.debug("Heartbeat, leader healthy {}", !startElection);
                    }
                }

                if (startElection && !electionState.equals(ElectionState.ElectionInProgress)) {
                    startLeaderElection();
                }
            }
        }
    }

    public enum ElectionState {
        Init, ElectionInProgress, Elected
    }
}
