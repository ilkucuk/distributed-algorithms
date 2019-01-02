package com.kucuk.distributed.leaderelection;

import lombok.AllArgsConstructor;

import java.util.Random;

@AllArgsConstructor
public class NodeConnection implements Node {

    private final Node node;
    private final int delayMilliseconds;
    private final int dropPercentage;

    private final Random random = new Random();

    @Override
    public long getPriorityId() {
        return node.getPriorityId();
    }

    private void delay() {
        if (delayMilliseconds > 0) {
            try {
                Thread.sleep(delayMilliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void drop() throws CommunicationException {
        if (dropPercentage > 0){
            int chance = random.nextInt(100);
            if (chance <= dropPercentage) {
                throw new CommunicationException(getPriorityId());
            }
        }
    }

    @Override
    public LivelinessState getLivelinessState() throws CommunicationException {
        drop();
        delay();
        return node.getLivelinessState();
    }

    @Override
    public ElectionResult electionMessage() throws CommunicationException {
        drop();
        delay();
        return node.electionMessage();
    }

    @Override
    public void coordinateMessage(long leaderPriorityId) throws CommunicationException {
        drop();
        delay();
        node.coordinateMessage(leaderPriorityId);
    }

    @Override
    public boolean isLeader() {
        return node.isLeader();
    }
}
