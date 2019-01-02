package com.kucuk.distributed.leaderelection;

public interface Node {

    long getPriorityId();
    LivelinessState getLivelinessState() throws CommunicationException;

    ElectionResult electionMessage() throws CommunicationException;
    void coordinateMessage(long leaderPriorityId) throws CommunicationException;
    boolean isLeader();

}
