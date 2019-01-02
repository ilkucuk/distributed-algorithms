package com.kucuk.distributed.leaderelection;

import lombok.Getter;

public class CommunicationException extends Exception {
    @Getter
    private long targetPriorityId;
    public CommunicationException(long targetPriorityId) {
        this.targetPriorityId = targetPriorityId;
    }

    public CommunicationException(long targetPriorityId, Exception inner) {
        super(inner);
        this.targetPriorityId = targetPriorityId;
    }
}
