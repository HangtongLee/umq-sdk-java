package com.ucloud.umq.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/8/16.
 */
public class QueueEntity {
    @JsonProperty("QueueId")
    private String QueueId;

    @JsonProperty("EntityId")
    private String EntityId;

    @JsonProperty("EntityToken")
    private String EntityToken;

    public String getQueueId() {
        return QueueId;
    }

    public void setQueueId(String queueId) {
        QueueId = queueId;
    }

    public String getEntityId() {
        return EntityId;
    }

    public void setEntityId(String entityId) {
        EntityId = entityId;
    }

    public String getEntityToken() {
        return EntityToken;
    }

    public void setEntityToken(String entityToken) {
        EntityToken = entityToken;
    }
}
