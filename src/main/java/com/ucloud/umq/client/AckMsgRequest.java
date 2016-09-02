package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/11/16.
 */
public class AckMsgRequest extends ApiRequest {
    @JsonProperty("QueueId")
    private String queueId;
    @JsonProperty("ConsumerId")
    private String consumerId;
    @JsonProperty("MsgId")
    private String msgId;

    public AckMsgRequest(String queueId, String consumerId, String msgId) {
        this.setAction("AckMsg");
        this.queueId = queueId;
        this.consumerId = consumerId;
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "AckMsgRequest{" +
                "queueId='" + queueId + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", msgId='" + msgId + '\'' +
                '}';
    }

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
}

