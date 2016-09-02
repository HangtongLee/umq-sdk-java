package com.ucloud.umq.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by alpha on 8/8/16.
 */
public class Queue {
    @JsonProperty("QueueId")
    private String QueueId;
    @JsonProperty("QueueName")
    private String QueueName;
    @JsonProperty("PushType")
    private String PushType;
    @JsonProperty("MsgTtl")
    private int MsgTTL;
    @JsonProperty("CreateTime")
    private int CreateTime;
    @JsonProperty("HttpAddr")
    private String HttpAddr;
    @JsonProperty("QoS")
    private String QoS;
    @JsonProperty("Publishers")
    private List<Role> publishers;
    @JsonProperty("Consumers")
    private List<Role> consumers;

    public String getQueueId() {
        return QueueId;
    }

    public void setQueueId(String queueId) {
        QueueId = queueId;
    }

    public String getQueueName() {
        return QueueName;
    }

    public void setQueueName(String queueName) {
        QueueName = queueName;
    }

    public String getPushType() {
        return PushType;
    }

    public void setPushType(String pushType) {
        PushType = pushType;
    }

    public int getMsgTTL() {
        return MsgTTL;
    }

    public void setMsgTTL(int msgTTL) {
        MsgTTL = msgTTL;
    }

    public int getCreateTime() {
        return CreateTime;
    }

    public void setCreateTime(int createTime) {
        CreateTime = createTime;
    }

    public String getHttpAddr() {
        return HttpAddr;
    }

    public void setHttpAddr(String httpAddr) {
        HttpAddr = httpAddr;
    }

    public String getQoS() {
        return QoS;
    }

    public void setQoS(String qoS) {
        QoS = qoS;
    }

    public List<Role> getPublishers() {
        return publishers;
    }

    public void setPublishers(List<Role> publishers) {
        this.publishers = publishers;
    }

    public List<Role> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<Role> consumers) {
        this.consumers = consumers;
    }
}
