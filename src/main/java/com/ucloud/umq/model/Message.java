package com.ucloud.umq.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/9/16.
 */
public class Message {
    @JsonProperty("MsgId") private String MsgId;
    @JsonProperty("MsgBody") private String MsgBody;
    @JsonProperty("MsgTime") private int MsgTime;

    public String getMsgId() {
        return MsgId;
    }

    public void setMsgId(String msgId) {
        MsgId = msgId;
    }

    public String getMsgBody() {
        return MsgBody;
    }

    public void setMsgBody(String msgBody) {
        MsgBody = msgBody;
    }

    public int getMsgTime() {
        return MsgTime;
    }

    public void setMsgTime(int msgTime) {
        MsgTime = msgTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "MsgId='" + MsgId + '\'' +
                ", MsgBody='" + MsgBody + '\'' +
                ", MsgTime=" + MsgTime +
                '}';
    }
}
