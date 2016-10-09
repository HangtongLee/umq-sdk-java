package com.ucloud.umq.action;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 9/29/16.
 */

public class MessageData {
    @JsonProperty("MsgBody") private String MsgBody;
    @JsonProperty("MsgId") private String MsgId;

    public String getMsgBody() {
        return MsgBody;
    }

    public void setMsgBody(String msgBody) {
        MsgBody = msgBody;
    }

    public String getMsgId() {
        return MsgId;
    }

    public void setMsgId(String msgId) {
        MsgId = msgId;
    }

    @Override
    public String toString() {
        return "MessageData{" +
                "MsgBody='" + MsgBody + '\'' +
                ", MsgId='" + MsgId + '\'' +
                '}';
    }
}
