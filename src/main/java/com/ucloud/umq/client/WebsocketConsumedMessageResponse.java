package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/5/16.
 */
public class WebsocketConsumedMessageResponse {
    @JsonProperty("Action") private String Action;
    @JsonProperty("RetCode") private int RetCode;
    @JsonProperty("Request") private String Request;
    @JsonProperty("Message") private String Message;
    @JsonProperty("Data") private MessageData Data;

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

    public String getAction() {
        return Action;
    }

    public void setAction(String action) {
        Action = action;
    }

    public int getRetCode() {
        return RetCode;
    }

    public void setRetCode(int retCode) {
        RetCode = retCode;
    }

    public String getRequest() {
        return Request;
    }

    public void setRequest(String request) {
        Request = request;
    }

    public String getMessage() {
        return Message;
    }

    public void setMessage(String message) {
        Message = message;
    }

    public MessageData getData() {
        return Data;
    }

    public void setData(MessageData data) {
        Data = data;
    }

    @Override
    public String toString() {
        return "WebsocketConsumedMessageResponse{" +
                "Action='" + Action + '\'' +
                ", RetCode=" + RetCode +
                ", Request='" + Request + '\'' +
                ", Message='" + Message + '\'' +
                ", Data=" + Data +
                '}';
    }
}
