package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/5/16.
 */
public class WebsocketStartConsumeMessageResponse {
    @JsonProperty("Action") private String Action;
    @JsonProperty("RetCode") private int RetCode;
    @JsonProperty("Request") private String Request;
    @JsonProperty("Message") private String Message;
    @JsonProperty("Data") private String Data;

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

    public String getData() {
        return Data;
    }

    public void setData(String data) {
        Data = data;
    }

    @Override
    public String toString() {
        return "WebsocketStartConsumeMessageResponse{" +
                "Action='" + Action + '\'' +
                ", RetCode=" + RetCode +
                ", Request='" + Request + '\'' +
                ", Message='" + Message + '\'' +
                ", Data='" + Data + '\'' +
                '}';
    }
}
