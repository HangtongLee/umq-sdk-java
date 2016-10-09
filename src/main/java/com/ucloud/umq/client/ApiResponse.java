package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/8/16.
 */
public class ApiResponse {
    @JsonProperty("RetCode")
    private int RetCode;

    @JsonProperty("Message")
    private String Message;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonProperty("Action")
    private String Action;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonProperty("DataSet")
    private String DataSet;

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonProperty("TotalCount")
    private int TotalCount;

    public String getRequest() {
        return Request;
    }

    public void setRequest(String request) {
        Request = request;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonProperty("Request")
    private String Request;

    public int getRetCode() {
        return RetCode;
    }

    public void setRetCode(int retCode) {
        RetCode = retCode;
    }

    public String getAction() {
        return Action;
    }

    public void setAction(String action) {
        Action = action;
    }

    public String getMessage() {
        return Message;
    }

    public void setMessage(String message) {
        Message = message;
    }

    public int getTotalCount() {
        return TotalCount;
    }

    public void setTotalCount(int totalCount) {
        TotalCount = totalCount;
    }
}
