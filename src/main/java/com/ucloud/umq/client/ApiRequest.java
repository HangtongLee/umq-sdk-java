package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/11/16.
 */
public class ApiRequest {
    @JsonProperty("Action")
    private String action;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
