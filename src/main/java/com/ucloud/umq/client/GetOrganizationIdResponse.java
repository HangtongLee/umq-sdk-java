package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 9/29/16.
 */
public class GetOrganizationIdResponse extends ApiResponse {
    @JsonProperty("Data")
    private int Data;

    public int getData() {
        return Data;
    }

    public void setData(int data) {
        Data = data;
    }
}
