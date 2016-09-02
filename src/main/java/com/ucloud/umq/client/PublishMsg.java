package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ucloud.umq.model.Message;

/**
 * Created by alpha on 8/10/16.
 */
public class PublishMsg extends ApiResponse{
    @JsonProperty("DataSet")
    private Message DataSet;


    public Message  getDataSet() {
        return DataSet;
    }

    public void setDataSet(Message dataSet) {
        DataSet = dataSet;
    }
}
