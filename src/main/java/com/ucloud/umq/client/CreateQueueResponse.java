package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ucloud.umq.model.QueueEntity;

/**
 * Created by alpha on 8/10/16.
 */
public class CreateQueueResponse extends ApiResponse{
    @JsonProperty("DataSet")
    private QueueEntity DataSet;


    public QueueEntity getDataSet() {
        return DataSet;
    }

    public void setDataSet(QueueEntity dataSet) {
        DataSet = dataSet;
    }
}
