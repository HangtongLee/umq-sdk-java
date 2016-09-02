package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ucloud.umq.model.Queue;
import com.ucloud.umq.model.QueueEntity;

import java.util.List;

/**
 * Created by alpha on 8/10/16.
 */
public class ListQueueResponse extends ApiResponse {
    @JsonProperty("DataSet")
    private List<Queue> DataSet;

    public List<Queue> getDataSet() {
        return DataSet;
    }

    public void setDataSet(List<Queue> dataSet) {
        DataSet = dataSet;
    }
}



