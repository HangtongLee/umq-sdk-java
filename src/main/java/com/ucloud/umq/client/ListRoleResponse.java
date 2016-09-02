package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ucloud.umq.model.Queue;
import com.ucloud.umq.model.Role;

import java.util.List;

/**
 * Created by alpha on 8/10/16.
 */
public class ListRoleResponse extends ApiResponse {
    @JsonProperty("DataSet")
    private List<Role> DataSet;

    public List<Role> getDataSet() {
        return DataSet;
    }

    public void setDataSet(List<Role> dataSet) {
        DataSet = dataSet;
    }
}



