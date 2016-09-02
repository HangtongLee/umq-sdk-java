package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ucloud.umq.model.Message;

import java.util.List;

/**
 * Created by alpha on 8/10/16.
 */
public class PullMessageResponse extends ApiResponse{
    @JsonProperty("DataSet")
    private MessageWithMetadata DataSet;


    public MessageWithMetadata  getDataSet() {
        return DataSet;
    }

    public void setDataSet(MessageWithMetadata dataSet) {
        DataSet = dataSet;
    }

    public class MessageWithMetadata {
        @JsonProperty("IsStacked")
        private String isStacked;
        @JsonProperty("Msgs")
        private List<Message> msgs;

        public String getIsStacked() {
            return isStacked;
        }

        public void setIsStacked(String isStacked) {
            this.isStacked = isStacked;
        }

        public List<com.ucloud.umq.model.Message> getMsgs() {
            return msgs;
        }

        public void setMsgs(List<com.ucloud.umq.model.Message> msgs) {
            this.msgs = msgs;
        }
    }
}

