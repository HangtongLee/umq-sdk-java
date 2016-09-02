package com.ucloud.umq.client;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/11/16.
 */
public class ConsumeMsgRequest extends ApiRequest{
    @JsonProperty("Data")
    private ConsumeData data;

    public ConsumeMsgRequest(int orgId, String queueId, String consumerId, String consumerToken) {
        this.setAction("ConsumeMsg");
        this.data = new ConsumeData();
        this.data.setOrganizationId(orgId);
        this.data.setQueueId(queueId);
        this.data.setConsumerId(consumerId);
        this.data.setConsumerToken(consumerToken);
    }

    public class ConsumeData {
        @JsonProperty("OrganizationId")
        private int organizationId;

        @JsonProperty("QueueId")
        private String queueId;

        @JsonProperty("ConsumerId")
        private String consumerId;

        @JsonProperty("ConsumerToken")
        private String consumerToken;

        public int getOrganizationId() {
            return organizationId;
        }

        public void setOrganizationId(int organizationId) {
            this.organizationId = organizationId;
        }

        public String getQueueId() {
            return queueId;
        }

        public void setQueueId(String queueId) {
            this.queueId = queueId;
        }

        public String getConsumerId() {
            return consumerId;
        }

        public void setConsumerId(String consumerId) {
            this.consumerId = consumerId;
        }

        public String getConsumerToken() {
            return consumerToken;
        }

        public void setConsumerToken(String consumerToken) {
            this.consumerToken = consumerToken;
        }
    }

    public ConsumeData getData() {
        return data;
    }

    public void setData(ConsumeData data) {
        this.data = data;
    }
}
