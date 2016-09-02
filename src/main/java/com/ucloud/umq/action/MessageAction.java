package com.ucloud.umq.action;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ucloud.umq.client.*;
import com.ucloud.umq.model.Message;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alpha on 8/9/16.
 */
public class MessageAction {
    public static Message pullMsg(String region, String queueId, String consumerId, String consumerToken) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqPullMsg");
        params.put("Region", region);
        params.put("QueueId", queueId);
        params.put("ConsumerId", consumerId);
        params.put("ConsumerToken", consumerToken);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        PullMessageResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, PullMessageResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }

        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        PullMessageResponse.MessageWithMetadata message = apiRes.getDataSet();
        if (message == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }
        List<Message> msgs = message.getMsgs();
        if (msgs == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }

        return msgs.get(0);
    }

    public static boolean publishMsg(String region, String queueId, String publisherId, String publisherToken, String content) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqPublishMsg");
        params.put("Region", region);
        params.put("QueueId", queueId);
        params.put("PublisherId", publisherId);
        params.put("PublisherToken", publisherToken);
        params.put("Content", content);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ApiResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ApiResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }

        if (apiRes.getRetCode() != 0) {
            return false;
        }
        return  true;
    }

    public static boolean ackMsg(String region, String queueId, String consumerId, String msgId) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqAckMsg");
        params.put("Region", region);
        params.put("QueueId", queueId);
        params.put("ConsumerId", consumerId);
        params.put("MsgId", msgId);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ApiResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ApiResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }

        if (apiRes.getRetCode() != 0) {
            return false;
        }
        return true;
    }
}
