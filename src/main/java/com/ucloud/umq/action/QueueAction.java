package com.ucloud.umq.action;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ucloud.umq.client.*;
import com.ucloud.umq.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alpha on 8/8/16.
 */
public class QueueAction {
    /**
     * 创建队列
     * @param region 地区
     * @param queueName 队列名
     * @param pushType 发送方式。枚举值为"Direct","Fanout"
     * @param qos 是否需要对消费进行服务质量管控。枚举值为: "Yes",表示消费消息时客户端需要确认消息已收到(Ack模式)；"No",表示消费消息时不需要确认(NoAck模式)。默认为"Yes"。
     * @param couponId 优惠券Id。默认为空
     * @param remark 有业务组信息。默认为空
     * @return 生成的队列Id
     */
    public static String createQueue(String region, String queueName, String pushType, String qos, String couponId, String remark) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqCreateQueue");
        params.put("Region", region);
        params.put("QueueName", queueName);
        params.put("PushType", pushType);
        if (qos == null) {
            params.put("QoS", qos);
        }

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        CreateQueueResponse apiRes = null;
        try {
            System.out.println(res);
            apiRes = mapper.readValue(res, CreateQueueResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServerResponseException(-1, "Response parse response error");
        }
        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        QueueEntity queueEntity = apiRes.getDataSet();
        if (queueEntity == null) {
            throw new ServerResponseException(-1, "Response has no DataSet");
        }
        return queueEntity.getQueueId();
    }

    /**
     * 删除队列
     * @param region 地区
     * @param queueId 队列Id
     * @return 成功与否
     */
    public static boolean deleteQueue(String region, String queueId) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqDeleteQueue");
        params.put("Region", region);
        params.put("QueueId", queueId);

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

    /**
     * 返回队列信息（包括绑定的生产者,消费者）
     * @param region 地区
     * @param limit 获取的最大数目
     * @param offset 便宜
     * @return 队列信息
     * @throws ServerResponseException
     */
    public static List<Queue> listQueue(String region, int limit, int offset) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqGetQueue");
        params.put("Region", region);
        params.put("Limit", limit);
        params.put("offset", offset);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ListQueueResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ListQueueResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServerResponseException(-1, "Response parse response error");
        }

        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }

        List<Queue> queues = apiRes.getDataSet();
        if (queues == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }

        List<String> queueIds = new ArrayList<String>();

        if (queues != null) {
            for(Queue q: queues) {
                String id = q.getQueueId();
                List<Role> consumers = getConsumer(region, id, 1000000, 0);
                List<Role> publishers = getPublisher(region, id, 1000000, 0);
                q.setConsumers(consumers);
                q.setPublishers(publishers);
            }
        }
        return queues;
    }

    private static List<Role> getPublisher(String region, String queueId, int limit, int offset) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqGetRole");
        params.put("Role", "Pub");
        params.put("QueueId", queueId);
        params.put("Region", region);
        params.put("Limit", limit);
        params.put("offset", offset);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ListRoleResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ListRoleResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServerResponseException(-1, "Response parse response error");
        }

        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        List<Role> publishers = apiRes.getDataSet();
        if (publishers == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }
        return publishers;
    }

    private static List<Role> getConsumer(String region, String queueId, int limit, int offset) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqGetRole");
        params.put("Role", "Sub");
        params.put("QueueId", queueId);
        params.put("Region", region);
        params.put("Limit", limit);
        params.put("offset", offset);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ListRoleResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ListRoleResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServerResponseException(-1, "Response parse response error");
        }

        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        List<Role> consumers = apiRes.getDataSet();
        if (consumers == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }
        return consumers;
    }
}
