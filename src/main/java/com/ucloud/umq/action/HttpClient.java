package com.ucloud.umq.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.ucloud.umq.client.*;
import com.ucloud.umq.model.Message;
import com.ucloud.umq.model.Queue;
import com.ucloud.umq.model.QueueEntity;
import com.ucloud.umq.model.Role;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_17_With_Origin;
import org.java_websocket.handshake.ServerHandshake;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alpha on 9/29/16.
 */
public class HttpClient {
    private String host;
    private String publicKey;
    private String privateKey;
    private String region;
    private String account;
    private String projectId;
    private String ws;
    private String httpUrl;

    private UcloudApiClient client;

    private HttpClient(String host, String publicKey, String privateKey, String region, String account, String projectId) {
        this.host = host;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.region = region;
        this.account = account;
        this.region = region;
        if (projectId != null && !projectId.equals("")) {
            this.projectId = projectId;
        }

        this.ws = "ws://"  + host + "/ws";
//        this.ws = "ws://"  + host;
        this.httpUrl = "http://" + host;

        client = new UcloudApiClient("http://api.ucloud.cn", publicKey, privateKey);
    }

    /**
     * 创建Client
     * @param host 服务地址
     * @param publicKey 公钥
     * @param privateKey 私钥
     * @param region 地域
     * @param account 账户邮箱
     * @param projectId 项目Id
     * @return Client
     */
    static public HttpClient NewClient(String host, String publicKey, String privateKey, String region, String account, String projectId) {
        return new HttpClient(host, publicKey, privateKey, region, account, projectId);
    }

    /**
     * 创建队列
     * @param couponId 优惠券Id
     * @param remark 业务组名称
     * @param queueName 队列名字
     * @param pushType 推送方式,为枚举值: "Direct", 直接推送; "Fanout", 广播推送
     * @param qos 是否需要对消费进行服务质量管控。枚举值为: "Yes",表示消费消息时客户端需要确认消息已收到(Ack模式)；"No",表示消费消息时不需要确认(NoAck模式)
     * @return 队列Id
     * @throws ServerResponseException
     */
    public String createQueue(String couponId, String remark, String queueName, String pushType, String qos) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqCreateQueue");
        params.put("Region", this.region);
        params.put("Remark", remark);
        params.put("QueueName", queueName);
        params.put("PushType", pushType);

        if (couponId != null) {
            params.put("CouponId", couponId);
        }

        if (qos != null) {
            params.put("QoS", qos);
        }
        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        CreateQueueResponse apiRes = null;
        try {
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
     * @param queueId 队列Id
     * @return 删除的队列Id
     * @throws ServerResponseException
     */
    public String deleteQueue(String queueId) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqDeleteQueue");
        params.put("Region", this.region);
        params.put("QueueId", queueId);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ApiResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ApiResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }
        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        return queueId;
    }

    /**
     * 获取队列信息
     * @param limit 分页限制
     * @param offset 分页偏移
     * @return 队列信息
     */
    public List<Queue> listQueue(int limit, int offset) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqGetQueue");
        params.put("Region", this.region);
        params.put("Limit", limit);
        params.put("offset", offset);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

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
                List<Role> consumers = getConsumer(id, 1000000, 0);
                List<Role> publishers = getPublisher(id, 1000000, 0);
                q.setConsumers(consumers);
                q.setPublishers(publishers);
            }
        }
        return queues;
    }

    private List<Role> createRole(String queueId, int num, String role, String projectId) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqCreateRole");
        params.put("Region", this.region);
        params.put("QueueId", queueId);
        params.put("Num", num);
        params.put("Role", role);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ListRoleResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ListRoleResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }
        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        List<Role> roles = apiRes.getDataSet();
        if (roles == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }
        return roles;
    }

    /**
     * 创建角色
     * @param queueId 队列Id
     * @param num 创建数量
     * @param role 角色类型。枚举值: "Sub", 订阅者; "Pub", 生产者
     * @return 角色信息
     * @throws ServerResponseException
     */
    public List<Role> createRole(String queueId, int num, String role) throws ServerResponseException {
        return createRole(queueId, num, role, null);
    }

    /**
     * 删除角色
     * @param queueId 队列Id
     * @param roleId 角色Id
     * @param role 角色类型。枚举值: "Consumer", 消费者; "Producer", 生产者"
     * @return 删除成功的队列Id
     * @throws ServerResponseException
     */
    public String deleteRole(String queueId, String roleId, String role) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqDeleteRole");
        params.put("Region", this.region);
        params.put("QueueId", queueId);
        params.put("RoleId", roleId);
        params.put("Role", role);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ApiResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ApiResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }
        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        return roleId;
    }

    /**
     * 发布消息
     * @param queueId 队列Id
     * @param publisherId 发送者Id
     * @param publisherToken 发送者token
     * @param content 信息内容
     * @return 发送成功与否
     * @throws ServerResponseException
     */
    public boolean publishMsg(String queueId, String publisherId, String publisherToken, String content) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqPublishMsg");
        params.put("Region", this.region);
        params.put("QueueId", queueId);
        params.put("PublisherId", publisherId);
        params.put("PublisherToken", publisherToken);
        params.put("Content", content);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

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

    /**
     * 获取消息
     * @param queueId 队列Id
     * @param consumerId 订阅者Id
     * @param consumerToken 订阅者Token
     * @return 消息信息
     * @throws ServerResponseException
     */
    public Message getMsg(String queueId, String consumerId, String consumerToken) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqPullMsg");
        params.put("Region", this.region);
        params.put("QueueId", queueId);
        params.put("ConsumerId", consumerId);
        params.put("ConsumerToken", consumerToken);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

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

    /**
     * 回执消息
     * @param queueId 队列Id
     * @param consumerId 订阅者Id
     * @param msgId 消息Id
     * @return 成功与否
     * @throws ServerResponseException
     */
    public boolean ackMsg(String queueId, String consumerId, String msgId) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqAckMsg");
        params.put("Region", this.region);
        params.put("QueueId", queueId);
        params.put("ConsumerId", consumerId);
        params.put("MsgId", msgId);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

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

    private List<Role> getPublisher(String queueId, int limit, int offset) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqGetRole");
        params.put("Role", "Pub");
        params.put("QueueId", queueId);
        params.put("Region", this.region);
        params.put("Limit", limit);
        params.put("offset", offset);

        if (projectId != null) {
            params.put("ProjectId", projectId);
        }

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

    private List<Role> getConsumer(String queueId, int limit, int offset) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqGetRole");
        params.put("Role", "Sub");
        params.put("QueueId", queueId);
        params.put("Region", this.region);
        params.put("Limit", limit);
        params.put("offset", offset);

        if (this.projectId != null) {
            params.put("ProjectId", this.projectId);
        }

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

    /**
     * @return 组织Id
     * @throws ServerResponseException
     */
    public int getOrganizationId() throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "GetOrganizationId");
        params.put("UserEmail", this.account);
        if (this.projectId != null) {
            params.put("OrganizationAlias", this.projectId);
        }

        String res = client.rawGet(this.httpUrl, params);
        ObjectMapper mapper = new ObjectMapper();
        GetOrganizationIdResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, GetOrganizationIdResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServerResponseException(-1, "Response parse error");
        }
        return apiRes.getData();
    }

    /**
     *  订阅队列消息
     * @param organizationId 组织Id
     * @param queueId 队列Id
     * @param consumerId 订阅者Id
     * @param consumerToken 订阅者Token
     * @param handler 消息处理函数
     * @throws URISyntaxException
     */
    public void subscribeQueue(final int organizationId, final String queueId, final String consumerId, final String consumerToken, final MsgHandler handler) throws URISyntaxException {
        WebSocketClient wsClient = new WebSocketClient(new URI(this.ws), new Draft_17_With_Origin()) {
            @Override
            public void onMessage(String message) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    WebsocketStartConsumeMessageResponse result = null;
                    result = mapper.readValue(message, WebsocketStartConsumeMessageResponse.class);
                    if (result.getRetCode() == 0) {
                        System.out.println("Start Consuming message succeeded!");
                    } else {
                        System.out.println("Start Consuming message failed!");
                        System.out.println(result);
                    }
                } catch (IOException e) {
                    try {
                        WebsocketConsumedMessageResponse result = mapper.readValue(message, WebsocketConsumedMessageResponse.class);
                        System.out.println(result);
                        if (result.getRetCode() == 0) {
                            if (handler.process(result.getData())) {
                                ackMsg(queueId, consumerId, result.getData().getMsgId());
                            }
                        }
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    } catch (ServerResponseException e1) {
                        e1.printStackTrace();
                    }
                }
            }

            @Override
            public void onOpen(ServerHandshake handshake) {
                ConsumeMsgRequest consumeMsgRequest = new ConsumeMsgRequest(organizationId, queueId, consumerId, consumerToken);
                ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                String jsonStr = null;
                try {
                    jsonStr = ow.writeValueAsString(consumeMsgRequest);
                    System.out.println(jsonStr);
                    this.send(jsonStr);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("Code: " + code);
                System.out.println("Reason: " + reason);
                System.out.println( "closed connection" );
            }

            @Override
            public void onError(Exception ex) {
                ex.printStackTrace();
            }
        };

        wsClient.connect();
    }
}
