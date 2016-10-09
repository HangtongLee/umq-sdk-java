package com.ucloud.umq.action;

import com.ucloud.umq.client.ServerResponseException;
import com.ucloud.umq.model.Message;
import com.ucloud.umq.model.Queue;
import com.ucloud.umq.model.Role;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by alpha on 9/29/16.
 */
public class Example {
    public static void main(String[] args) throws ServerResponseException {
        String host = "";
        String publicKey = "";
        String privateKey = "";
        String region = "";
        String account = "";
        String projectId = "";

        HttpClient client = HttpClient.NewClient(host, publicKey, privateKey, region, account, projectId);
        String queueId = client.createQueue(null, "queue_for_message_test", "Direct", "Yes", null);

        List<Role> publishers = client.createRole(queueId, 1, "Pub");
        List<Role> consumers = client.createRole(queueId, 1, "Sub");

        Role publisher = publishers.get(0);
        Role consumer = consumers.get(0);
        boolean succ = client.publishMsg(queueId, publisher.getId(), publisher.getToken(), "msg_test_1");
        if (succ) {
            System.out.println("publishMsg succ");
        } else {
            System.out.println("publishMsg fail");
        }

        Message msg  = client.getMsg(queueId, consumer.getId(), consumer.getToken());
        System.out.println(msg.toString());

        succ = client.ackMsg(queueId, consumer.getId(), msg.getMsgId());
        if (succ) {
            System.out.println("ackMsg succ");
        } else {
            System.out.println("ackMsg fail");
        }

        /*
        List<Queue> queues = client.listQueue(20, 0);
        System.out.println(queues);

        client.deleteRole(queueId, consumer.getId(), "Sub");
        client.deleteRole(queueId, publisher.getId(), "Pub");
        client.deleteQueue(queueId);
        */
        int organizationId = client.getOrganizationId();
        System.out.println(organizationId);
        SubscribeThread t = new SubscribeThread(client, organizationId, queueId, consumer.getId(), consumer.getToken());
        t.run();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 10; i++) {
            client.publishMsg(queueId, publisher.getId(), publisher.getToken(), "msg_test_" + i);
        }
    }

    static class SubscribeThread implements Runnable {
        Thread t;
        private HttpClient client;
        private int organizationId;
        private String queueId;
        private String consumerId;
        private String consumerToken;

        public SubscribeThread(HttpClient client, int organizationId, String queueId, String consumerId, String consumerToken) {
            this.client = client;
            this.organizationId = organizationId;
            this.queueId = queueId;
            this.consumerId = consumerId;
            this.consumerToken = consumerToken;
        }

        public void run() {
            try {
                client.subscribeQueue(this.organizationId, this.queueId, this.consumerId, this.consumerToken, new MyMsgHandler());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        class MyMsgHandler implements MsgHandler {
            public boolean process(MessageData msg) {
                System.out.println(msg);
                return true;
            }
        }
    }
}
