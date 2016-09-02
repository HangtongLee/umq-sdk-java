package com.ucloud.umq.action;

import com.ucloud.umq.client.ServerResponseException;
import com.ucloud.umq.common.ServiceAttributes;
import com.ucloud.umq.model.Message;
import com.ucloud.umq.model.Queue;
import com.ucloud.umq.model.Role;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;

/**
 * Created by alpha on 8/10/16.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MessageActionTest {
    private static String region = "cn-bj2";
    private static String queueId = "";
    private static Role publisher = null;
    private static Role consumer = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String publicKey = ServiceAttributes.getPublicKey();
        String privateKey = ServiceAttributes.getPrivateKey();
        UmqConnection.NewConnection(publicKey, privateKey);

        queueId = QueueAction.createQueue(region, "queue_for_message_test", "Direct", "Yes", null, null);
        if(queueId.equals("")) {
            Assert.fail("setUp fail");
        }

        List<Role> publishers = RoleAction.createRole(region, queueId, 1, "Pub");
        Assert.assertNotEquals(publishers.size(), 0);
        publisher = publishers.get(0);

        List<Role> consumers = RoleAction.createRole(region, queueId, 1, "Sub");
        Assert.assertNotEquals(consumers.size(), 0);
        consumer = consumers.get(0);
    }

    @Test
    public void test01PublishAndConsumeMsg() {
        try {
            boolean succ = MessageAction.publishMsg(region, queueId, publisher.getId(), publisher.getToken(), "msg_test_1");
            Assert.assertTrue(succ);

            Message msg = MessageAction.pullMsg(region, queueId, consumer.getId(), consumer.getToken());
            Assert.assertEquals(msg.getMsgBody(), "msg_test_1");

            succ = MessageAction.ackMsg(region, queueId, consumer.getId(), msg.getMsgId());
            Assert.assertTrue(succ);
        } catch (ServerResponseException e) {
            e.printStackTrace();
            System.out.println("createQueue failed");
            Assert.fail();
        }
    }
}
