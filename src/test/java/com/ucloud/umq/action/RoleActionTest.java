package com.ucloud.umq.action;

import com.ucloud.umq.client.ServerResponseException;
import com.ucloud.umq.common.ServiceAttributes;
import com.ucloud.umq.model.Queue;
import com.ucloud.umq.model.Role;
import junit.framework.TestCase;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.List;

/**
 * Created by alpha on 8/10/16.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RoleActionTest {
    private static String region = "cn-bj2";
    private static String queueId = "";
    private static String subId = "";
    private static String pubId = "";


    @BeforeClass
    public static void initTest() throws ServerResponseException {
        String publicKey = ServiceAttributes.getPublicKey();
        String privateKey = ServiceAttributes.getPrivateKey();
        UmqConnection.NewConnection(publicKey, privateKey);

        queueId = QueueAction.createQueue(region, "queue_for_role_test", "Direct", "Yes", null, null);
        if(queueId.equals("")) {
            Assert.fail("setUp fail");
        }
    }

    @Test
    public void test01CreateRole() {
        try {
            List<Role> publishers = RoleAction.createRole(region, queueId, 1, "Pub");
            Assert.assertNotEquals(publishers.size(), 0);
            pubId = publishers.get(0).getId();

            List<Role> consumers = RoleAction.createRole(region, queueId, 1, "Sub");
            Assert.assertNotEquals(consumers.size(), 0);
            subId = consumers.get(0).getId();
            System.out.println("createRole succ " + queueId);
        } catch (ServerResponseException e) {
            e.printStackTrace();
            System.out.println("createRole failed");
            Assert.fail();
        }
    }

    @Test
    public void test02GetRole() {
        try {
            List<Queue> queues= QueueAction.listQueue(region, 100, 0);
            Queue q = null;
            for (Queue qu: queues) {
                if (qu.getQueueId().equals(queueId)) {
                    q = qu;
                    break;
                }
            }
            if (q == null) {
                Assert.fail("cannot get specified queue");
            }
            Assert.assertNotEquals(q.getConsumers().size(), 0);
            Assert.assertNotEquals(q.getPublishers().size(), 0);
            System.out.println("listConsumer succ " + queues);
        } catch (ServerResponseException e) {
            e.printStackTrace();
            System.out.println("listPublisher failed");
            Assert.fail();
        }
    }

    @Test
    public void test03DeleteRole() {
        try {
            boolean succ = RoleAction.deleteRole(region, queueId, pubId, "Pub");
            Assert.assertTrue(succ);
            succ = RoleAction.deleteRole(region, queueId, subId, "Sub");
            Assert.assertTrue(succ);
        } catch (ServerResponseException e) {
            System.out.println("deleteQueue failed");
            Assert.fail();
        }
    }
}
