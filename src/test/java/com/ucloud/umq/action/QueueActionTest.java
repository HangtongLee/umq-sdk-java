package com.ucloud.umq.action;

import com.ucloud.umq.client.ServerResponseException;
import com.ucloud.umq.common.ServiceAttributes;
import com.ucloud.umq.model.Queue;
import junit.framework.TestCase;
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
public class QueueActionTest {
    private static String region = "cn-bj2";
    private String queueId = "";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String publicKey = ServiceAttributes.getPublicKey();
        String privateKey = ServiceAttributes.getPrivateKey();
        UmqConnection.NewConnection(publicKey, privateKey);
        System.out.println("setUp");
    }

    @Test
    public void test01CreateQueue() {
        try {
            queueId = QueueAction.createQueue(region, "queue1", "Fanout", "Yes", null, null);
            System.out.println("createQueue succ " + queueId);
        } catch (ServerResponseException e) {
            e.printStackTrace();
            System.out.println("createQueue failed");
            Assert.fail();
        }
    }

    @Test
    public void test02GetQueue() {
        try {
            List<Queue> queues= QueueAction.listQueue(region, 100, 0);
            System.out.println("listQueue succ " + queues);
        } catch (ServerResponseException e) {
            e.printStackTrace();
            System.out.println("listQueue failed");
            Assert.fail();
        }
    }

    @Test
    public void test03DeleteQueue() {
        try {
            boolean succ = QueueAction.deleteQueue(region, queueId);
            Assert.assertTrue(succ);
        } catch (ServerResponseException e) {
            System.out.println("deleteQueue failed");
            Assert.fail();
        }
    }
}
