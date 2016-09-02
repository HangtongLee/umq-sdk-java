package com.ucloud.umq.action;

import com.ucloud.umq.client.UcloudApiClient;
import com.ucloud.umq.common.ServiceAttributes;


/**
 * Created by alpha on 8/8/16.
 */
public class UmqConnection {
    private volatile static UcloudApiClient instance;

    public static void NewConnection(String publicKey, String privateKey) {
//        System.out.println("publicKey " + publicKey);
//        System.out.println("privateKey " + privateKey);
        if (instance == null) {
            synchronized (UmqConnection.class) {
                if (instance == null) {
                    if (instance == null) {
                        instance = new UcloudApiClient(ServiceAttributes.getApiUri(), publicKey, privateKey);
                    }
                }
            }
        }
    }

    // singleton
    protected static UcloudApiClient getClient() {
        if (instance == null) {
            throw new IllegalArgumentException();
        }
        return instance;
    }
}
