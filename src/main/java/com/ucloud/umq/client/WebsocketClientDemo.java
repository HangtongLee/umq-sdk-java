package com.ucloud.umq.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.ucloud.umq.common.ServiceAttributes;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_17_With_Origin;
import org.java_websocket.handshake.ServerHandshake;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by alpha on 8/5/16.
 */

public class WebsocketClientDemo {
    private static String httpUrl = ServiceAttributes.getHttpUrl();
    private static String wsUrl = ServiceAttributes.getWsUrl();
    private static int orgId = ServiceAttributes.getOrganizationId();
    private static String queueId = ServiceAttributes.getProperty("umq.QueueId");
    private static String consumerId = ServiceAttributes.getProperty("umq.ConsumerId");
    private static String consumerToken = ServiceAttributes.getProperty("umq.ConsumerToken");

    public static void main(String[] args) throws URISyntaxException {
        final UmqHttpClient httpClient = new UmqHttpClient(httpUrl);
        System.out.println(wsUrl);
        WebSocketClient client = new WebSocketClient(new URI(wsUrl), new Draft_17_With_Origin()) {
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
                            AckMsgResponse apiRes = httpClient.ackMsg(queueId, consumerId, result.getData().getMsgId());
                            if (apiRes.getRetCode() != 0) {
                                System.out.println("Ack msg error: " + result.getData().getMsgId());
                            }  else {
                                System.out.println("Ack msg success: " + result.getData().getMsgId());
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
                System.out.println("opened connection");
                ConsumeMsgRequest consumeMsgRequest = new ConsumeMsgRequest(orgId, queueId, consumerId, consumerToken);
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

        client.connect();
    }
}
