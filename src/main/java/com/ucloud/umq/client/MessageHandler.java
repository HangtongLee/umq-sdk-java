package com.ucloud.umq.client;


/**
 * Created by alpha on 8/5/16.
 */
public interface MessageHandler {
    public void handle(WebsocketConsumedMessageResponse result);
}
