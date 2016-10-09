package com.ucloud.umq.action;

import com.ucloud.umq.model.Message;

/**
 * Created by alpha on 9/29/16.
 */
public interface MsgHandler {
    public boolean process(MessageData msg);
}
