package com.ucloud.umq.client;

/**
 * Created by alpha on 8/8/16.
 */
public class ServerResponseException extends Exception {
    private int retcode;
    private String message;

    public ServerResponseException(int retcode, String message) {
        this.retcode = retcode;
        this.message = message;
    }

    public int getRetcode() {
        return retcode;
    }

    public void setRetcode(int retcode) {
        this.retcode = retcode;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
