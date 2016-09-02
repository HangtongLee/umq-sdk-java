package com.ucloud.umq.model;

/**
 * Created by alpha on 8/9/16.
 */
public class Consumer {
    private String Id;
    private String Token;
    private int CreateTime;

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        Id = id;
    }

    public String getToken() {
        return Token;
    }

    public void setToken(String token) {
        Token = token;
    }

    public int getCreateTime() {
        return CreateTime;
    }

    public void setCreateTime(int createTime) {
        CreateTime = createTime;
    }
}
