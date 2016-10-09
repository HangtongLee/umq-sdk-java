package com.ucloud.umq.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alpha on 8/9/16.
 */
public class Role {
    @JsonProperty("Id")
    private String Id;
    @JsonProperty("Token")
    private String Token;
    @JsonProperty("CreateTime")
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

    @Override
    public String toString() {
        return "Role{" +
                "Id='" + Id + '\'' +
                ", Token='" + Token + '\'' +
                ", CreateTime=" + CreateTime +
                '}';
    }
}
