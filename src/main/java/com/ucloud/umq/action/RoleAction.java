package com.ucloud.umq.action;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ucloud.umq.client.ApiResponse;
import com.ucloud.umq.client.ListRoleResponse;
import com.ucloud.umq.client.ServerResponseException;
import com.ucloud.umq.client.UcloudApiClient;
import com.ucloud.umq.model.QueueEntity;
import com.ucloud.umq.model.Role;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alpha on 8/9/16.
 */
public class RoleAction {
    public static List<Role> createRole(String region, String queueId, int num, String role) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqCreateRole");
        params.put("Region", region);
        params.put("QueueId", queueId);
        params.put("Num", num);
        params.put("Role", role);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ListRoleResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ListRoleResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }
        if (apiRes.getRetCode() != 0) {
            throw new ServerResponseException(apiRes.getRetCode(), apiRes.getMessage());
        }
        List<Role> roles = apiRes.getDataSet();
        if (roles == null) {
            throw new ServerResponseException(-1, "Response parse queues error");
        }
        return roles;
    }

    public static boolean deleteRole(String region, String queueId, String roleId, String role) throws ServerResponseException {
        UcloudApiClient client = UmqConnection.getClient();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqDeleteRole");
        params.put("Region", region);
        params.put("QueueId", queueId);
        params.put("RoleId", roleId);
        params.put("Role", role);

        String res = client.get("/", params);
        ObjectMapper mapper = new ObjectMapper();
        ApiResponse apiRes = null;
        try {
            apiRes = mapper.readValue(res, ApiResponse.class);
        } catch (IOException e) {
            throw new ServerResponseException(-1, "Response parse response error");
        }
        if (apiRes.getRetCode() != 0) {
            return false;
        }
        return true;
    }
}
