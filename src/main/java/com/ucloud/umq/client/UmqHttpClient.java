package com.ucloud.umq.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by alpha on 8/5/16.
 */
public class UmqHttpClient {
    private String baseUrl = null;

    private HttpClient client;

    public UmqHttpClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String get(String uri, Map<String, Object> params) {
        client = HttpClientBuilder.create().build();

        String query = MapQuery.urlEncodeUTF8(params);

        String finalUrl = this.baseUrl + uri + "?" + query;
        HttpGet getRequest = new HttpGet(finalUrl);
        getRequest.addHeader("accept", "application/json");

        HttpResponse response = null;
        try {
            response = client.execute(getRequest);
            int statusCode = response.getStatusLine().getStatusCode();

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            StringBuffer result = new StringBuffer();
            String line = "";
            while ((line = br.readLine()) != null) {
                result.append(line);
            }

            return result.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public AckMsgResponse ackMsg(String queueId, String consumerId, String msgId) throws ServerResponseException {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "AckMsg");
        params.put("QueueId", queueId);
        params.put("ConsumerId", consumerId);
        params.put("MsgId", msgId);

        String res = this.get("/", params);
        if (res == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        AckMsgResponse apiRes;
        try {
            apiRes = mapper.readValue(res, AckMsgResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ServerResponseException(-1, "Response parse response error");
        }
        return apiRes;
    }
}
