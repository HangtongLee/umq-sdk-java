package com.ucloud.umq.client;

import com.ucloud.umq.common.ServiceAttributes;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by alpha on 8/5/16.
 */
public class UcloudApiClient {
    private String baseUrl = null;
    private String publicKey = null;
    private String privateKey = null;
    private String projectId = null;

    private HttpClient client;

    public UcloudApiClient(String baseUrl, String publicKey, String privateKey, String projectId) {
        this.baseUrl = baseUrl;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
        this.projectId = projectId;
    }

    public UcloudApiClient(String baseUrl, String publicKey, String privateKey) {
        this.baseUrl = baseUrl.trim();
        this.publicKey = publicKey;
        this.privateKey = privateKey;
    }


    public String get(String uri, Map<String, Object> params) {
        client = HttpClientBuilder.create().build();
        params.put("PublicKey", this.publicKey);
        if (this.projectId != null && !this.projectId.equals("")) {
            params.put("ProjectId", this.projectId);
        }

        String signature = null;
        try {
            signature = verify(params);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
        }
        params.put("Signature", signature);

        String query = MapQuery.urlEncodeUTF8(params);

        String finalUrl = this.baseUrl + uri + "?" + query;
        HttpGet getRequest = new HttpGet(this.baseUrl + uri + "?" + query);
        System.out.println("query uri "+ getRequest.getURI());
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

            System.out.println("response: " + result.toString());
            return result.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String verify(Map<String, Object> params) throws NoSuchAlgorithmException {

        StringBuffer str = new StringBuffer();
        Set<String> keys = params.keySet();
        List<String> keyList = new ArrayList<String>();
        for (String key: keys) {
            keyList.add(key);
        }
        Collections.sort(keyList);
        for (String key: keyList) {
//            System.out.println(key + " " + params.get(key).toString());
            str.append(key).append(params.get(key).toString());
        }
        str.append(this.privateKey);

        // use sha1 to encode keys
        return DigestUtils.sha1Hex(str.toString());
    }



    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public static void main(String[] args) {
        String baseUrl = ServiceAttributes.getApiUri();
        String publicKey = ServiceAttributes.getPublicKey();
        String privateKey = ServiceAttributes.getPrivateKey();

        UcloudApiClient api = new UcloudApiClient(baseUrl, publicKey, privateKey);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Action", "UmqCreateQueue");
        params.put("Region", "cn-bj2");
        params.put("QueueName", "queue1");
//        params.put("Action", "DescribeEIP");
//        params.put("Region", "cn-bj2");

        String res = api.get("/", params);
        System.out.println(res);
    }
}
