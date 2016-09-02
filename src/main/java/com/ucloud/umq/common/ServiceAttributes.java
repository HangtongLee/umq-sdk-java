package com.ucloud.umq.common;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by alpha on 8/5/16.
 */
public class ServiceAttributes {
    private static String SETTING_FILE =
            System.getProperty("user.home")
                    + System.getProperty("file.separator")
                    + "ucloud_umq.properties";

    private static Properties properties = new Properties();

    private static final Log log = LogFactory.getLog(ServiceAttributes.class);

    static {
        load();
    }

    public static void load() {
        load(SETTING_FILE);
    }

    public static void load (String configFile) {
        InputStream is = null;
        try {
            is = new FileInputStream(configFile);
            properties.load(is);
        } catch (FileNotFoundException e) {
            log.warn("The setting file " + configFile + " was not found.");
        } catch (java.io.IOException e) {
            log.warn("Load properties from file " + configFile + " failed");
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {

                }
            }
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }


    public static String getPublicKey() {
        return properties.getProperty("ucloud.PublicKey");
    }

    public static String getPrivateKey() {
        return properties.getProperty("ucloud.PrivateKey");
    }

    public static String getProjectId() {
        return properties.getProperty("ucloud.ProjectId");
    }

    public static String getApiUri() {
        return properties.getProperty("ucloud.ApiUri");
    }

    public static String getHttpUrl() {
        return properties.getProperty("umq.HttpUrl");
    }

    public static String getWsUrl() {
        return properties.getProperty("umq.WsUrl");
    }

    public static int getOrganizationId() {
        String orgId = properties.getProperty("ucloud.OrganizationId");
        return Integer.parseInt(orgId);
    }
}
