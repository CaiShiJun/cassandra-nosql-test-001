package org.github.caishijun.cassandra.test.junit;

import org.github.caishijun.cassandra.test.http.HttpClientUtils;
import org.junit.Test;

public class RedissonTest {
    private static String HOST = "localhost";
    private static int PORT = 8080;

    private static int FOR_TIMES = 1;
    private static int SLEEP_TIME = 5000;

    public static String getUrl(String uri) {
        return "http://" + HOST + ":" + PORT + uri;
    }

    @Test
    public void createKeyspace() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/createKeyspace"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void createTable() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/createTable"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void insert() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/insert"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void update() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/update"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void delete() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/delete"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void query() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/query"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void runAll() throws Exception {
        while (true) {
        }
    }
}