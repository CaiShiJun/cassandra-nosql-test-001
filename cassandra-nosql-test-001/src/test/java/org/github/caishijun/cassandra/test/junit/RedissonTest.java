package org.github.caishijun.cassandra.test.junit;

import org.github.caishijun.cassandra.test.http.HttpClientUtils;
import org.junit.Test;

public class RedissonTest {
    private static String HOST = "localhost";
    private static int PORT = 8080;

    private static int FOR_TIMES = 1;
    private static int SLEEP_TIME = 10000;

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
    public void queryBuilder() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/queryBuilder"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void bindStatement() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/bindStatement"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void prepareStatement() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/prepareStatement"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void bindStatementQueryBuilder() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/bindStatementQueryBuilder"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void batchStatement() throws Exception {
        for (int i = 0; i < FOR_TIMES; i++) {
            HttpClientUtils.sendGetRequest(getUrl("/batchStatement"));
            Thread.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void runAll() throws Exception {
        while (true) {
            // createKeyspace();
            // createTable();
            insert();
            query();
            update();
            delete();
            queryBuilder();
            bindStatement();
            prepareStatement();
            bindStatementQueryBuilder();
            batchStatement();
        }
    }
}


