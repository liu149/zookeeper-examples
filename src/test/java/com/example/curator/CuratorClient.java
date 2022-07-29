package com.example.curator;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Test;

import javax.swing.plaf.IconUIResource;

@Slf4j
public class CuratorClient {
    private final String zkServer = "node1:2181";

    private static CuratorFramework client;

    @Before
    public void init() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder().retryPolicy(retryPolicy)
                .connectString(zkServer)
                 .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .namespace("base")
                .build();
        client.start();
    }

    @Test
    public void createNode() throws Exception{
        String path = client.create().forPath("/testCurator");
        log.info("create node {}", path);
    }

    @Test
    public void createNodeWithParent() throws Exception {
        String path = "/node-parent/node-client1";
        client.create().creatingParentsIfNeeded().forPath(path);
        log.info("create node {}", path);
    }

    @Test
    public void getNodeData() throws Exception {
        String data = new String(client.getData().forPath("/testCurator"));
        log.info("{} data = {}","/testCurator", data);
    }

    @Test
    public void changeNodeData() throws Exception {
        client.setData().forPath("/testCurator", "changed".getBytes());
    }

    @Test
    public void deleteNode() throws  Exception {
        client.delete().forPath("/testCurator");
        log.info("node deleted {}", "testCurator");
    }

    @Test
    public void createNodeAsync() throws Exception {
        client.create().inBackground((item1, item2)->{
          log.info("event===={}", item2.getName());
        }).forPath("/testCurator");
        Thread.sleep(3000);
    }
}
