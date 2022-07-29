package com.example.zookeeper;


import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class Client {

    private static ZooKeeper zookeeper;

    private final String zkNode = "node1:2181";

    private static final String ZK_NODE = "/testNode";

    @Before
    public void init() throws Exception{
        CountDownLatch countDownLatch = new CountDownLatch(1);
        zookeeper = new ZooKeeper(zkNode, 3000, event -> {
            if(event.getState() == Watcher.Event.KeeperState.SyncConnected && event.getType() == Watcher.Event.EventType.None) {
                log.info("connection success");
                countDownLatch.countDown();
            }
        });
        log.info("connecting");
        countDownLatch.await();
    }

    @Test
    public void createNodeSync() throws Exception{
        String path = zookeeper.create(ZK_NODE, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("created path:" + path);
    }

    @Test
    public void createNodeAsync() throws Exception{
        zookeeper.create(ZK_NODE, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, (rc, path, ctx, name) -> {
                    log.info("node created");
                }, "context");
        log.info("main thread running");
        Thread.sleep(3000);
    }

    @Test
    public void modify() throws Exception {
        Stat stat = new Stat();
        zookeeper.getData(ZK_NODE, false, stat);
        zookeeper.setData(ZK_NODE, "changedData".getBytes(), stat.getVersion());
        System.out.println("data changed");
    }
}
