package com.atguigu.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkClient {

    private String connectString = "zookeeper-1,zookeeper-2,zookeeper-3";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("----------------");
                List<String> children = null;

                for (String str : children) {
                    System.out.println(str);
                }

                System.out.println("----------------");
            }
        });
    }

    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/atguigu", "ss.avi".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        // 延时
        Thread.sleep(Long.MAX_VALUE);
    }


    @Test
    public void exist() throws KeeperException, InterruptedException {

        Stat stat = zkClient.exists("/atguigu", false);

        System.out.println(stat==null? "not exist " : "exist");
    }


}
