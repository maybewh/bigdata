package com.atguigu.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private String connectString = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;  //
    private String currentMode; //

    public DistributedLock() throws IOException, InterruptedException, KeeperException {

        // 获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectLatch 如果连接上zk，则可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }

                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        // zk正常连接后，往下走程
        connectLatch.await();

        // 判断根节点/locks是否存在
        Stat stat = zk.exists("/locks", false);

        if (stat == null) {
            zk.create("/locks", "/locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // zk加锁
    public void zkLock() throws KeeperException, InterruptedException {
        // 创建对应的临时带序号节点
        currentMode = zk.create("/locks" + "seq-",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // wait一小会, 让结果更清晰一些
        Thread.sleep(10);

        // 判断创建的节点是否是最小的序号节点，如果是获取到锁；如果不是，监听他序号前一个节点
        List<String> children = zk.getChildren("/locks", false);

        // 如果children只有一个值，则获得锁
        if (children.size() == 1) {
            return;
        }

        Collections.sort(children);

        // 获取节点名称 seq-00000000
        String thisNode = currentMode.substring("/locks/".length());
        // 通过 seq-00000000 获取节点在集合中的位置
        int index = children.indexOf(children);
        if (index == -1) {
            System.out.println("数据异常");
            return;
        } else if (index == 0) {
            // 只有一个节点，就可以获取锁。也就是当前节点是最小序列节点
            return;
        } else {

            // 需要监听它前一个节点的变化
            waitPath = "/locks/" + children.get(index - 1);
            zk.getData(waitPath, true, new Stat());

            // 等待监听
            waitLatch.await();
            return;
        }

    }

    // 解锁
    public void unZkLock() {

        // 删除节点
        try {
            zk.delete(this.currentMode,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }
}
