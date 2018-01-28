package com.yuantek.bigdata.zklock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 分布式共享锁
 */
public class DistributedClientLock {
    // 会话超时时间
    private static final int SESSION_TIMEOUT = 2000; // command + shift + u 转大写
    // Zookeepeer集群地址
    private String hosts = "192.168.182.135:2181,192.168.182.136:2181,192.168.182.137:2181";

    private String groupNode = "locks";
    private String subNode = "sub";
    private boolean haveLock = false;
    private ZooKeeper zk;
    // 记录自己创建的子节点路径
    private volatile String thisPath; // /locks/sub00000000001

    /**
     * 连接Zookeepeer
     * @throws IOException
     */
    public void connectZookeepeer() throws Exception {
        zk = new ZooKeeper(hosts,SESSION_TIMEOUT,(event) ->{
            try {
                // 判断事件类型,此处只处理子节点变化事件 0. 刚开始为NONE
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged
                        && event.getPath().equals("/" + groupNode)) {

                    // 获取父节点下的所有子节点,并对父节点进行监听 事件是继承下来的,上面已经定义了
                    List<String> children = zk.getChildren("/" + groupNode, true);

                    //sub00000000001
                    String thisNode = thisPath.substring(("/" + groupNode + "/").length());

                    // 排序
                    Collections.sort(children);

                    // 最小id
                    if(children.indexOf(thisNode) == 0) {
                        doSometing();
                        thisPath = zk.create("/" + groupNode + "/" + subNode,
                                null,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.EPHEMERAL_SEQUENTIAL);
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // 1. 先注册一把锁到zk上 /locks/sub000000000000
        thisPath = zk.create("/" + groupNode + "/" + subNode ,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL_SEQUENTIAL);

        // 随机休息几百毫秒,以便观察
        Thread.sleep(new Random().nextInt(1000));

        // 从父目录下,获取所有子节点,并且注册对父节点的监听
        List<String> children = zk.getChildren("/" + groupNode, true);

        if (children.size() == 1) {
            // 业务处理
            doSometing();
            thisPath = zk.create("/" + groupNode + "/" + subNode,null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        }
    }

    private void doSometing() throws Exception {
        try {
            System.out.println("获得了锁: " + thisPath);
            Thread.sleep(2000);
            // do something
        }finally {
            System.out.println("finished: " + thisPath);
            // 释放锁
            zk.delete(thisPath,-1);
        }
    }

    public static void main(String[] args) throws Exception {
        DistributedClientLock dl = new DistributedClientLock();
        dl.connectZookeepeer();
        Thread.sleep(Long.MAX_VALUE);
    }
}
