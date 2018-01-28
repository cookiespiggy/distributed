package com.yuantek.bigdata.zkdist;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.collect.Lists;

public class DistributedClient {

	private static final String CONNECTSTRING = "192.168.182.135:2181,192.168.182.136:2181,192.168.182.137:2181";
	private static final int SESSIONTIMEOUT = 2000;
	private static final String PARENTNODE = "/servers";

	private static volatile List<String> serverList;

	ZooKeeper zk = null;

	/**
	 * 获取zk集群的客户端连接
	 * 
	 * @throws Exception
	 */
	public void getConnection() throws Exception {
		zk = new ZooKeeper(CONNECTSTRING, SESSIONTIMEOUT, (event) -> {
			try {
				// 重新更新服务器列表,并且继续注册监听
				getServerList();
			} catch (Exception e) {
			}
		});
	}

	/**
	 * 获取服务器信息列表
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void getServerList() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(PARENTNODE, true);

		// 先创建一个局部的list来存放服务器信息
		List<String> servers = Lists.newArrayList();
		children.forEach((child) -> {
			try {
				byte[] data = zk.getData(PARENTNODE + "/" + child, false, null);
				servers.add(new String(data));
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		});
		// 不要在循环里面去改,这样写一瞬间就改了
		// 把servers赋值给成员变量serverList,以提供给各业务线程使用
		serverList = servers;
		System.out.println(serverList);
	}

	/**
	 * 模拟业务功能
	 * 
	 * @throws InterruptedException
	 */
	public void handleBussiness(List<String> serverList) throws InterruptedException {

		System.out.println(" client start working...");

		// 模拟连接
		serverList.forEach(System.out::println);

		// 此处也可以搞一个socket服务器一直监听 也会阻塞
		// zk客户端线程设计的是守护线程 thread.setDaemon(true);
		Thread.sleep(Integer.MAX_VALUE);
	}

	public static void main(String[] args) throws Exception {
		// 获取zk连接
		DistributedClient distClient = new DistributedClient();
		distClient.getConnection();

		// 获取servers的子节点信息(并监听),从中获取服务器信息列表
		distClient.getServerList();
		// 业务线程启动
		distClient.handleBussiness(serverList);
	}
}
