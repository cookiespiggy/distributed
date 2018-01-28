package com.yuantek.bigdata.zkdist;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * 分布式系统对外提供服务的服务端程序 伪代码
 * 
 * @since 2018-01-28
 * @author mi.zhe
 *
 */
public class DistributedServer {

	private static final String CONNECTSTRING = "192.168.182.135:2181,192.168.182.136:2181,192.168.182.137:2181";
	private static final int SESSIONTIMEOUT = 2000;
	private static final String PARENTNODE = "/servers/";

	ZooKeeper zk = null;

	/**
	 * 获取zk集群的客户端连接
	 * 
	 * @throws Exception
	 */
	public void getConnection() throws Exception {
		zk = new ZooKeeper(CONNECTSTRING, SESSIONTIMEOUT, (event) -> {

			// 收到zk发回的rpc回调,需要做的事情
			System.out.println(event.getType() + "<===>" + event.getPath());

			try {
				zk.getChildren("/", true);
			} catch (Exception e) {
			}
		});
	}

	/**
	 * 向zk集群中注册服务器的元信息
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void registServerInfoToZk(String hostName) throws KeeperException, InterruptedException {
		String createPath = zk.create(PARENTNODE + "server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostName + "is online ..." + createPath);
	}

	/**
	 * 模拟业务功能
	 * 
	 * @throws InterruptedException
	 */
	public void handleBussiness(String hostName) throws InterruptedException {

		System.out.println(hostName + "start working...");

		// 此处也可以搞一个socket服务器一直监听 也会阻塞
		Thread.sleep(Integer.MAX_VALUE);
	}

	/**
	 * 伪代码 服务端程序
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 获取zk连接
		DistributedServer distServer = new DistributedServer();

		distServer.getConnection();

		// 利用zk提供的服务 注册服务器信息给它
		distServer.registServerInfoToZk(args[0]);

		// 处理业务了
		distServer.handleBussiness(args[0]);

	}
}
