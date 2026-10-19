
package com.raftkv;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerHeartbeat 
{
	private static final Logger logger = LoggerFactory.getLogger(PeerHeartbeat.class);
	private static final long HEARTBEAT_INTERVAL_SECONDS = 3;

	private final String nodeId;
	private final NodeRpcClient rpcClient;
	private final ScheduledExecutorService scheduler;
	private String[][] peers;

	private String node1 = ConfManager.getNode1Id();
	private String node2 = ConfManager.getNode2Id();
	private String node3 = ConfManager.getNode3Id();

	private String node1Port = String.valueOf(ConfManager.getNode1Port());
	private String node2Port = String.valueOf(ConfManager.getNode2Port());
	private String node3Port = String.valueOf(ConfManager.getNode3Port());

	public PeerHeartbeat(String nodeId) 
	{
		this.nodeId = nodeId;
		this.rpcClient = new NodeRpcClient();
		this.scheduler = Executors.newSingleThreadScheduledExecutor();
		configurePeers();
	}

	private void configurePeers() 
	{
		if (node1.equals(nodeId)) 
		{
			peers = new String[][] {{node2, node2Port}, {node3, node3Port}};
		} 
		else if (node2.equals(nodeId)) 
		{
			peers = new String[][] {{node1, node1Port}, {node3, node3Port}};
		} 
		else if (node3.equals(nodeId)) 
		{
			peers = new String[][] {{node1, node1Port}, {node2, node2Port}};
		} 
		else 
		{
			throw new IllegalArgumentException("Unknown node ID: " + nodeId);
		}
	}

	public void start() 
	{
		logger.info("{} starting peer heartbeats", nodeId);

		scheduler.scheduleAtFixedRate(new Runnable() 
		{
			@Override
			public void run() 
			{
				sendHeartbeats();
			}
		}, ConfManager.getHeartbeatInitialDelaySeconds(), ConfManager.getHeartbeatIntervalSeconds(), TimeUnit.SECONDS);
	}

	private void sendHeartbeats() {
		for (String[] peer : peers) {
			String peerId = peer[0];
			int peerPort = Integer.parseInt(peer[1]);
			String response = rpcClient.sendRequest("localhost", peerPort, "PING");

			if (("PONG " + peerId).equals(response)) {
				logger.info("{} heartbeat successful with {}", nodeId, peerId);
			} else {
				logger.warn("{} heartbeat failed with {}", nodeId, peerId);
			}
		}
	}

	public void stop() {
		scheduler.shutdownNow();
		logger.info("{} stopped peer heartbeats", nodeId);
	}
}

