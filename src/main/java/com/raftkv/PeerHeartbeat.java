
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

	public PeerHeartbeat(String nodeId) 
	{
		this.nodeId = nodeId;
		this.rpcClient = new NodeRpcClient();
		this.scheduler = Executors.newSingleThreadScheduledExecutor();
		configurePeers();
	}

	private void configurePeers() 
	{
		if ("node1".equals(nodeId)) 
		{
			peers = new String[][] {{"node2", "7002"}, {"node3", "7003"}};
		} 
		else if ("node2".equals(nodeId)) 
		{
			peers = new String[][] {{"node1", "7001"}, {"node3", "7003"}};
		} 
		else if ("node3".equals(nodeId)) 
		{
			peers = new String[][] {{"node1", "7001"}, {"node2", "7002"}};
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
		}, 5, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
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

