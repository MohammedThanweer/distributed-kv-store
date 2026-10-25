package com.raftkv;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftNode
{
	private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
	private final NodeRpcClient rpcClient = new NodeRpcClient();
	private final ScheduledExecutorService electionRpcExecutor;
	private final ScheduledExecutorService leaderHeartbeatScheduler;
	private ScheduledFuture<?> leaderHeartbeatTask;

	public enum State
	{
		FOLLOWER,
		CANDIDATE,
		LEADER
	}

	private final String nodeId;
	private State state;
	private long currentTerm;
	private String votedFor;

	private final ScheduledExecutorService electionScheduler;
	private ScheduledFuture<?> electionTimeoutTask;

	public RaftNode(String nodeId)
	{
		this.nodeId = nodeId;
		this.state = State.FOLLOWER;
		this.currentTerm = ConfManager.getInitialRaftTerm();
		this.votedFor = null;
		this.electionRpcExecutor = Executors.newSingleThreadScheduledExecutor();
		this.leaderHeartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

		this.electionScheduler = Executors.newSingleThreadScheduledExecutor(
			new java.util.concurrent.ThreadFactory()
			{
				@Override
				public Thread newThread(Runnable task)
				{
					Thread thread = new Thread(task, nodeId + "-election-timer");
					thread.setDaemon(true);
					return thread;
				}
			}
			);

		logger.info("Raft node {} initialized as {}", nodeId, state);
	}

	public synchronized String getNodeId()
	{
		return nodeId;
	}

	public synchronized State getState()
	{
		return state;
	}

	public synchronized long getCurrentTerm()
	{
		return currentTerm;
	}

	public synchronized String getVotedFor()
	{
		return votedFor;
	}

	public synchronized void startElectionTimeout()
	{
		if (state == State.FOLLOWER)
		{
			resetElectionTimeout();
		}
	}

	public synchronized void resetElectionTimeout()
	{
		if (state != State.FOLLOWER)
		{
			return;
		}

		if (electionTimeoutTask != null)
		{
			electionTimeoutTask.cancel(false);
		}

		int minimumTimeout = ConfManager.getElectionTimeoutMinMs();
		int maximumTimeout = ConfManager.getElectionTimeoutMaxMs();

		if (minimumTimeout <= 0 || maximumTimeout < minimumTimeout)
		{
			logger.error("Invalid election timeout configuration for {}", nodeId);
			return;
		}

		int timeout = ThreadLocalRandom.current().nextInt(minimumTimeout, maximumTimeout + 1);

		electionTimeoutTask = electionScheduler.schedule(
			new Runnable()
			{
				@Override
				public void run()
				{
					handleElectionTimeout();
				}
			}, timeout,TimeUnit.MILLISECONDS);

		logger.debug("{} election timeout reset to {} ms", nodeId, timeout);
	}

	private synchronized void handleElectionTimeout()
	{
		if (state != State.FOLLOWER)
		{
			return;
		}

		logger.warn("{} election timeout expired", nodeId);
		becomeCandidate();
	}

	public synchronized void becomeCandidate()
	{
		if (state == State.LEADER)
		{
			return;
		}

		if (electionTimeoutTask != null)
		{
			electionTimeoutTask.cancel(false);
			electionTimeoutTask = null;
		}

		currentTerm++;
		state = State.CANDIDATE;
		votedFor = nodeId;

		logger.info("{} became CANDIDATE for term {}", nodeId, currentTerm);

		electionRpcExecutor.execute(new Runnable()
		{
			@Override
			public void run()
			{
				sendRequestVoteToPeers();
			}
		});
	}
	public synchronized void becomeFollower(long term)
	{
		if (term < currentTerm)
		{
			logger.warn("{} rejected older term {}", nodeId, term);
			return;
		}

		if (term > currentTerm)
		{
			currentTerm = term;
			votedFor = null;
		}

		state = State.FOLLOWER;
		logger.info("{} became FOLLOWER for term {}", nodeId, currentTerm);
		resetElectionTimeout();
	}

	public synchronized boolean requestVote(String candidateId, long candidateTerm)
	{
		if (candidateTerm < currentTerm)
		{
			logger.info("{} rejected vote for {} because its term is older", nodeId, candidateId);
			return false;
		}

		if (candidateTerm > currentTerm)
		{
			currentTerm = candidateTerm;
			state = State.FOLLOWER;
			votedFor = null;
		}

		if (votedFor == null || votedFor.equals(candidateId))
		{
			votedFor = candidateId;
			resetElectionTimeout();

			logger.info("{} granted vote to {} for term {}", nodeId, candidateId, currentTerm);
			return true;
		}

		logger.info("{} rejected vote for {}; already voted for {}", nodeId, candidateId, votedFor);
		return false;
	}

	public synchronized void becomeLeader()
	{
		if (state != State.CANDIDATE)
		{
			logger.warn("{} cannot become LEADER from state {}", nodeId, state);
			return;
		}

		if (electionTimeoutTask != null)
		{
			electionTimeoutTask.cancel(false);
			electionTimeoutTask = null;
		}

		state = State.LEADER;
		logger.info("{} became LEADER for term {}", nodeId, currentTerm);
		startLeaderHeartbeats();
	}

	public synchronized void stopElectionTimeout()
	{
		if (electionTimeoutTask != null)
		{
			electionTimeoutTask.cancel(false);
			electionTimeoutTask = null;
		}

		electionScheduler.shutdownNow();
		electionRpcExecutor.shutdownNow();

		logger.info("{} election timers stopped", nodeId);
	}

	private String getPeerAddress(String peerId)
	{
		if (ConfManager.getNode1Id().equals(peerId))
		{
			return ConfManager.getNode1Host() + ConfManager.getColon() + ConfManager.getNode1Port();
		}

		if (ConfManager.getNode2Id().equals(peerId))
		{
			return ConfManager.getNode2Host() + ConfManager.getColon() + ConfManager.getNode2Port();
		}

		if (ConfManager.getNode3Id().equals(peerId))
		{
			return ConfManager.getNode3Host() + ConfManager.getColon() + ConfManager.getNode3Port();
		}

		return null;
	}
	private void sendRequestVoteToPeers()
	{
		String[] peerIds = {ConfManager.getNode1Id(), ConfManager.getNode2Id(),ConfManager.getNode3Id()};

		long electionTerm;

		synchronized (this)
		{
			if (state != State.CANDIDATE)
			{
				return;
			}

			electionTerm = currentTerm;
		}

		int votesReceived = 1;

		logger.info("{} started election for term {} with its own vote", nodeId, electionTerm);

		for (String peerId : peerIds)
		{
			if (nodeId.equals(peerId))
			{
				continue;
			}

			synchronized (this)
			{
				if (state != State.CANDIDATE || currentTerm != electionTerm)
				{
					logger.info("{} stopped processing votes for term {}", nodeId, electionTerm);
					return;
				}
			}

			String address = getPeerAddress(peerId);

			if (address == null)
			{
				logger.warn("No address configured for peer {}", peerId);
				continue;
			}

			String[] addressParts = address.split(ConfManager.getColon());

			String request = ConfManager.getRequestVoteCommand() + ConfManager.getSpace() + nodeId + ConfManager.getSpace() + electionTerm;

			String response = rpcClient.sendRequest(addressParts[0], Integer.parseInt(addressParts[1]),request);

			logger.info("{} received vote response from {}: {}", nodeId, peerId, response);

			if (response == null)
			{
				continue;
			}

			String[] responseParts = response.split(ConfManager.getSpace());

			if (responseParts.length != 2)
			{
				logger.warn("Invalid vote response from {}: {}", peerId, response);
				continue;
			}

			long responseTerm;

			try
			{
				responseTerm = Long.parseLong(responseParts[1]);
			}
			catch (NumberFormatException exception)
			{
				logger.warn("Invalid term in vote response from {}: {}", peerId, response);
				continue;
			}

			synchronized (this)
			{
				if (responseTerm > currentTerm)
				{
					becomeFollower(responseTerm);
					return;
				}

				if (state != State.CANDIDATE || currentTerm != electionTerm)
				{
					return;
				}

				if (responseTerm != electionTerm)
				{
					continue;
				}

				if (ConfManager.getVoteGrantedPrefix().equals(responseParts[0]))
				{
					votesReceived++;

					logger.info("{} has {} vote(s) for term {}", nodeId, votesReceived, electionTerm);

					int majority = (peerIds.length / 2) + 1;

					if (votesReceived >= majority)
					{
						becomeLeader();
						return;
					}
				}
				else if (ConfManager.getVoteRejectedPrefix().equals(responseParts[0]))
				{
					logger.info("{} did not receive a vote from {}", nodeId, peerId);
				}
				else
				{
					logger.warn("Unknown vote response from {}: {}", peerId, response);
				}
			}
		}
		logger.info("{} did not obtain a majority in term {}", nodeId, electionTerm);

		synchronized (this)
		{
			if (state == State.CANDIDATE && currentTerm == electionTerm)
			{
				scheduleElectionRetry();
			}
		}		
	}
	public synchronized boolean handleAppendEntries(String leaderId, long leaderTerm)
	{
		if (leaderTerm < currentTerm)
		{
			logger.info("{} rejected heartbeat from {} for older term {}", nodeId, leaderId, leaderTerm);

			return false;
		}

		if (leaderTerm > currentTerm)
		{
			currentTerm = leaderTerm;
			votedFor = null;
		}

		if (state == State.LEADER)
		{
			stopLeaderHeartbeats();
		}

		state = State.FOLLOWER;

		logger.info("{} accepted heartbeat from {} for term {}", nodeId, leaderId, currentTerm);

		resetElectionTimeout();

		return true;
	}

	private synchronized void stopLeaderHeartbeats()
	{
		if (leaderHeartbeatTask != null)
		{
			leaderHeartbeatTask.cancel(false);
			leaderHeartbeatTask = null;
		}
	}

	private synchronized void startLeaderHeartbeats()
	{
		stopLeaderHeartbeats();

		leaderHeartbeatTask = leaderHeartbeatScheduler.scheduleAtFixedRate(new Runnable()
		{
			@Override
			public void run()
			{
				sendLeaderHeartbeats();
			}
		}, 0, ConfManager.getRaftHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
	}

	private void sendLeaderHeartbeats()
	{
		String[] peerIds = {ConfManager.getNode1Id(), ConfManager.getNode2Id(), ConfManager.getNode3Id()};

		long leaderTerm;

		synchronized (this)
		{
			if (state != State.LEADER)
			{
				return;
			}

			leaderTerm = currentTerm;
		}

		for (String peerId : peerIds)
		{
			if (nodeId.equals(peerId))
			{
				continue;
			}

			String address = getPeerAddress(peerId);

			if (address == null)
			{
				continue;
			}

			String[] addressParts = address.split(ConfManager.getColon());

			String request = "APPEND_ENTRIES " + nodeId + " " + leaderTerm;

			String response = rpcClient.sendRequest(addressParts[0],Integer.parseInt(addressParts[1]), request);

			logger.debug("{} sent heartbeat to {}: {}", nodeId, peerId, response);
		}
	}
	private synchronized void scheduleElectionRetry()
	{
		if (state != State.CANDIDATE)
		{
			return;
		}

		int minimumTimeout = ConfManager.getElectionTimeoutMinMs();
		int maximumTimeout = ConfManager.getElectionTimeoutMaxMs();

		if (minimumTimeout <= 0 || maximumTimeout < minimumTimeout)
		{
			logger.error("Invalid election retry timeout configuration for {}", nodeId);
			return;
		}

		int timeout = ThreadLocalRandom.current().nextInt(
				minimumTimeout, maximumTimeout + 1);

		if (electionTimeoutTask != null)
		{
			electionTimeoutTask.cancel(false);
		}

		electionTimeoutTask = electionScheduler.schedule(new Runnable()
			{
			@Override
			public void run()
			{
				synchronized (RaftNode.this)
				{
					if (state != State.CANDIDATE)
					{
					return;
					}
				}

				logger.info("{} retrying election after timeout", nodeId);
				becomeCandidate();
				}
			}, timeout, TimeUnit.MILLISECONDS);

		logger.debug("{} election retry scheduled after {} ms", nodeId, timeout);
	}
}
