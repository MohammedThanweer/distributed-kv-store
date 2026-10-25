package com.raftkv;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
	private final List<LogEntry> logEntries;
	private long commitIndex;
	private long lastApplied;
	private final KeyValueStore store;

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
	private final Map<String, Long> matchIndex;

	public RaftNode(String nodeId, KeyValueStore store)
	{
		this.nodeId = nodeId;
		this.state = State.FOLLOWER;
		this.currentTerm = ConfManager.getInitialRaftTerm();
		this.votedFor = null;
		this.electionRpcExecutor = Executors.newSingleThreadScheduledExecutor();
		this.leaderHeartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
		this.logEntries = new ArrayList<LogEntry>();
		this.commitIndex = 0L;
		this.lastApplied = 0L;
		this.matchIndex = new HashMap<String, Long>();
		this.store = store;

		this.matchIndex.put(ConfManager.getNode1Id(), 0L);
		this.matchIndex.put(ConfManager.getNode2Id(), 0L);
		this.matchIndex.put(ConfManager.getNode3Id(), 0L);

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
		long leaderCommitIndex;

		synchronized (this)
		{
			if (state != State.LEADER)
			{
				return;
			}

			leaderTerm = currentTerm;
			leaderCommitIndex = commitIndex;
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

			// Always send a heartbeat, even when the log is empty.
			String heartbeatRequest =
				ConfManager.getAppendEntriesCommand()
				+ ConfManager.getSpace() + nodeId
				+ ConfManager.getSpace() + leaderTerm
				+ ConfManager.getSpace() + leaderCommitIndex;

			String heartbeatResponse = rpcClient.sendRequest(
					addressParts[0],
					Integer.parseInt(addressParts[1]),
					heartbeatRequest);

			logger.debug("{} sent heartbeat to {}: {}",
					nodeId, peerId, heartbeatResponse);

			// Replicate any existing log entries after the heartbeat.
			List<LogEntry> entries = getLogEntries();

			for (LogEntry entry : entries)
			{
				String encodedCommand = Base64.getEncoder().encodeToString(
						entry.getCommand().getBytes(
							java.nio.charset.StandardCharsets.UTF_8));

				String entryRequest =
					ConfManager.getAppendEntriesCommand()
					+ ConfManager.getSpace() + nodeId
					+ ConfManager.getSpace() + leaderTerm
					+ ConfManager.getSpace() + entry.getIndex()
					+ ConfManager.getSpace() + entry.getTerm()
					+ ConfManager.getSpace() + encodedCommand
					+ ConfManager.getSpace() + leaderCommitIndex;


				String entryResponse = rpcClient.sendRequest(addressParts[0], Integer.parseInt(addressParts[1]),entryRequest);

				logger.debug("{} received replication response for entry {} from {}: {}", nodeId, entry.getIndex(), peerId, entryResponse);

				processReplicationResponse(peerId, entry.getIndex(), leaderTerm, entryResponse);
			}
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
	public synchronized LogEntry appendLocalEntry(String command)
	{
		if (state != State.LEADER)
		{
			logger.warn("{} cannot append a client command because it is not leader", nodeId);
			return null;
		}

		long index = logEntries.size() + 1L;
		LogEntry entry = new LogEntry(index, currentTerm, command);

		logEntries.add(entry);

		logger.info("{} appended log entry {}", nodeId, entry);

		return entry;
	}


	public synchronized LogEntry appendClientCommand(String command)
	{
		if (state != State.LEADER)
		{
			logger.warn("{} rejected client write because it is not the leader",
					nodeId);
			return null;
		}

		return appendLocalEntry(command);
	}


	public synchronized List<LogEntry> getLogEntries()
	{
		return Collections.unmodifiableList(new ArrayList<LogEntry>(logEntries));
	}

	public synchronized boolean handleAppendEntries(String leaderId, long leaderTerm, LogEntry entry)
	{
		if (entry == null)
		{
			return false;
		}

		if (entry.getIndex() < 1 || entry.getTerm() < 0)
		{
    			return false;
		}

		if (!handleAppendEntries(leaderId, leaderTerm))
		{
			return false;
		}

		long expectedIndex = logEntries.size() + 1L;

		if (entry.getIndex() <= logEntries.size())
		{
			LogEntry existingEntry = logEntries.get((int) entry.getIndex() - 1);

			if (existingEntry.getIndex() == entry.getIndex() && existingEntry.getTerm() == entry.getTerm()&& existingEntry.getCommand().equals(entry.getCommand()))
			{
				return true;
			}

			logger.warn("{} rejected conflicting log entry at index {}", nodeId, entry.getIndex());
			return false;
		}

		if (entry.getIndex() != expectedIndex)
		{
			logger.warn("{} expected log index {} but received {}", nodeId, expectedIndex, entry.getIndex());
			return false;
		}

		logEntries.add(entry);

		logger.info("{} stored replicated log entry {} from {}", nodeId, entry, leaderId);

		return true;
	}
	public synchronized long getCommitIndex()
	{
		return commitIndex;
	}

	public synchronized long getLastApplied()
	{
		return lastApplied;
	}

	public synchronized void recordReplicationAcknowledgement(String followerId, long acknowledgedIndex)
	{
		if (state != State.LEADER)
		{
			return;
		}

		if (!matchIndex.containsKey(followerId))
		{
			logger.warn("{} received acknowledgement from unknown node {}", nodeId, followerId);
			return;
		}

		long lastLogIndex = logEntries.size();

		if (acknowledgedIndex < 0 || acknowledgedIndex > lastLogIndex)
		{
			logger.warn("{} received invalid replication index {} from {}", nodeId, acknowledgedIndex, followerId);
			return;
		}

		long previousIndex = matchIndex.get(followerId);

		if (acknowledgedIndex > previousIndex)
		{
			matchIndex.put(followerId, acknowledgedIndex);

			logger.info("{} recorded replication acknowledgement from {} up to index {}", nodeId, followerId, acknowledgedIndex);
		}
	}

	public synchronized long getMatchIndex(String peerId)
	{
		Long index = matchIndex.get(peerId);

		if (index == null)
		{
			return 0L;
		}

		return index;
	}


	private void processReplicationResponse(String followerId, long entryIndex, long leaderTerm, String response)
	{
		if (response == null)
		{
			return;
		}

		String[] responseParts = response.trim().split("\\s+");

		if (responseParts.length != 2)
		{
			logger.warn("{} received an invalid replication response from {}: {}", nodeId, followerId, response);
			return;
		}

		long responseTerm;

		try
		{
			responseTerm = Long.parseLong(responseParts[1]);
		}
		catch (NumberFormatException exception)
		{
			logger.warn("{} received an invalid term from {}: {}", nodeId, followerId, response);
			return;
		}

		synchronized (this)
		{
			if (responseTerm > currentTerm)
			{
				becomeFollower(responseTerm);
				return;
			}

			if (state != State.LEADER || currentTerm != leaderTerm)
			{
				return;
			}

			if (responseTerm != leaderTerm)
			{
				return;
			}

			if (ConfManager.getAppendEntriesOkPrefix().equals(responseParts[0]))
			{
				recordReplicationAcknowledgement(followerId, entryIndex);
				advanceCommitIndex();
			}
			else
			{
				logger.debug("{} did not receive confirmation for entry {} from {}: {}",
						nodeId, entryIndex, followerId, response);
			}
		}
	}
	
	private synchronized void advanceCommitIndex()
	{
		if (state != State.LEADER)
		{
			return;
		}

		int clusterSize = 3;
		int majority = (clusterSize / 2) + 1;

		for (long index = logEntries.size(); index > commitIndex; index--)
		{
			LogEntry entry = logEntries.get((int) index - 1);

			if (entry.getTerm() != currentTerm)
			{
				continue;
			}

			int replicatedCount = 1; 

			for (String peerId : matchIndex.keySet())
			{
				if (nodeId.equals(peerId))
				{
					continue;
				}

				if (matchIndex.get(peerId) >= index)
				{
					replicatedCount++;
				}
			}

			if (replicatedCount >= majority)
			{
				commitIndex = index;

				logger.info("{} advanced commitIndex to {} with {} replicas", nodeId, commitIndex, replicatedCount);

				applyCommittedEntries();
				return;
			}
		}
	}


	private synchronized void applyCommittedEntries()
	{
		while (lastApplied < commitIndex)
		{
			int entryPosition = (int) lastApplied;
			LogEntry entry = logEntries.get(entryPosition);

			String[] commandParts =
				entry.getCommand().trim().split("\\s+", 3);

			if (commandParts.length == 0)
			{
				logger.error("Cannot apply empty command at index {}",
						entry.getIndex());
				return;
			}

			String command = commandParts[0].toUpperCase();

			if (ConfManager.getCommandPut().equals(command))
			{
				if (commandParts.length != 3)
				{
					logger.error("Invalid PUT at log index {}", entry.getIndex());
					return;
				}

				store.put(commandParts[1], commandParts[2]);
			}
			else if (ConfManager.getCommandDelete().equals(command))
			{
				if (commandParts.length < 2)
				{
					logger.error("Invalid DELETE at log index {}",
							entry.getIndex());
					return;
				}

				store.delete(commandParts[1]);
			}
			else
			{
				logger.error("Unsupported log command at index {}: {}",
						entry.getIndex(), entry.getCommand());
				return;
			}

			lastApplied = entry.getIndex();

			logger.info("{} applied committed entry {}",
					nodeId, lastApplied);
		}
	}

	private synchronized void updateCommitIndexFromLeader(long leaderCommitIndex)
	{
		if (leaderCommitIndex <= commitIndex)
		{
			return;
		}

		long newCommitIndex = Math.min(leaderCommitIndex, logEntries.size());

		if (newCommitIndex > commitIndex)
		{
			commitIndex = newCommitIndex;

			logger.info("{} updated commitIndex to {} from leader",
					nodeId, commitIndex);

			applyCommittedEntries();
		}
	}


	public synchronized boolean handleAppendEntries(String leaderId, long leaderTerm, long leaderCommitIndex)
	{
		boolean accepted = handleAppendEntries(leaderId, leaderTerm);

		if (!accepted)
		{
			return false;
		}

		updateCommitIndexFromLeader(leaderCommitIndex);

		return true;
	}

	public synchronized boolean handleAppendEntries(String leaderId, long leaderTerm, LogEntry entry, long leaderCommitIndex)
	{
		boolean accepted = handleAppendEntries(leaderId, leaderTerm, entry);

		if (!accepted)
		{
			return false;
		}

		updateCommitIndexFromLeader(leaderCommitIndex);

		return true;
	}

	
}
