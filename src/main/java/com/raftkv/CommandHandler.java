package com.raftkv;

import com.raftkv.ConfManager;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandHandler 
{
	private static final Logger logger = LoggerFactory.getLogger(CommandHandler.class);

	private final String nodeId;
	private final KeyValueStore store;

	private static final String GET = ConfManager.getCommandGet();
	private static final String PUT = ConfManager.getCommandPut();
	private static final String DELETE = ConfManager.getCommandDelete();
	private static final String PING = ConfManager.getCommandPing();
	private static final String RPC_PING = ConfManager.getCommandRpcPing();

	private final RaftNode raftNode;

	public CommandHandler(KeyValueStore store, String nodeId, RaftNode raftNode) 
	{
	    	this.store = store;
	    	this.nodeId = nodeId;
	    	this.raftNode = raftNode;
	}
	/*public CommandHandler(KeyValueStore store)
	{
    		this(store, ConfManager.getNodeId(), raftNode);
	}*/

	public String handle(String command) 
	{

		String[] parts = command.trim().split("\\s+", 3);

		if (parts.length == 0 || parts[0].isEmpty()) 
		{
			return "ERROR Empty command";
		}

		switch (parts[0].toUpperCase()) 
		{

			case "PUT":
				if (parts.length < 3) 
				{
					return "ERROR Usage: PUT <key> <value>";
				}

				store.put(parts[1], parts[2]);
				return "OK";

			case "GET":
				if (parts.length != 2) 
				{
					return "ERROR Usage: GET <key>";
				}

				Optional<String> value = store.get(parts[1]);

				if (value.isPresent()) 
				{
					return "VALUE " + value.get();
				} 
				else 
				{
					return "NOT_FOUND";
				}

			case "DELETE":
				if (parts.length != 2) 
				{
					return "ERROR Usage: DELETE <key>";
				}

				boolean deleted = store.delete(parts[1]);

				if (deleted) 
				{
					return "OK";
				} 
				else 
				{
					return "NOT_FOUND";
				}

			case "PING":
				logger.info("{} received PING request", nodeId);
    				return "PONG " + nodeId;

			case "RPC_PING":
				if(parts.length != 3)
				{
					return "ERROR Usage: RPC_PING <host> <port>";
				}
				try
				{
					int peerPort = Integer.parseInt(parts[2]);
        				NodeRpcClient rpcClient = new NodeRpcClient();
        				String response = rpcClient.sendRequest(parts[1], peerPort, "PING");
        				return response == null ? "ERROR Peer unreachable" : "RPC_RESULT " + response;
				}
				catch(NumberFormatException exception) 
				{
        				return "ERROR Invalid port";
    				}
			case "REQUEST_VOTE":
				String[] voteParts = command.split(" ");
				if (voteParts.length != 3) 
				{
					return "VOTE_REJECTED " + raftNode.getCurrentTerm();
				}

				String candidateId = voteParts[1];
				long candidateTerm;

				try 
				{
					candidateTerm = Long.parseLong(voteParts[2]);
				} 
				catch (NumberFormatException exception) 
				{
					return "VOTE_REJECTED " + raftNode.getCurrentTerm();
				}

				boolean voteGranted = raftNode.requestVote(candidateId, candidateTerm);

				if (voteGranted) 
				{
					return "VOTE_GRANTED " + raftNode.getCurrentTerm();
				}

				return "VOTE_REJECTED " + raftNode.getCurrentTerm();

			case "APPEND_ENTRIES":
				String[] appendParts = command.trim().split("\\s+");

				if (appendParts.length != 3)
				{
					return "APPEND_ENTRIES_REJECTED " + raftNode.getCurrentTerm();
				}

				String leaderId = appendParts[1];
				long leaderTerm;

				try
				{
					leaderTerm = Long.parseLong(appendParts[2]);
				}
				catch (NumberFormatException exception)
				{
					return "APPEND_ENTRIES_REJECTED " + raftNode.getCurrentTerm();
				}

				boolean accepted = raftNode.handleAppendEntries(leaderId, leaderTerm);

				if (accepted)
				{
					return "APPEND_ENTRIES_OK " + raftNode.getCurrentTerm();
				}

				return "APPEND_ENTRIES_REJECTED " + raftNode.getCurrentTerm();

			default:
				return "ERROR Unknown command";
		}
	}
}
