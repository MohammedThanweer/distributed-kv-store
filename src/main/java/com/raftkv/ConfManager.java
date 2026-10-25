
package com.raftkv;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ConfManager 
{

	private static final Map<String, String> CONFIG;

	private static final int INITIAL_RAFT_TERM = 0;

	private static final String NODE_ID = "node";
	private static final String NODE1_ID = "node1";
	private static final String NODE2_ID = "node2";
	private static final String NODE3_ID = "node3";

	private static final String NODE1_HOST = "localhost";
	private static final String NODE2_HOST = "localhost";
	private static final String NODE3_HOST = "localhost";

	private static final int NODE1_PORT = 7001;
	private static final int NODE2_PORT = 7002;
	private static final int NODE3_PORT = 7003;

	private static final String CONFIG_HEARTBEAT_INITIAL_DELAY = "HEARTBEAT_INITIAL_DELAY_SECONDS";
	private static final String CONFIG_HEARTBEAT_INTERVAL = "HEARTBEAT_INTERVAL_SECONDS";
	private static final String CONFIG_CONNECTION_TIMEOUT = "CONNECTION_TIMEOUT_MS";
	private static final String CONFIG_READ_TIMEOUT = "READ_TIMEOUT_MS";

	private static final String HEARTBEAT_INITIAL_DELAY_SECONDS = "5";
	private static final String HEARTBEAT_INTERVAL_SECONDS = "3";
	private static final String CONNECTION_TIMEOUT_MS = "2000";
	private static final String READ_TIMEOUT_MS = "2000";

	private static final String PING_MESSAGE = "PING";
	private static final String PONG_PREFIX = "PONG";
	private static final String RPC_PING_COMMAND = "RPC_PING";
	private static final String RPC_RESULT_PREFIX = "RPC_RESULT";

	private static final String COMMAND_PUT = "PUT";
	private static final String COMMAND_GET = "GET";
	private static final String COMMAND_DELETE = "DELETE";
	private static final String COMMAND_PING = "PING";

	private static final String RESPONSE_OK = "OK";
	private static final String RESPONSE_ERROR = "ERROR";
	private static final String RESPONSE_NOT_FOUND = "NOT_FOUND";

	private static final String DEFAULT_NODE_ID = NODE1_ID;
	private static final int DEFAULT_PORT = NODE1_PORT;
	private static final String DEFAULT_HOST = NODE1_HOST;

	private static final int COMMAND_ARGUMENT_COUNT = 0;
	private static final String EMPTY_STRING = "";
	private static final String SPACE = " ";
	private static final String COLON = ":";

	private static final String CONFIG_ELECTION_TIMEOUT_MIN = "ELECTION_TIMEOUT_MIN_MS";
	private static final String CONFIG_ELECTION_TIMEOUT_MAX = "ELECTION_TIMEOUT_MAX_MS";

	private static final String UNKNOWN_CONFIG_MESSAGE = "Unknown configuration key: ";

	private static final String REQUEST_VOTE_COMMAND = "REQUEST_VOTE";
	private static final String VOTE_GRANTED_PREFIX = "VOTE_GRANTED";
	private static final String VOTE_REJECTED_PREFIX = "VOTE_REJECTED";

	private static final String ELECTION_TIMEOUT_MIN_MS = "3000";
	private static final String ELECTION_TIMEOUT_MAX_MS = "5000";
	private static final String RAFT_HEARTBEAT_INTERVAL_MS = "1000";

	static 
	{
		Map<String, String> values = new HashMap<String, String>();

		values.put(CONFIG_HEARTBEAT_INITIAL_DELAY, HEARTBEAT_INITIAL_DELAY_SECONDS);
		values.put(CONFIG_HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL_SECONDS);
		values.put(CONFIG_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_MS);
		values.put(CONFIG_READ_TIMEOUT, READ_TIMEOUT_MS);

		values.put(CONFIG_ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MIN_MS);
		values.put(CONFIG_ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MAX_MS);

		CONFIG = Collections.unmodifiableMap(values);
	}

	private ConfManager() 
	{
	}

	public static String getNodeId()
	{
		return NODE_ID;
	}

	public static String getNode1Id() 
	{ 
		return NODE1_ID; 
	}

	public static String getNode2Id() 
	{ 
		return NODE2_ID; 
	}

	public static String getNode3Id() 
	{ 
		return NODE3_ID; 
	}

	public static String getNode1Host() 
	{ 
		return NODE1_HOST; 
	}

	public static String getNode2Host() 
	{ 
		return NODE2_HOST; 
	}

	public static String getNode3Host() 
	{ 
		return NODE3_HOST; 
	}

	public static int getNode1Port() 
	{ 
		return NODE1_PORT; 
	}

	public static int getNode2Port() 
	{ 
		return NODE2_PORT; 
	}

	public static int getNode3Port() 
	{ 
		return NODE3_PORT; 
	}

	public static String getConfigHeartbeatInitialDelay() 
	{ 
		return CONFIG_HEARTBEAT_INITIAL_DELAY; 
	}

	public static String getConfigHeartbeatInterval() 
	{ 
		return CONFIG_HEARTBEAT_INTERVAL; 
	}

	public static String getConfigConnectionTimeout() 
	{ 
		return CONFIG_CONNECTION_TIMEOUT; 
	}

	public static String getConfigReadTimeout() 
	{ 
		return CONFIG_READ_TIMEOUT; 
	}

	public static long getHeartbeatInitialDelaySeconds() 
	{ 
		return Long.parseLong(HEARTBEAT_INITIAL_DELAY_SECONDS); 
	}

	public static long getHeartbeatIntervalSeconds() 
	{ 
		return Long.parseLong(HEARTBEAT_INTERVAL_SECONDS); 
	}

	public static int getConnectionTimeoutMs() 
	{ 
		return Integer.parseInt(CONNECTION_TIMEOUT_MS); 
	}

	public static int getReadTimeoutMs() 
	{ 
		return Integer.parseInt(READ_TIMEOUT_MS); 
	}

	public static String getPingMessage() 
	{ 
		return PING_MESSAGE; 
	}

	public static String getPongPrefix() 
	{ 
		return PONG_PREFIX; 
	}

	public static String getCommandRpcPing() 
	{ 
		return RPC_PING_COMMAND; 
	}

	public static String getRpcResultPrefix() 
	{ 
		return RPC_RESULT_PREFIX; 
	}

	public static String getCommandPut() 
	{ 
		return COMMAND_PUT; 
	}

	public static String getCommandGet() 
	{ 
		return COMMAND_GET; 
	}

	public static String getCommandDelete() 
	{ 
		return COMMAND_DELETE; 
	}

	public static String getCommandPing() 
	{ 
		return COMMAND_PING; 
	}

	public static String getResponseOk() 
	{ 
		return RESPONSE_OK; 
	}

	public static String getResponseError() 
	{ 
		return RESPONSE_ERROR; 
	}

	public static String getResponseNotFound() 
	{ 
		return RESPONSE_NOT_FOUND; 
	}

	public static String getDefaultNodeId() 
	{ 
		return DEFAULT_NODE_ID; 
	}

	public static int getDefaultPort() 
	{ 
		return DEFAULT_PORT; 
	}

	public static String getDefaultHost() 
	{ 
		return DEFAULT_HOST; 
	}

	public static int getCommandArgumentCount() 
	{ 
		return COMMAND_ARGUMENT_COUNT; 
	}

	public static String getEmptyString() 
	{ 
		return EMPTY_STRING; 
	}

	public static String getSpace() 
	{ 
		return SPACE; 
	}

	public static String getColon() 
	{ 
		return COLON; 
	}

	public static int getInitialRaftTerm()
	{
		return INITIAL_RAFT_TERM;
	}
	public static int getElectionTimeoutMinMs()
	{
		return Integer.parseInt(ELECTION_TIMEOUT_MIN_MS);
	}

	public static int getElectionTimeoutMaxMs()
	{
		return Integer.parseInt(ELECTION_TIMEOUT_MAX_MS);
	}

	public static String getRequestVoteCommand()
	{
		return REQUEST_VOTE_COMMAND;
	}

	public static String getVoteGrantedPrefix()
	{
		return VOTE_GRANTED_PREFIX;
	}

	public static String getVoteRejectedPrefix()
	{
		return VOTE_REJECTED_PREFIX;
	}

	public static int getRaftHeartbeatIntervalMs()
	{
		return Integer.parseInt(RAFT_HEARTBEAT_INTERVAL_MS);
	}
}
