
package com.raftkv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVServer 
{

	private static final Logger logger = LoggerFactory.getLogger(KVServer.class);

	public static void main(String[] args) throws IOException 
	{
		
		String nodeId = args.length > 0 ? args[0] : ConfManager.getNode1Id();
		int port = args.length > 1 ? Integer.parseInt(args[1]) : ConfManager.getNode1Port();

		KeyValueStore store = new KeyValueStore();
		RaftNode raftNode = new RaftNode(nodeId);
		CommandHandler handler = new CommandHandler(store, nodeId, raftNode);

		PeerHeartbeat heartbeat = new PeerHeartbeat(nodeId);
		//heartbeat.start();

		ServerSocket serverSocket = null;

		try
		{
			serverSocket = new ServerSocket(port);
			raftNode.startElectionTimeout();

			final ServerSocket activeServerSocket = serverSocket;
			final int serverPort = port;

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() 
			{
    				@Override
    				public void run() 
				{
        				try 
					{
            					if (!activeServerSocket.isClosed()) 
						{
                					activeServerSocket.close();
            					}
            					logger.info("Server on port {} stopped", serverPort);
        				} 
					catch (IOException exception) 
					{
            					logger.error("Error shutting down server on port {}", serverPort, exception);
        				}
    				}
			}));

			logger.info("KV Server started on port {}", port);	

			while (true) 
			{

				final Socket client = serverSocket.accept();

				logger.debug("Client connected: {}", client.getRemoteSocketAddress());

				Runnable clientTask = new Runnable() 
				{
					@Override
					public void run() 
					{
						handleClient(client, handler);
					}
				};

				Thread clientThread = new Thread(clientTask);
				clientThread.start();
			}

		} 
		catch (IOException exception) 
		{

			logger.error("Server encountered an I/O error", exception);
		} 
		finally 
		{

			if (serverSocket != null && !serverSocket.isClosed()) 
			{

				try 
				{
					serverSocket.close();
				} 
				catch (IOException exception) 
				{
					logger.error("Error closing server socket",exception);				
				}
			}
			raftNode.stopElectionTimeout();
			//heartbeat.stop();
		}
	}

	private static void handleClient(Socket client, CommandHandler handler) 
	{

		BufferedReader reader = null;
		PrintWriter writer = null;
		try 
		{
			reader = new BufferedReader(new InputStreamReader(client.getInputStream(),StandardCharsets.UTF_8));

			writer = new PrintWriter(client.getOutputStream(), true, StandardCharsets.UTF_8);

			String command;

			while ((command = reader.readLine()) != null) 
			{
				logger.debug("Received command: {}", command);
				String response = handler.handle(command);
				writer.println(response);
				logger.debug("Sent response: {}", response);
			}

		} 
		catch (IOException exception) 
		{

			logger.error("Error handling client {}", client.getRemoteSocketAddress(), exception);
		} 
		finally 
		{

			if (reader != null) 
			{
				try 
				{
					reader.close();
				} 
				catch (IOException exception) 
				{
					logger.warn("Error closing client reader", exception);
				}
			}

			if (writer != null) 
			{
				writer.close();
			}

			if (client != null && !client.isClosed()) 
			{
				try 
				{
					client.close();
				} 
				catch (IOException exception) 
				{
					logger.warn("Error closing client socket", exception);				
				}
			}
		}
	}
}

