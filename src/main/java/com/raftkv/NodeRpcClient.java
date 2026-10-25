package com.raftkv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRpcClient 
{

	private static final Logger logger = LoggerFactory.getLogger(NodeRpcClient.class);

	public String sendRequest(String host, int port, String request) 
	{
		Socket socket = null;
		BufferedReader reader = null;
		PrintWriter writer = null;

		try 
		{
			socket = new Socket();
			socket.connect(new InetSocketAddress(host, port),ConfManager.getConnectionTimeoutMs());
			socket.setSoTimeout(ConfManager.getReadTimeoutMs());
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
			writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
			
			System.out.println("-------> REQUEST --------> "+ request);

			writer.println(request);

			if (writer.checkError()) 
			{
				logger.error("Failed to send request to {}:{}", host, port);
				return null;
			}

			String response = reader.readLine();
			System.out.println("*****RESPONSE*****"+ response);

			if (response == null) 
			{
				logger.warn("Node {}:{} closed the connection without a response", host, port);
				return null;
			}

			logger.info("Request sent to {}:{}; response: {}", host, port, response);
			return response;

		} 
		catch (IOException exception) 
		{
			logger.error("Communication failed with node {}:{}", host, port, exception);
			return null;

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
					logger.warn("Error closing RPC reader", exception);
				}
			}

			if (writer != null) 
			{
				writer.close();
			}

			if (socket != null && !socket.isClosed()) 
			{
				try 
				{
					socket.close();
				} 
				catch (IOException exception) 
				{
					logger.warn("Error closing RPC socket", exception);
				}
			}
		}
	}

	public static void main(String[] args) 
	{
		if (args.length != 2) 
		{
			logger.error("Usage: NodeRpcClient <host> <port>");
			return;
		}

		String host = args[0];
		int port;

		try 
		{
			port = Integer.parseInt(args[1]);
		} 
		catch (NumberFormatException exception) 
		{
			logger.error("Port must be a valid integer");
			return;
		}

		NodeRpcClient rpcClient = new NodeRpcClient();
		String response = rpcClient.sendRequest(host, port, "PING");

		if ("PONG".equals(response)) 
		{
			logger.info("Node {}:{} is reachable", host, port);
		} 
		else 
		{
			logger.warn("Node {}:{} did not respond with PONG", host, port);
		}
	}
}

