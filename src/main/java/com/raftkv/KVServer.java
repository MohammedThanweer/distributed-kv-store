
package com.raftkv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class KVServer 
{

	private static final int PORT = 7000;

	public static void main(String[] args) throws IOException 
	{

		KeyValueStore store = new KeyValueStore();
		CommandHandler handler = new CommandHandler(store);

		ServerSocket serverSocket = null;

		try 
		{
			serverSocket = new ServerSocket(PORT);
			System.out.println("KV Server listening on port " + PORT);

			while (true) 
			{

				final Socket client = serverSocket.accept();

				System.out.println("Client connected: "+ client.getRemoteSocketAddress());

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

			System.err.println("Server error: " + exception.getMessage());

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
					System.err.println("Error closing server socket");
				}
			}
		}
	}

	private static void handleClient(Socket client, CommandHandler handler) 
	{

		BufferedReader reader = null;
		PrintWriter writer = null;

		try {
			reader = new BufferedReader(new InputStreamReader(client.getInputStream(),StandardCharsets.UTF_8));

			writer = new PrintWriter(client.getOutputStream(), true, StandardCharsets.UTF_8);

			String command;

			while ((command = reader.readLine()) != null) 
			{

				String response = handler.handle(command);
				writer.println(response);
			}

		} 
		catch (IOException exception) 
		{

			System.err.println("Client connection error: "+ exception.getMessage());

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
					System.err.println("Error closing reader");
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
					System.err.println("Error closing client socket");
				}
			}
		}
	}
}

