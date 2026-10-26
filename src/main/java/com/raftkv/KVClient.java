package com.raftkv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVClient {

	private static final Logger logger = LoggerFactory.getLogger(KVClient.class);
	
	public static void main(String[] args) 
	{
		String HOST = ConfManager.getNode1Host();
		int PORT = ConfManager.getNode1Port();

		if (args.length >= 2)
		{
			HOST = args[0];
			PORT = Integer.parseInt(args[1]);
    		}

		Socket socket = null;
		Scanner console = null;
		BufferedReader reader = null;
		PrintWriter writer = null;

		try 
		{
			socket = new Socket(HOST, PORT);
			logger.info("Connected to KV server at {}:{}", HOST, PORT);
			console = new Scanner(System.in);
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
			writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
			logger.info("Available commands: PUT <key> <value>, GET <key>, DELETE <key>");
			logger.info("Type EXIT to quit");

			while (true) 
			{
				logger.info("Enter a command:");

				if (!console.hasNextLine()) 
				{
					break;
				}

				String command = console.nextLine();

				if (command.equalsIgnoreCase("EXIT")) 
				{
					logger.info("Exiting KV client");
					break;
				}

				writer.println(command);

				if (writer.checkError()) 
				{
					logger.error("Failed to send command to server");
					break;
				}

				String response = reader.readLine();

				if (response == null) 
				{
					logger.warn("Server disconnected");
					break;
				}

				logger.info("Server response: {}", response);
			}

		} 
		catch (IOException exception) 
		{
			logger.error("KV client encountered an I/O error", exception);

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

			if (console != null) 
			{
				console.close();
			}

			if (socket != null && !socket.isClosed()) 
			{
				try 
				{
					socket.close();
					logger.info("KV client connection closed");
				} 
				catch (IOException exception) 
				{
					logger.warn("Error closing client socket", exception);
				}
			}
		}
	}
}
