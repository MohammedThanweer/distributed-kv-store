package com.raftkv;

import java.util.Optional;

public class CommandHandler 
{

    private final KeyValueStore store;

    public CommandHandler(KeyValueStore store) 
    {
        this.store = store;
    }

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

            default:
                return "ERROR Unknown command";
        }
    }
}
