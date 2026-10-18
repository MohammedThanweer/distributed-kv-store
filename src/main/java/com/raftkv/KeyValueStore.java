package com.raftkv;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyValueStore 
{

    private final Map<String, String> data = new HashMap<>();

    public synchronized void put(String key, String value) 
    {
        data.put(key, value);
    }

    public synchronized Optional<String> get(String key) 
    {
        return Optional.ofNullable(data.get(key));
    }

    public synchronized boolean delete(String key) 
    {
        return data.remove(key) != null;
    }
}
