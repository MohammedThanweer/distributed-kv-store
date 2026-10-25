package com.raftkv;

public class LogEntry
{
	private final long index;
	private final long term;
	private final String command;

	public LogEntry(long index, long term, String command)
	{
		this.index = index;
		this.term = term;
		this.command = command;
	}

	public long getIndex()
	{
		return index;
	}

	public long getTerm()
	{
		return term;
	}

	public String getCommand()
	{
		return command;
	}

	@Override
	public String toString()
	{
		return "LogEntry{index=" + index + ", term=" + term + ", command='" + command + "'}";
	}
}
