package com.exascale.misc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class MyReentrantLock extends ReentrantLock
{
	private final ArrayList<String> owners = new ArrayList<String>();

	public ArrayList<String> getOwners()
	{
		synchronized (owners)
		{
			return (ArrayList<String>)owners.clone();
		}
	}

	@Override
	public void lock()
	{
		super.lock();
		// save stack trace to owners
		final StringWriter sw = new StringWriter();
		new Throwable("").printStackTrace(new PrintWriter(sw));
		final String stackTrace = sw.toString();
		synchronized (owners)
		{
			owners.add(stackTrace);
		}
	}

	@Override
	public void unlock()
	{
		super.unlock();
		// remove last entry from owners
		synchronized (owners)
		{
			owners.remove(owners.size() - 1);
		}
	}

	public Thread whoIsOwner()
	{
		return super.getOwner();
	}
}
