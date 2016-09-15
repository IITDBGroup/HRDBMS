package com.exascale.managers;

import java.util.logging.Level;
import org.sosy_lab.common.configuration.Configuration;
import org.sosy_lab.common.log.LogManager;

public class SMTLogManager implements org.sosy_lab.common.log.LogManager
{
	private String component;
	
	private SMTLogManager(String component)
	{
		this.component = component;
	}
	
	private SMTLogManager()
	{
		this.component = "";
	}
	
	@Override
	public void flush()
	{
	}

	@Override
	public void log(Level arg0, Object... arg1)
	{
		StringBuilder sb = new StringBuilder();
		if (component.length() > 0)
		{
			sb.append(component);
			sb.append(": ");
		}
		int i = 0;
		while (i < arg1.length)
		{
			sb.append(arg1[i++].toString());
			sb.append(" ");
		}
		
		String args = sb.toString();
		
		if (arg0.equals(Level.SEVERE))
		{
			HRDBMSWorker.logger.error(args);
		}
		else if (arg0.equals(Level.WARNING))
		{
			HRDBMSWorker.logger.warn(args);
		}
		else
		{
			HRDBMSWorker.logger.debug(args);
		}
	}

	@Override
	public void logDebugException(Throwable arg0)
	{
		HRDBMSWorker.logger.debug(component + ": ", arg0);
	}

	@Override
	public void logDebugException(Throwable arg0, String arg1)
	{
		arg1 = component + ": " + arg1;
		HRDBMSWorker.logger.debug(arg1, arg0);
	}

	@Override
	public void logException(Level arg0, Throwable arg1, String arg2)
	{
		arg2 = component + ": " + arg2;
		if (arg0.equals(Level.SEVERE))
		{
			HRDBMSWorker.logger.error(arg2, arg1);
		}
		else if (arg0.equals(Level.WARNING))
		{
			HRDBMSWorker.logger.warn(arg2, arg1);
		}
		else
		{
			HRDBMSWorker.logger.debug(arg2, arg1);
		}
	}

	@Override
	public void logUserException(Level arg0, Throwable arg1, String arg2)
	{
		logException(arg0, arg1, arg2);
	}

	@Override
	public void logf(Level arg0, String arg1, Object... arg2)
	{
		String args = String.format(arg1, arg2);
		args = component + ": " + args;
		if (arg0.equals(Level.SEVERE))
		{
			HRDBMSWorker.logger.error(args);
		}
		else if (arg0.equals(Level.WARNING))
		{
			HRDBMSWorker.logger.warn(args);
		}
		else
		{
			HRDBMSWorker.logger.debug(args);
		}
	}

	@Override
	public LogManager withComponentName(String arg0)
	{
		return new SMTLogManager(arg0);
	}

	@Override
	public boolean wouldBeLogged(Level arg0)
	{
		return true; //TODO
	}
	
	public static LogManager create(Configuration config)
	{
		return new SMTLogManager();
	}
}
