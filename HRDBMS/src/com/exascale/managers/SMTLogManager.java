package com.exascale.managers;

import java.util.logging.Level;
import org.sosy_lab.common.configuration.Configuration;
import org.sosy_lab.common.log.LogManager;

public class SMTLogManager implements org.sosy_lab.common.log.LogManager
{
	private final String component;

	private SMTLogManager()
	{
		this.component = "";
	}

	private SMTLogManager(final String component)
	{
		this.component = component;
	}

	public static LogManager create(final Configuration config)
	{
		return new SMTLogManager();
	}

	@Override
	public void flush()
	{
	}

	@Override
	public void log(final Level arg0, final Object... arg1)
	{
		final StringBuilder sb = new StringBuilder();
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

		final String args = sb.toString();

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
	public void logDebugException(final Throwable arg0)
	{
		HRDBMSWorker.logger.debug(component + ": ", arg0);
	}

	@Override
	public void logDebugException(final Throwable arg0, String arg1)
	{
		arg1 = component + ": " + arg1;
		HRDBMSWorker.logger.debug(arg1, arg0);
	}

	@Override
	public void logException(final Level arg0, final Throwable arg1, String arg2)
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
	public void logf(final Level arg0, final String arg1, final Object... arg2)
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
	public void logUserException(final Level arg0, final Throwable arg1, final String arg2)
	{
		logException(arg0, arg1, arg2);
	}

	@Override
	public LogManager withComponentName(final String arg0)
	{
		return new SMTLogManager(arg0);
	}

	@Override
	public boolean wouldBeLogged(final Level arg0)
	{
		return true; // TODO
	}
}
