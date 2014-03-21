package com.exascale.client;

import java.net.Socket;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class HRDBMSDriver implements Driver
{
	static
	{
		try
		{
			DriverManager.registerDriver(new HRDBMSDriver());
		}
		catch (final Exception e)
		{
			e.printStackTrace(System.out);
		}
	}

	@Override
	public boolean acceptsURL(String arg0) throws SQLException
	{
		// TODO
	}

	@Override
	public Connection connect(String arg0, Properties arg1) throws SQLException
	{
		try
		{
			final String protocol = arg0.substring(0, 11);
			if (!protocol.equals("jdbc:hrdbms://"))
			{
				return null;
			}

			final int portDelim = arg0.indexOf(":", 11);
			final int dbDelim = arg0.indexOf("/", portDelim);
			final String hostname = arg0.substring(11, portDelim);
			final String port = arg0.substring(portDelim + 1, dbDelim);
			final String database = arg0.substring(dbDelim + 1);
			final int portNum = Integer.parseInt(port);
			final Socket sock = new Socket(hostname, portNum);
			return new HRDBMSConnection(sock, arg1.getProperty("user"), arg1.getProperty("password"), database);
		}
		catch (final Exception e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public int getMajorVersion()
	{
		return 1;
	}

	@Override
	public int getMinorVersion()
	{
		return 0;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1) throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	@Override
	public boolean jdbcCompliant()
	{
		return true;
	}

}
