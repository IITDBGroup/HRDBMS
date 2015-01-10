package com.exascale.threads;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.StringTokenizer;
import com.exascale.managers.HRDBMSWorker;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class StartCoordsThread extends HRDBMSThread
{
	public StartCoordsThread()
	{
		this.setWait(true);
		this.description = "Start Coordinators";
	}

	public static boolean isThisMyIpAddress(InetAddress addr)
	{
		// Check if the address is a valid special local or loop back
		if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
		{
			return true;
		}

		// Check if the address is defined on any interface
		try
		{
			return NetworkInterface.getByInetAddress(addr) != null;
		}
		catch (final SocketException e)
		{
			return false;
		}
	}

	@Override
	public void run()
	{
		try
		{
			final BufferedReader in = new BufferedReader(new FileReader(new File("nodes.cfg")));
			String line = in.readLine();
			while (line != null)
			{
				final StringTokenizer tokens = new StringTokenizer(line, ",", false);
				final String host = tokens.nextToken().trim();
				final String type = tokens.nextToken().trim().toUpperCase();
				if (type.equals("C") || type.equals("W"))
				{
				}
				else
				{
					HRDBMSWorker.logger.error("Type found in nodes.cfg was not valid: " + type);
					System.exit(1);
				}

				if (type.equals("C"))
				{
					final InetAddress addr = InetAddress.getByName(host);
					if (isThisMyIpAddress(addr))
					{
						line = in.readLine();
						continue;
					}

					final String user = HRDBMSWorker.getHParms().getProperty("hrdbms_user");
					HRDBMSWorker.logger.info("Starting coordinator " + host);
					final String command1 = "java -Xmx" + HRDBMSWorker.getHParms().getProperty("Xmx_string") + " -Xss" + HRDBMSWorker.getHParms().getProperty("stack_size") + " -cp HRDBMS.jar:. com.exascale.managers.HRDBMSWorker " + HRDBMSWorker.TYPE_COORD;
					try
					{

						final java.util.Properties config = new java.util.Properties();
						config.put("StrictHostKeyChecking", "no");
						final JSch jsch = new JSch();
						final Session session = jsch.getSession(user, host, 22);
						final UserInfo ui = new MyUserInfo();
						session.setUserInfo(ui);
						jsch.addIdentity(".ssh/id_rsa");
						session.setConfig(config);
						session.connect();

						final Channel channel = session.openChannel("exec");
						((ChannelExec)channel).setCommand(command1);
						channel.setInputStream(null);
						((ChannelExec)channel).setErrStream(System.out);
						((ChannelExec)channel).setOutputStream(System.out);

						final InputStream in2 = channel.getInputStream();
						channel.connect();
						final byte[] tmp = new byte[1024];
						while (in2.available() > 0)
						{
							in2.read(tmp, 0, 1024);
						}
						channel.disconnect();
						session.disconnect();
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("Failed to start a coordinator node.", e);
					}
				}

				line = in.readLine();
			}
			HRDBMSWorker.logger.debug("Start Coordinator is about to terminate.");
			HRDBMSWorker.getThreadList().remove(index);
			HRDBMSWorker.terminateThread(index);
			return;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Failed to start a coordinator node.", e);
		}
	}

	private class MyUserInfo implements UserInfo
	{

		@Override
		public String getPassphrase()
		{
			return "";
		}

		@Override
		public String getPassword()
		{
			return null;
		}

		@Override
		public boolean promptPassphrase(String arg0)
		{
			return false;
		}

		@Override
		public boolean promptPassword(String arg0)
		{
			return false;
		}

		@Override
		public boolean promptYesNo(String arg0)
		{
			return false;
		}

		@Override
		public void showMessage(String arg0)
		{
		}

	}
}