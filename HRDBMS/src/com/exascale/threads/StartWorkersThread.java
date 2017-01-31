package com.exascale.threads;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;
import com.exascale.managers.HRDBMSWorker;

public class StartWorkersThread extends HRDBMSThread
{
	public StartWorkersThread()
	{
		this.setWait(true);
		this.description = "Start Workers";
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
				tokens.nextToken();
				final String wd = tokens.nextToken().trim();
				if (type.equals("C") || type.equals("W"))
				{
				}
				else
				{
					HRDBMSWorker.logger.error("Type found in nodes.cfg was not valid: " + type);
					System.exit(1);
				}

				String cmd = HRDBMSWorker.getHParms().getProperty("java_path");
				if (cmd.equals(""))
				{
					cmd = "java";
				}
				else
				{
					if (!cmd.endsWith("/"))
					{
						cmd += "/";
					}

                    // TODO - Add'l options for debugging / docker should be made optional
					cmd += "java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5015";
				}

				if (type.equals("W"))
				{
					HRDBMSWorker.getHParms().getProperty("hrdbms_user");
					HRDBMSWorker.logger.info("Starting worker " + host);
                    // TODO - can debugging work with  -cp HRDBMS.jar:.  instead of the bin classpath?
					final String command1 = "cd " + wd + "; ulimit -n " + HRDBMSWorker.getHParms().getProperty("max_open_files") + "; ulimit -u 100000; nohup " + cmd + " -Xmx" + HRDBMSWorker.getHParms().getProperty("Xmx_string") + " -Xms" + HRDBMSWorker.getHParms().getProperty("Xmx_string") + " -Xss" + HRDBMSWorker.getHParms().getProperty("stack_size") + " " + HRDBMSWorker.getHParms().getProperty("jvm_args") + " -classpath /home/hrdbms/app/bin: com.exascale.managers.HRDBMSWorker " + HRDBMSWorker.TYPE_WORKER + " &";

					try
					{

						// final java.util.Properties config = new
						// java.util.Properties();
						// config.put("StrictHostKeyChecking", "no");
						// final JSch jsch = new JSch();
						// final Session session = jsch.getSession(user, host,
						// 22);
						// final UserInfo ui = new MyUserInfo();
						// session.setUserInfo(ui);
						// jsch.addIdentity("~/.ssh/id_rsa");
						// session.setConfig(config);
						// session.connect();

						// final Channel channel = session.openChannel("exec");
						// ((ChannelExec)channel).setCommand(command1);
						// channel.setInputStream(null);
						// ((ChannelExec)channel).setErrStream(System.out);
						// ((ChannelExec)channel).setOutputStream(System.out);

						// final InputStream in2 = channel.getInputStream();
						// channel.connect();
						// final byte[] tmp = new byte[1024];
						// while (in2.available() > 0)
						// {
						// in2.read(tmp, 0, 1024);
						// }
						// channel.disconnect();
						// session.disconnect();
                        // TODO Added StrictHostKeyChecking=no for easy docker deployment... make optional?
						HRDBMSWorker.logger.info("Command: " + "ssh -o StrictHostKeyChecking=no -n -f " + host + "  \"bash -c '" + command1 + "'\"");
						Runtime.getRuntime().exec(new String[] { "bash", "-c", "ssh -o StrictHostKeyChecking=no -n -f " + host + "  \"bash -c '" + command1 + "'\"" });
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("Error starting a worker node.", e);
					}
				}

				line = in.readLine();
			}
			in.close();
			HRDBMSWorker.logger.debug("Start Workers is about to terminate.");
			HRDBMSWorker.getThreadList().remove(index);
			HRDBMSWorker.terminateThread(index);
			return;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error starting a worker node.", e);
		}
	}
}