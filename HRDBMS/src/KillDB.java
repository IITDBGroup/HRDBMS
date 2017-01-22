import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.StringTokenizer;

public class KillDB
{
	public static void main(String[] args)
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
				tokens.nextToken().trim();
				if (type.equals("C") || type.equals("W"))
				{
				}
				else
				{
					System.out.println("Type found in nodes.cfg was not valid: " + type);
					System.exit(1);
				}

				String cmd = "pkill java";

				// if (type.equals("C"))
				{
					InetAddress.getByName(host);

					// final String user =
					// HRDBMSWorker.getHParms().getProperty("hrdbms_user");
					// HRDBMSWorker.logger.info("Starting coordinator " + host);
					// final String command1 = "cd " + wd + "; ulimit -n 102400;
					// ulimit -u 100000; nohup " + cmd + " -Xmx" +
					// HRDBMSWorker.getHParms().getProperty("Xmx_string") + "
					// -Xms" +
					// HRDBMSWorker.getHParms().getProperty("Xmx_string") + "
					// -Xss" +
					// HRDBMSWorker.getHParms().getProperty("stack_size") + " "
					// + HRDBMSWorker.getHParms().getProperty("jvm_args") + "
					// -cp HRDBMS.jar:. com.exascale.managers.HRDBMSWorker " +
					// HRDBMSWorker.TYPE_COORD + " > /dev/null 2>&1 &";
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
						// HRDBMSWorker.logger.info("Command: " + "ssh -n -f " +
						// host + " \"sh -c '" + command1 + "'\"");
						Runtime.getRuntime().exec(new String[] { "bash", "-c", "ssh -o StrictHostKeyChecking=no -n -f " + host + "  \"sh -c '" + cmd + "'\"" });
					}
					catch (final Exception e)
					{
						System.out.println("Failed to execute pkill command");
					}
				}

				line = in.readLine();
			}
		}
		catch (final Exception e)
		{
			System.out.println("An exception occurred");
			e.printStackTrace();
		}
	}
}