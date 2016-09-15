

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.StringTokenizer;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.HParms;

public class StartDB 
{
	private static HParms hparms; // configurable parameters
	
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
	
	public static HParms getHParms()
	{
		return hparms;
	}

	public static void main(String[] args)
	{
		try
		{
			hparms = HParms.getHParms();
			getHParms().setProperty("scfc", "true");
		}
		catch (final Exception e)
		{
			System.out.println("Could not load HParms");
			e.printStackTrace();
			System.exit(1);
		}
		
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

				String cmd = getHParms().getProperty("java_path");
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

					cmd += "java";
				}

				if (type.equals("C"))
				{
					final InetAddress addr = InetAddress.getByName(host);
					if (!isThisMyIpAddress(addr))
					{
						line = in.readLine();
						continue;
					}

					// final String user =
					// HRDBMSWorker.getHParms().getProperty("hrdbms_user");
					System.out.println("Starting master " + host);
					final String command1 = "cd " + wd + "; ulimit -n " + getHParms().getProperty("max_open_files") + "; ulimit -u 100000; nohup " + cmd + " -Xmx" + getHParms().getProperty("Xmx_string") + " -Xms" + getHParms().getProperty("Xmx_string") + " -Xss" + getHParms().getProperty("stack_size") + " " + getHParms().getProperty("jvm_args") + " -cp HRDBMS.jar:. com.exascale.managers.HRDBMSWorker 0" + " > /dev/null 2>&1 &";
					try
					{
						System.out.println("Command: " + "ssh -n -f " + host + "  \"bash -c '" + command1 + "'\"");
						Runtime.getRuntime().exec(new String[] { "bash", "-c", "ssh -n -f " + host + "  \"bash -c '" + command1 + "'\"" });
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("Failed to start master node.", e);
					}
				}

				line = in.readLine();
			}
		
			return;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Failed to start master node.", e);
		}
	}
}