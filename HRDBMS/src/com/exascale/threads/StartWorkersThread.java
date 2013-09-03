package com.exascale;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class StartWorkersThread extends HRDBMSThread
{	
	public StartWorkersThread()
	{
		this.setWait(true);
		this.description = "Start Workers";
	}
	
	public void run()
	{
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(new File("nodes.cfg")));
			String line = in.readLine();
			while (line != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, ",", false);
				String host = tokens.nextToken().trim();
				String type = tokens.nextToken().trim().toUpperCase();
				if (type.equals("C") || type.equals("W"))
				{
				}
				else
				{
					System.err.println("Type found in nodes.cfg was not valid: " + type);
				}
				
				if (type.equals("W"))
				{
					String user = HRDBMSWorker.getHParms().getProperty("hrdbms_user");
			        String command1 = "hrdbms " + HRDBMSWorker.TYPE_WORKER;
			        try{
			             
			            java.util.Properties config = new java.util.Properties(); 
			            config.put("StrictHostKeyChecking", "no");
			            JSch jsch = new JSch();
			            Session session=jsch.getSession(user, host, 22);
			        
			            session.setConfig(config);
			            session.connect();
			             
			            Channel channel=session.openChannel("exec");
			            ((ChannelExec)channel).setCommand(command1);
			            channel.setInputStream(null);
			            ((ChannelExec)channel).setErrStream(System.err);
			             
			            InputStream in2 = channel.getInputStream();
			            channel.connect();
			            byte[] tmp=new byte[1024];
			            while(true){
			              while(in2.available()>0){
			                int i=in2.read(tmp, 0, 1024);
			                if(i<0)break;
			                System.out.print(new String(tmp, 0, i));
			              }
			              if(channel.isClosed()){
			                System.out.println("exit-status: "+channel.getExitStatus());
			                break;
			              }
			              try{Thread.sleep(1000);}catch(Exception ee){}
			            }
			            channel.disconnect();
			            session.disconnect();
			            System.out.println("SSH COMMAND DONE");
			        }catch(Exception e){
			        	System.err.println("Failed to start worker nodes!");
			            e.printStackTrace(System.err);
			        }
				}
			}
		}
		catch(Exception e)
		{
			System.err.println("Failed to start worker nodes!");
			e.printStackTrace(System.err);
		}
	}
}