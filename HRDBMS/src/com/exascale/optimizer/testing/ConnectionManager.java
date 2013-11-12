package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionManager 
{
	private static final int WORKER_PORT = 3232;
	
	public static void main(String[] args)
	{
		new ResourceManager().start();
		try
		{
			ServerSocket server = new ServerSocket(WORKER_PORT);
			while (true)
			{
				Socket sock = server.accept();
				new ConnectionWorker(sock).start();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
}
