package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public final class ConnectionManager 
{
	protected static final int WORKER_PORT = 3232;
	
	public static void main(String[] args)
	{
		if (args.length == 0)
		{
			new ResourceManager().start();
		}
		try
		{
			CompressedServerSocket server = new CompressedServerSocket(WORKER_PORT);
			while (true)
			{
				CompressedSocket sock = (CompressedSocket)server.accept();
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
