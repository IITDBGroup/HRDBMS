package com.exascale.compression;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public final class CompressedServerSocket extends ServerSocket
{

	/**
	 * Creates a compressed server socket.
	 *
	 * @param port
	 *            the port number, or <code>0</code> to use any free port.
	 * @throws java.io.IOException
	 */
	public CompressedServerSocket(final int port) throws IOException
	{
		super(port);
		// this.setReceiveBufferSize(64 * 1024 * 1024);
	}

	@Override
	public Socket accept() throws IOException
	{
		final Socket socket = CompressedSocket.newCompressedSocket();
		implAccept(socket);
		return socket;
	}
}
