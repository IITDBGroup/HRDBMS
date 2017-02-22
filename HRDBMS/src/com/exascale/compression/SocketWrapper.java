/**
 * Tentackle - a framework for java desktop applications
 * Copyright (C) 2001-2008 Harald Krake, harald@krake.de, +49 7722 9508-0
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

// $Id: SocketWrapper.java 336 2008-05-09 14:40:20Z harald $

package com.exascale.compression;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

/**
 * Wrapper for existing sockets to add functionality for not-extensible sockets,
 * e.g. to add compression over SSLSockets (which are abstract and created by a
 * factory). It works by replacing the SocketImpl by a wrapped SocketImpl.
 */
public abstract class SocketWrapper extends Socket
{

	private final Socket socket; // the wrapped socket (there is no public

	// getImpl() in Socket :-()

	/**
	 * Creates a wrapper for the given socket.
	 *
	 * @param socket
	 *            the socket to wrap
	 * @throws SocketException
	 *             if creating the socket failed.
	 */
	public SocketWrapper(final Socket socket) throws SocketException
	{
		super(new SocketImplWrapper(socket));
		this.socket = socket;
	}

	@Override
	public void bind(final SocketAddress bindpoint) throws IOException
	{
		socket.bind(bindpoint);
	}

	@Override
	public void close() throws IOException
	{
		socket.close();
	}

	@Override
	public void connect(final SocketAddress endpoint, final int timeout) throws IOException
	{
		socket.connect(endpoint, timeout);
	}

	@Override
	public boolean equals(final Object obj)
	{
		// this is important! otherwise we get internal_error alerts in
		// SSL-layer!
		return obj instanceof SocketWrapper && ((SocketWrapper)obj).socket.equals(socket);
	}

	@Override
	public SocketChannel getChannel()
	{
		return socket.getChannel();
	}

	@Override
	public int hashCode()
	{
		// this is important! otherwise we get internal_error alerts in
		// SSL-layer!
		return socket.hashCode();
	}

	@Override
	public boolean isBound()
	{
		return socket.isBound();
	}

	@Override
	public boolean isClosed()
	{
		return socket.isClosed();
	}

	@Override
	public boolean isConnected()
	{
		return socket.isConnected();
	}

	@Override
	public boolean isInputShutdown()
	{
		return socket.isInputShutdown();
	}

	@Override
	public boolean isOutputShutdown()
	{
		return socket.isOutputShutdown();
	}

}
