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

// $Id: CompressedServerSocket.java 336 2008-05-09 14:40:20Z harald $

package com.exascale.optimizer.testing;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;




/**
 * Zip-compressed server socket.
 *
 * @author harald
 */
public final class CompressedServerSocket extends ServerSocket {
  
  
  /**
   * Creates a compressed server socket.
   * 
   * @param port the port number, or <code>0</code> to use any free port.
   * @throws java.io.IOException
   */
  public CompressedServerSocket(int port) throws IOException {
    super(port);
  }
  
  @Override
  public Socket accept() throws IOException { 
    Socket socket = new CompressedSocket();
    implAccept(socket);
    return socket;
  }  
}
