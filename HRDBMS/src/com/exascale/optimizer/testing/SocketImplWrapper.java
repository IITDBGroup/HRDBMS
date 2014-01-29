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

// $Id: SocketImplWrapper.java 336 2008-05-09 14:40:20Z harald $

package com.exascale.optimizer.testing;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.SocketOptions;

/**
 * A wrapping SocketImpl.<br>
 * 
 * Nice to wrap SSL sockets to add compression, for example.
 * Notice that wrapped sockets are always connected/bound!
 * 
 * @author harald
 */
public class SocketImplWrapper extends SocketImpl {

  private final Socket socket;    // the wrapped socket
  
  
  private static class WrappedException extends IOException {
    private WrappedException() {
      super("operation not allowed for wrapped socket");
    }
  } 
  
  
  /**
   * Creates an impl wrapper that delegates all method invocations
   * to the wrapped socket.
   * 
   * @param socket the socket to wrap
   */
  public SocketImplWrapper(Socket socket) {
    this.socket = socket;
  }
  
  
  
  
  // ------------------- overrides SocketImpl ------------------------------

  @Override
  protected void shutdownInput() throws IOException {
    socket.shutdownInput();
  }

  @Override
  protected void shutdownOutput() throws IOException {
    socket.shutdownOutput();
  }  

  @Override
  protected FileDescriptor getFileDescriptor() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected InetAddress getInetAddress() {
    return socket.getInetAddress();
  }

  @Override
  protected int getPort() {
    return socket.getPort();
  }
  
  @Override
  protected int getLocalPort() {
    return socket.getLocalPort();
  }


  
  
  // --------------------- implements SocketImpl -----------------------------
  
  protected void create(boolean stream) throws IOException {
    throw new WrappedException();
  }

  protected void connect(String host, int port) throws IOException {
    throw new WrappedException();
  }

  protected void connect(InetAddress address, int port) throws IOException {
    throw new WrappedException();
  }

  protected void connect(SocketAddress address, int timeout) throws IOException {
    throw new WrappedException();
  }

  protected void bind(InetAddress host, int port) throws IOException {
    throw new WrappedException();
  }

  protected void listen(int backlog) throws IOException {
    throw new WrappedException();
  }

  protected void accept(SocketImpl s) throws IOException {
    throw new WrappedException();
  }

  protected InputStream getInputStream() throws IOException {
    return socket.getInputStream();
  }

  protected OutputStream getOutputStream() throws IOException {
    return socket.getOutputStream();
  }

  protected int available() throws IOException {
    return getInputStream().available();
  }

  protected void close() throws IOException {
    socket.close();
  }

  
  protected void sendUrgentData (int data) throws IOException {
    throw new WrappedException();
  }

  
  // ----------------------- implements SocketOptions -----------------------
  
  public void setOption(int optID, Object value) throws SocketException {
    
    Boolean bval = null;
    Integer ival = null;
    
    if (value instanceof Boolean) {
      bval = ((Boolean)value).booleanValue();
      ival = 0;
    }
    else  {
      ival = ((Integer)value).intValue();
      bval = ival > 0;
    }

    
    switch (optID) {
      
      // case SocketOptions.SO_BINDADDR:  read only!
      
      case SocketOptions.TCP_NODELAY:
        socket.setTcpNoDelay(bval);
        break;
          
      case SocketOptions.SO_REUSEADDR:
        socket.setReuseAddress(bval);
        break;
                    
      // case SocketOptions.SO_BROADCAST:  ?? -> not supported
      // case SocketOptions.IP_MULTICAST_IF:
      // case SocketOptions.IP_MULTICAST_IF2:
      // case SocketOptions.IP_MULTICAST_LOOP:      
          
      case SocketOptions.IP_TOS:
        socket.setTrafficClass(ival);
        break;
        
      case SocketOptions.SO_LINGER:
        socket.setSoLinger(bval, ival);
        break;
        
      case SocketOptions.SO_TIMEOUT:
        socket.setSoTimeout(ival);
        break;
        
      case SocketOptions.SO_SNDBUF:
        socket.setSendBufferSize(ival);
        break;

      case SocketOptions.SO_RCVBUF:
        socket.setReceiveBufferSize(ival);
        break;
          
      case SocketOptions.SO_KEEPALIVE:
        socket.setKeepAlive(bval);
        break;

      case SocketOptions.SO_OOBINLINE:
        socket.setOOBInline(bval);
        break;
          
      default:
          throw new SocketException("unsupported option ID");
    }
  }

  
  public Object getOption(int optID) throws SocketException {
    switch (optID) {
      case SocketOptions.TCP_NODELAY:   return Boolean.valueOf(socket.getTcpNoDelay());
      case SocketOptions.SO_BINDADDR:   return socket.getLocalAddress();
      case SocketOptions.SO_REUSEADDR:  return Boolean.valueOf(socket.getReuseAddress());
      // case SocketOptions.SO_BROADCAST:  ?? -> not supported
      // case SocketOptions.IP_MULTICAST_IF:
      // case SocketOptions.IP_MULTICAST_IF2:
      // case SocketOptions.IP_MULTICAST_LOOP:
      case SocketOptions.IP_TOS:        return Integer.valueOf(socket.getTrafficClass());
      case SocketOptions.SO_LINGER:     return Integer.valueOf(socket.getSoLinger());
      case SocketOptions.SO_TIMEOUT:    return Integer.valueOf(socket.getSoTimeout());
      case SocketOptions.SO_SNDBUF:     return Integer.valueOf(socket.getSendBufferSize());
      case SocketOptions.SO_RCVBUF:     return Integer.valueOf(socket.getReceiveBufferSize());
      case SocketOptions.SO_KEEPALIVE:  return Boolean.valueOf(socket.getKeepAlive());
      case SocketOptions.SO_OOBINLINE:  return Boolean.valueOf(socket.getOOBInline());
      
      default:                          throw new SocketException("unsupported option ID");
    }
  }

}
