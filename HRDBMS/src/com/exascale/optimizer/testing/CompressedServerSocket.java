package com.exascale.optimizer.testing;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
