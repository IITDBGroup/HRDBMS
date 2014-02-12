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

// $Id: CompressedOutputStream.java 466 2009-07-24 09:16:17Z svn $

package com.exascale.optimizer.testing;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;



/**
 * Stream to write compressed data to the underlying output stream.
 * <p>
 * Replacement for GZIPOutputStream and ZipOutputStream of java.util.zip
 * capable for communication link streams like tcp-sockets.
 * The standard classes are only file-capable (relying on EOF-detection).
 * In comlinks, however, there are several packets instead of a single file.
 * Hence, the stream must be blocked, i.e. the (varying) blocksize must
 * become part of the protocol to allow the corresponding input stream
 * to detect the end of the packets.
 * Furthermore, we optimize to compress only packets larger than a certain size.
 * All smaller packets will be transferred uncompressed.
 * For the packet size we use the first two bytes in the packet (short), while
 * MSBit signals whether the packet is compressed or not.
 * Because of this, the maximum buffer size is limited to 32K - 1.
 */
public final class CompressedOutputStream extends FilterOutputStream {

  /**
   * The maximum compression buffer size. The larger the buffer, the better the compression.
   */
  public static final int MAX_BUFFER_SIZE = Short.MAX_VALUE;
  
  
  final static int COMPRESSED = MAX_BUFFER_SIZE + 1;    // 0x8000 MSBit: 1 = compressed, 0 = uncompressed

  private int minCompressSize;          // minimum packet size for compression
  private int bufSize;                  // buffer size
  
  private byte[] orgBuf;                // original uncompressed data buffer
  private int orgLen;                   // number of bytes in orgBuf
  private Deflater deflater;            // the zip deflater
  private byte[] defBuf;                // deflated/compressed output data buffer
  private int defLen;                   // number of bytes in defBuf
  private byte[] byteBuf = new byte[1]; // single byte buffer for write(b)
  private boolean closed;               // true if closed
  
  // for statistic only (Level.FINE must be enabled)
  private long totalWritten;            // total number of bytes written by application
  private long totalCompressed;         // number of compressed bytes written to stream
  private long totalUncompressed;       // number of uncompressed bytes written to stream
  
  /**
   * Creates a compressed output stream.
   *
   * @param out the underlying output stream (e.g. from a socket)
   * @param bufSize the buffer size for compression. Packets larger than bufSize are split.
   * @param minCompressSize the minimum compressed packet size. Smaller packets pass the stream uncompressed.
   */
  public CompressedOutputStream(OutputStream out, int bufSize, int minCompressSize) {
    
    super(out);
    
    if (bufSize <= minCompressSize || bufSize > MAX_BUFFER_SIZE) {
      throw new IllegalArgumentException("illegal bufSize [" + minCompressSize + " < ?" + bufSize + "? <= " + MAX_BUFFER_SIZE + "]");
    }
    
    this.bufSize         = bufSize;
    this.minCompressSize = minCompressSize;
    
    orgBuf        = new byte[bufSize];
    deflater      = new Deflater(Deflater.BEST_COMPRESSION, true);  // with noWrap: less metadata -> better compression
    defBuf        = new byte[bufSize];
  }
  
  /**
   * Creates a compressed output stream with maximum allowed buffersize (32K-1) and
   * a default minCompressSize of 64.
   * 
   * @param out the underlying output stream
   */
  public CompressedOutputStream(OutputStream out) {
    this(out, MAX_BUFFER_SIZE, 64);
  }


  /**
   * Writes the specified <code>byte</code> to this output stream. 
   *
   * @param      b   the <code>byte</code>.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public void write(int b) throws IOException {
    byteBuf[0] = (byte)b;
    write(byteBuf, 0, 1);
  }

  
  /**
   * Writes <code>len</code> bytes from the specified 
   * <code>byte</code> array starting at offset <code>off</code> to 
   * this output stream.
   * <p>
   * Packets larger than the buffer size will be split and written to
   * the underlying output stream as separate packets.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    
    totalWritten += len;
    
    while (len > 0) {
      
      int num = len;
      if (num > bufSize - orgLen) {
        num = bufSize - orgLen;    // align to buffersize
      }
      
      System.arraycopy(b, off, orgBuf, orgLen, num);
      off    += num;
      len    -= num;
      orgLen += num;
      
      if (orgLen >= bufSize) {
        // buffer full: write/compress it!
        flushBuffer();
      }
    }
  }

  
  /**
   * Flushes this output stream and forces any buffered output bytes 
   * to be written out to the stream. 
   *
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public void flush() throws IOException {
    flushBuffer();    // write any pending data
    super.flush();
  }
  
  
  /**
   * Overridden to print stats only
   */
  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      super.close();
    }
  }
  
  
  /**
   * @return true if closed
   */
  public boolean isClosed() {
    return closed;
  }
  
  
  /**
   * Writes the header.
   *
   * @param size the logical packet size
   * @param compressed is true if packet is compressed, false if uncompressed
   */
  private void writeHeader(int size, boolean compressed) throws IOException {
    if (compressed) {
      size |= COMPRESSED;
    }
    // msb first
    out.write((size >>> 8) & 0xff);
    out.write(size & 0xff);
  }
  
  

  /**
   * Flushes the buffer.
   * If the packet size is large enough, the data will be compressed.
   * If the compressed data is smaller than the original data, a compressed
   * packet will be written. Otherwise, an uncompressed packet is written
   * to the underlying output stream.
   */
  private void flushBuffer() throws IOException {
    if (orgLen > 0) {
      if (orgLen >= minCompressSize) {
        // compress the data
        deflater.reset();   // sadly we must reset() cause of finish() :(
        deflater.setInput(orgBuf, 0, orgLen);
        deflater.finish();
        defLen = 0;
        while (!deflater.finished()) {
          int num = deflater.deflate(defBuf, defLen, bufSize - defLen);
          if (num <= 0) {
            // can this really happen? Yes: if minCompressSize is too small!
            if (deflater.needsInput()) {
              throw new IOException("Deflater needs more input! Bytes in buffer: " + orgLen);
            }
          }
          defLen += num;
        }
        if (defLen < orgLen) {
          // deflate
          writeHeader(defLen, true);
          out.write(defBuf, 0, defLen);
          totalCompressed += defLen;
          orgLen = 0;
          return;
        }
      }
      
      // uncompressed packet
      writeHeader(orgLen, false);
      out.write(orgBuf, 0, orgLen);
      totalUncompressed += orgLen;
      orgLen = 0;
    }
  }
}
                                              
