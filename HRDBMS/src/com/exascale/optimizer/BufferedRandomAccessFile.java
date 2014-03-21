package com.exascale.optimizer;

/*
 * $RCSfile: BEBufferedRandomAccessFile.java,v $
 * $Revision: 1.1 $
 * $Date: 2005/02/11 05:02:15 $
 * $State: Exp $
 *
 * Interface:           RandomAccessIO.java
 *
 * Description:         Class for random access I/O (big-endian ordering).
 *
 *
 *
 * COPYRIGHT:
 *
 * This software module was originally developed by Raphaël Grosbois and
 * Diego Santa Cruz (Swiss Federal Institute of Technology-EPFL); Joel
 * Askelöf (Ericsson Radio Systems AB); and Bertrand Berthelot, David
 * Bouchard, Félix Henry, Gerard Mozelle and Patrice Onno (Canon Research
 * Centre France S.A) in the course of development of the JPEG2000
 * standard as specified by ISO/IEC 15444 (JPEG 2000 Standard). This
 * software module is an implementation of a part of the JPEG 2000
 * Standard. Swiss Federal Institute of Technology-EPFL, Ericsson Radio
 * Systems AB and Canon Research Centre France S.A (collectively JJ2000
 * Partners) agree not to assert against ISO/IEC and users of the JPEG
 * 2000 Standard (Users) any of their rights under the copyright, not
 * including other intellectual property rights, for this software module
 * with respect to the usage by ISO/IEC and Users of this software module
 * or modifications thereof for use in hardware or software products
 * claiming conformance to the JPEG 2000 Standard. Those intending to use
 * this software module in hardware or software products are advised that
 * their use may infringe existing patents. The original developers of
 * this software module, JJ2000 Partners and ISO/IEC assume no liability
 * for use of this software module or modifications thereof. No license
 * or right to this software module is granted for non JPEG 2000 Standard
 * conforming products. JJ2000 Partners have full right to use this
 * software module for his/her own purpose, assign or donate this
 * software module to any third party and to inhibit third parties from
 * using this software module for non JPEG 2000 Standard conforming
 * products. This copyright notice must be included in all copies or
 * derivative works of this software module.
 *
 * Copyright (c) 1999/2000 JJ2000 Partners.
 */

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * This class defines a Buffered Random Access File, where all I/O is considered
 * to be big-endian, and extends the <tt>BufferedRandomAccessFile</tt> class.
 * 
 * @see RandomAccessIO
 * @see BinaryDataOutput
 * @see BinaryDataInput
 * @see BufferedRandomAccessFile
 */
public final class BufferedRandomAccessFile
{
	private final String fileName;
	private boolean isReadOnly = true;
	protected RandomAccessFile theFile;
	private volatile byte[] byteBuffer;
	private volatile boolean byteBufferChanged;
	private volatile long offset;
	private volatile int pos;
	private volatile int maxByte;
	private volatile boolean isEOFInBuffer;
	private static final int BIG_ENDIAN = 0;
	private volatile long length;
	private boolean skipLF = false;

	/**
	 * Constructor. Uses the default value for the byte-buffer size (512 bytes).
	 * 
	 * @param file
	 *            The file associated with the buffer
	 * 
	 * @param mode
	 *            "r" for read, "rw" or "rw+" for read and write mode ("rw+"
	 *            opens the file for update whereas "rw" removes it before. So
	 *            the 2 modes are different only if the file already exists).
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public BufferedRandomAccessFile(File file, String mode) throws IOException
	{
		this(file, mode, 512);
	}

	/**
	 * Constructor. Always needs a size for the buffer.
	 * 
	 * @param file
	 *            The file associated with the buffer
	 * 
	 * @param mode
	 *            "r" for read, "rw" or "rw+" for read and write mode ("rw+"
	 *            opens the file for update whereas "rw" removes it before. So
	 *            the 2 modes are different only if the file already exists).
	 * 
	 * @param bufferSize
	 *            The number of bytes to buffer
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public BufferedRandomAccessFile(File file, String mode, int bufferSize) throws IOException
	{
		fileName = file.getName();
		if (mode.equals("rw") || mode.equals("rw+"))
		{ // mode read / write
			isReadOnly = false;
			if (mode.equals("rw"))
			{ // mode read / (over)write
				if (file.exists())
				{
					file.delete();
				}
			}
			mode = "rw";
		}
		theFile = new RandomAccessFile(file, mode);
		length = theFile.length();
		byteBuffer = new byte[bufferSize];
		readNewBuffer(0);
	}

	/**
	 * Constructor. Uses the default value for the byte-buffer size (512 bytes).
	 * 
	 * @param name
	 *            The name of the file associated with the buffer
	 * 
	 * @param mode
	 *            "r" for read, "rw" or "rw+" for read and write mode ("rw+"
	 *            opens the file for update whereas "rw" removes it before. So
	 *            the 2 modes are different only if the file already exists).
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public BufferedRandomAccessFile(String name, String mode) throws IOException
	{
		this(name, mode, 512);
	}

	/**
	 * Constructor. Always needs a size for the buffer.
	 * 
	 * @param name
	 *            The name of the file associated with the buffer
	 * 
	 * @param mode
	 *            "r" for read, "rw" or "rw+" for read and write mode ("rw+"
	 *            opens the file for update whereas "rw" removes it before. So
	 *            the 2 modes are different only if the file already exists).
	 * 
	 * @param bufferSize
	 *            The number of bytes to buffer
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public BufferedRandomAccessFile(String name, String mode, int bufferSize) throws IOException
	{
		this(new File(name), mode, bufferSize);
	}

	public void close() throws IOException
	{
		/*
		 * If the buffer has been changed, it need to be saved before closing
		 */
		flush();
		byteBuffer = null; // Release the byte-buffer reference
		theFile.close();
	}

	public final void flush() throws IOException
	{
		if (byteBufferChanged)
		{
			theFile.seek(offset);
			theFile.write(byteBuffer, 0, maxByte);
			byteBufferChanged = false;
		}
	}

	public final int getBufferSize()
	{
		return byteBuffer.length;
	}

	public long getFilePointer()
	{
		return offset + pos;
	}

	public long length() throws IOException
	{
		return length;
	}

	public long position()
	{
		return pos + offset;
	}

	public final int read() throws IOException, EOFException
	{
		if (pos < maxByte)
		{ // The byte can be read from the buffer
			// In Java, the bytes are always signed.
			return (byteBuffer[pos++] & 0xFF);
		}
		else if (isEOFInBuffer)
		{ // EOF is reached
			pos = maxByte + 1; // Set position to EOF
			throw new EOFException();
		}
		else
		{ // End of the buffer is reached
			readNewBuffer(offset + pos);
			return read();
		}
	}

	/**
	 * Reads an IEEE double precision (i.e., 64 bit) floating-point number from
	 * the input. Prior to reading, the input should be realigned at the byte
	 * level.
	 * 
	 * @return The next byte-aligned IEEE double (64 bit) from the input.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final double readDouble() throws IOException, EOFException
	{
		return Double.longBitsToDouble(((long)read() << 56) | ((long)read() << 48) | ((long)read() << 40) | ((long)read() << 32) | ((long)read() << 24) | ((long)read() << 16) | ((long)read() << 8) | (read()));
	}

	/**
	 * Reads an IEEE single precision (i.e., 32 bit) floating-point number from
	 * the input. Prior to reading, the input should be realigned at the byte
	 * level.
	 * 
	 * @return The next byte-aligned IEEE float (32 bit) from the input.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final float readFloat() throws EOFException, IOException
	{
		return Float.intBitsToFloat((read() << 24) | (read() << 16) | (read() << 8) | (read()));
	}

	/**
	 * Reads a signed int (i.e., 32 bit) from the input. Prior to reading, the
	 * input should be realigned at the byte level.
	 * 
	 * @return The next byte-aligned signed int (32 bit) from the input.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final int readInt() throws IOException, EOFException
	{
		return ((read() << 24) | (read() << 16) | (read() << 8) | read());
	}

	public String readLine() throws IOException
	{
		int c = 0;
		try
		{
			c = read();
		}
		catch (final EOFException e)
		{
			c = (0xFF & '\n');
		}
		if (skipLF && c == (0xFF & '\n'))
		{
			try
			{
				c = read();
			}
			catch (final EOFException e)
			{
				c = (0xFF & '\n');
			}
			skipLF = false;
		}

		final StringBuilder s = new StringBuilder();
		while (c != (0xFF & '\n') && c != (0xFF & '\r'))
		{
			s.append((char)c);
			try
			{
				c = read();
			}
			catch (final EOFException e)
			{
				c = (0xFF & '\n');
			}
		}

		if (c == (0xFF & '\r'))
		{
			skipLF = true;
		}

		if (s.length() > 0)
		{
			return s.toString();
		}
		else
		{
			return null;
		}
	}

	/**
	 * Reads a signed long (i.e., 64 bit) from the input. Prior to reading, the
	 * input should be realigned at the byte level.
	 * 
	 * @return The next byte-aligned signed long (64 bit) from the input.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final long readLong() throws IOException, EOFException
	{
		return (((long)read() << 56) | ((long)read() << 48) | ((long)read() << 40) | ((long)read() << 32) | ((long)read() << 24) | ((long)read() << 16) | ((long)read() << 8) | (read()));
	}

	/**
	 * Reads a signed short (i.e., 16 bit) from the input. Prior to reading, the
	 * input should be realigned at the byte level.
	 * 
	 * @return The next byte-aligned signed short (16 bit) from the input.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final short readShort() throws IOException, EOFException
	{
		return (short)((read() << 8) | (read()));
	}

	/**
	 * Reads an unsigned int (i.e., 32 bit) from the input. It is returned as a
	 * <tt>long</tt> since Java does not have an unsigned short type. Prior to
	 * reading, the input should be realigned at the byte level.
	 * 
	 * @return The next byte-aligned unsigned int (32 bit) from the input, as a
	 *         <tt>long</tt>.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final long readUnsignedInt() throws IOException, EOFException
	{
		return (read() << 24) | (read() << 16) | (read() << 8) | read();
	}

	/**
	 * Reads an unsigned short (i.e., 16 bit) from the input. It is returned as
	 * an <tt>int</tt> since Java does not have an unsigned short type. Prior to
	 * reading, the input should be realigned at the byte level.
	 * 
	 * @return The next byte-aligned unsigned short (16 bit) from the input, as
	 *         an <tt>int</tt>.
	 * 
	 * @exception java.io.EOFException
	 *                If the end-of file was reached before getting all the
	 *                necessary data.
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final int readUnsignedShort() throws IOException, EOFException
	{
		return ((read() << 8) | read());
	}

	public void seek(long off) throws IOException
	{
		/*
		 * If the new offset is within the buffer, only the pos value needs to
		 * be modified. Else, the buffer must be moved.
		 */
		skipLF = false;
		if ((off >= offset) && (off < (offset + byteBuffer.length)))
		{
			if (isReadOnly && isEOFInBuffer && off > offset + maxByte)
			{
				// We are seeking beyond EOF in read-only mode!
				throw new EOFException();
			}
			pos = (int)(off - offset);
		}
		else
		{
			readNewBuffer(off);
		}
	}

	public boolean skipLine() throws IOException
	{
		int c = 0;
		try
		{
			c = read();
		}
		catch (final EOFException e)
		{
			return false;
		}
		if (skipLF && c == (0xFF & '\n'))
		{
			try
			{
				c = read();
			}
			catch (final EOFException e)
			{
				return false;
			}
			skipLF = false;
		}

		while (c != (0xFF & '\n') && c != (0xFF & '\r'))
		{
			try
			{
				c = read();
			}
			catch (final EOFException e)
			{
				return false;
			}
		}

		if (c == (0xFF & '\r'))
		{
			skipLF = true;
		}

		return true;
	}

	/**
	 * Returns a string of information about the file and the endianess
	 */
	@Override
	public String toString()
	{
		return super.toString() + "\nBig-Endian ordering";
	}

	public final void write(byte b) throws IOException
	{
		// As long as pos is less than the length of the buffer we can write
		// to the buffer. If the position is after the buffer a new buffer is
		// needed
		if (pos < byteBuffer.length)
		{
			if (isReadOnly)
			{
				throw new IOException("File is read only");
			}
			byteBuffer[pos] = b;
			if (pos >= maxByte)
			{
				maxByte = pos + 1;
			}
			pos++;
			byteBufferChanged = true;
		}
		else
		{
			readNewBuffer(offset + pos);
			write(b);
		}
	}

	public void write(byte[] bs) throws IOException
	{
		for (final byte b : bs)
		{
			write(b);
		}
	}

	public final void write(int b) throws IOException
	{
		// As long as pos is less than the length of the buffer we can write
		// to the buffer. If the position is after the buffer a new buffer is
		// needed
		if (pos < byteBuffer.length)
		{
			if (isReadOnly)
			{
				throw new IOException("File is read only");
			}
			byteBuffer[pos] = (byte)b;
			if (pos >= maxByte)
			{
				maxByte = pos + 1;
				length = offset + maxByte;
			}
			pos++;
			byteBufferChanged = true;
		}
		else
		{
			readNewBuffer(offset + pos);
			write(b);
		}
	}

	/**
	 * Writes the IEEE double value <tt>v</tt> (i.e., 64 bits) to the output.
	 * Prior to writing, the output should be realigned at the byte level.
	 * 
	 * @param v
	 *            The value to write to the output
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final void writeDouble(double v) throws IOException
	{
		final long longV = Double.doubleToLongBits(v);

		write((int)(longV >>> 56));
		write((int)(longV >>> 48));
		write((int)(longV >>> 40));
		write((int)(longV >>> 32));
		write((int)(longV >>> 24));
		write((int)(longV >>> 16));
		write((int)(longV >>> 8));
		write((int)(longV));
	}

	/**
	 * Writes the IEEE float value <tt>v</tt> (i.e., 32 bits) to the output.
	 * Prior to writing, the output should be realigned at the byte level.
	 * 
	 * @param v
	 *            The value to write to the output
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final void writeFloat(float v) throws IOException
	{
		final int intV = Float.floatToIntBits(v);

		write(intV >>> 24);
		write(intV >>> 16);
		write(intV >>> 8);
		write(intV);
	}

	/**
	 * Writes the int value of <tt>v</tt> (i.e., the 32 bits) to the output.
	 * Prior to writing, the output should be realigned at the byte level.
	 * 
	 * @param v
	 *            The value to write to the output
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final void writeInt(int v) throws IOException
	{
		write(v >>> 24);
		write(v >>> 16);
		write(v >>> 8);
		write(v);
	}

	/**
	 * Writes the long value of <tt>v</tt> (i.e., the 64 bits) to the output.
	 * Prior to writing, the output should be realigned at the byte level.
	 * 
	 * @param v
	 *            The value to write to the output
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final void writeLong(long v) throws IOException
	{
		write((int)(v >>> 56));
		write((int)(v >>> 48));
		write((int)(v >>> 40));
		write((int)(v >>> 32));
		write((int)(v >>> 24));
		write((int)(v >>> 16));
		write((int)(v >>> 8));
		write((int)v);
	}

	/**
	 * Writes the short value of <tt>v</tt> (i.e., 16 least significant bits) to
	 * the output. Prior to writing, the output should be realigned at the byte
	 * level.
	 * 
	 * <P>
	 * Signed or unsigned data can be written. To write a signed value just pass
	 * the <tt>short</tt> value as an argument. To write unsigned data pass the
	 * <tt>int</tt> value as an argument (it will be automatically casted, and
	 * only the 16 least significant bits will be written).
	 * 
	 * @param v
	 *            The value to write to the output
	 * 
	 * @exception java.io.IOException
	 *                If an I/O error ocurred.
	 * */
	public final void writeShort(int v) throws IOException
	{
		write(v >>> 8);
		write(v);
	}

	private final void readNewBuffer(long off) throws IOException
	{
		/*
		 * If the buffer have changed. We need to write it to the file before
		 * reading a new buffer.
		 */
		if (byteBufferChanged)
		{
			flush();
		}
		// Don't allow to seek beyond end of file if reading only
		if (isReadOnly && off >= theFile.length())
		{
			throw new EOFException();
		}
		// Set new offset
		offset = off;

		theFile.seek(offset);

		maxByte = theFile.read(byteBuffer, 0, byteBuffer.length);
		pos = 0;

		if (maxByte < byteBuffer.length)
		{ // Not enough data in input file.
			isEOFInBuffer = true;
			if (maxByte == -1)
			{
				maxByte++;
			}
		}
		else
		{
			isEOFInBuffer = false;
		}
	}
}
