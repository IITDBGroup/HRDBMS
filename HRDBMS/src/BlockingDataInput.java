import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;


public class BlockingDataInput implements DataInput 
{
	private InputStream in;
	
	public BlockingDataInput(InputStream in)
	{
		this.in = in;
	}
	
	public byte readByte() throws IOException
	{
		return (byte)in.read();
	}

	@Override
	public boolean readBoolean() throws IOException {
		int ch = in.read();
		        if (ch < 0)
		            throw new EOFException();
		        return (ch != 0);
	}

	@Override
	public char readChar() throws IOException {
		throw new IOException("readChar not implemented");
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
		
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		while (len > 0)
		{
			int temp = in.read(b, off, len);
			off += temp;
			len -= temp;
		}
	}

	@Override
	public int readInt() throws IOException {
		int ch1 = in.read();
		        int ch2 = in.read();
		        int ch3 = in.read();
		        int ch4 = in.read();
		        if ((ch1 | ch2 | ch3 | ch4) < 0)
		            throw new EOFException();
		        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	@Override
	public String readLine() throws IOException {
		throw new IOException("readLine not implemented");
	}

	@Override
	public long readLong() throws IOException {
		byte[] readBuffer = new byte[8];
		readFully(readBuffer, 0, 8);
		        return (((long)readBuffer[0] << 56) +
		                ((long)(readBuffer[1] & 255) << 48) +
		                ((long)(readBuffer[2] & 255) << 40) +
		                ((long)(readBuffer[3] & 255) << 32) +
		                ((long)(readBuffer[4] & 255) << 24) +
		                ((readBuffer[5] & 255) << 16) +
		                ((readBuffer[6] & 255) <<  8) +
		                ((readBuffer[7] & 255) <<  0));
	}

	@Override
	public short readShort() throws IOException {
		throw new IOException("readShort not implemented");
	}

	@Override
	public String readUTF() throws IOException {
		throw new IOException("readUTF not implemented");
	}

	@Override
	public int readUnsignedByte() throws IOException {
		throw new IOException("readUnsignedByte not implemented");
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new IOException("readUnsignedShort not implemented");
	}

	@Override
	public int skipBytes(int n) throws IOException {
		throw new IOException("skipBytes not implemented");
	}
}
