import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;


public class MyDataOutput implements DataOutput
{
	private OutputStream out;
	
	public MyDataOutput(OutputStream out)
	{
		this.out = out;
	}

	@Override
	public void write(int arg0) throws IOException {
		out.write(arg0);
	}

	@Override
	public void write(byte[] arg0) throws IOException {
		out.write(arg0);
		
	}

	@Override
	public void write(byte[] arg0, int arg1, int arg2) throws IOException {
		out.write(arg0, arg1, arg2);
	}

	@Override
	public void writeBoolean(boolean arg0) throws IOException {
		out.write(arg0 ? 1 : 0);
		
	}

	@Override
	public void writeByte(int arg0) throws IOException {
		out.write(arg0);
	}

	@Override
	public void writeBytes(String arg0) throws IOException {
		throw new IOException("writeBytes(String) is not implemented");
		
	}

	@Override
	public void writeChar(int arg0) throws IOException {
		throw new IOException("writeChar is not implemented");
		
	}

	@Override
	public void writeChars(String arg0) throws IOException {
		throw new IOException("writeChars is not implemented");
		
	}

	@Override
	public void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
		
	}

	@Override
	public void writeFloat(float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
		
	}

	@Override
	public void writeInt(int v) throws IOException {
		out.write((v >>> 24) & 0xFF);
		        out.write((v >>> 16) & 0xFF);
		       out.write((v >>>  8) & 0xFF);
		        out.write((v >>>  0) & 0xFF);
		
	}

	@Override
	public void writeLong(long v) throws IOException {
		byte[] writeBuffer = new byte[8];
		writeBuffer[0] = (byte)(v >>> 56);
		        writeBuffer[1] = (byte)(v >>> 48);
		        writeBuffer[2] = (byte)(v >>> 40);
		        writeBuffer[3] = (byte)(v >>> 32);
		        writeBuffer[4] = (byte)(v >>> 24);
		        writeBuffer[5] = (byte)(v >>> 16);
		        writeBuffer[6] = (byte)(v >>>  8);
		        writeBuffer[7] = (byte)(v >>>  0);
		        out.write(writeBuffer, 0, 8);
		
	}

	@Override
	public void writeShort(int arg0) throws IOException {
		throw new IOException("writeShort is not implemented");
		
	}

	@Override
	public void writeUTF(String arg0) throws IOException {
		throw new IOException("writeUTF is not implemented");
		
	}

}
