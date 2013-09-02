import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;


public class BigDecimalWritable implements Writable
{
	private byte[] bigint;
	private int scale;
	
	public BigDecimalWritable()
	{
		
	}
	
	public BigDecimalWritable(BigDecimal val)
	{
		scale = val.scale();
		bigint = val.unscaledValue().toByteArray();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException 
	{
		BytesWritable temp = new BytesWritable();
		temp.readFields(arg0);
		bigint = temp.getBytes();
		VIntWritable temp2 = new VIntWritable();
		temp2.readFields(arg0);
		scale = temp2.get();
	}

	@Override
	public void write(DataOutput arg0) throws IOException 
	{
		(new BytesWritable(bigint)).write(arg0);
		(new VIntWritable(scale)).write(arg0);
	}
	
	public BigDecimal get()
	{
		return new BigDecimal(new BigInteger(bigint), scale);
	}

}
