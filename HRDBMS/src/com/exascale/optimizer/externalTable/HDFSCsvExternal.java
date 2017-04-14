package com.exascale.optimizer.externalTable;

import com.exascale.misc.HrdbmsType;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.OperatorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetUtils;

import java.io.*;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

public class HDFSCsvExternal extends HTTPCsvExternal
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}

	private DFSClient dfsClient;
	private List<LocatedBlock> blocks;
	private String previousLine = "";
    private Path path;
    private Configuration conf;
    private Iterator blockIterator;

    /** Parameters defined in SYS.EXTERNALTABLES */
    protected CsvExternalParams params;

    public Class getParamsClass() throws NoSuchFieldException
    {
        return this.getClass().getDeclaredField("params").getType();
    }

    @Override
    public void setParams(ExternalParamsInterface params) {
        this.params = (CsvExternalParams) params;
        if (!params.valid()) {
            throw new ExternalTableException("Parameters are not valid");
        }
    }


    @Override
	public void start()
	{
		try {
			conf = new Configuration();
			path = new Path(params.getLocation());
			DistributedFileSystem fs = (DistributedFileSystem) path.getFileSystem(conf);
			HdfsDataInputStream inputStream = (HdfsDataInputStream) fs.open(path);
			blocks = inputStream.getAllBlocks();
			dfsClient = fs.getClient();
            blockIterator = blocks.iterator();
            LocatedBlock block = (LocatedBlock) blockIterator.next();
            readBlock(block);
            skipHeader();
        } catch (Exception e) {
			throw new ExternalTableException("Unable to download CSV file " + params.getLocation());
		}

	}

    /** Skip header of CSV file if metadata parameters define to do so */
    protected void skipHeader() {
        try {
            if (params.getIgnoreHeader()) {
                // skip header;
                input.readLine();
                line++;
            }
        } catch (Exception e) {
            throw new ExternalTableException("Unable to read header in CSV file " + params.getLocation());
        }
    }

    @Override
    public ArrayList next() {
        String inputLine;
        try {
            line++;
            inputLine = input.readLine();

            // we can put the logic as follows:
            // output previous line only
            // if we read a new block, we check if previous line is full line
            //     if previous line is fully completed csv line, we output the line
            //     if it is not, we output previous line + current
            if (inputLine == null && blockIterator.hasNext()) {
                LocatedBlock block = (LocatedBlock) blockIterator.next();
                readBlock(block);
                inputLine = input.readLine();
            }

                /** This is the parser splitting the data that it found into columns of a map. */
            if (inputLine != null)
            {
                // temporal solution - skipping lines split across two blocks
                ArrayList<String> row = new ArrayList<>(Arrays.asList(inputLine.split(params.getDelimiter())));
                if (row.size() != pos2Col.size()) {
                    inputLine = input.readLine();
                    inputLine = input.readLine();
                }
                return convertCsvLineToObject(inputLine);
            } else {
                return null;
            }

        } catch (Exception e) {
            throw new ExternalTableException("Unable to read line "+ line +" in CSV file " + params.getLocation());
        }
    }


    /** Convert csv line into table row.
     *  Runtime exception is thrown when type of CSV column does not match type of table column	 */
    protected ArrayList<Object> convertCsvLineToObject(final String inputLine)
    {
        final ArrayList<Object> retval = new ArrayList<Object>();
        ArrayList<String> row = new ArrayList<>(Arrays.asList(inputLine.split(params.getDelimiter())));
        if (row.size() != pos2Col.size()) {
            throw new ExternalTableException(
                    "Line: " + line
                            + ".\nSize of external table does not match column count in CSV file '" + params.getLocation() + "'."
                            + "\nColumns in csv file: " + row.size()
                            + "\nColumns defined in external table schema: " + pos2Col.size()
            );
        }

        int column = 0;
        for (final Map.Entry<Integer, String> entry : pos2Col.entrySet()) {
            String type = cols2Types.get(entry.getValue());
            try {
                if (type.equals("INT")) {
                    retval.add(Integer.parseInt(row.get(column)));
                } else if (type.equals("LONG")) {
                    retval.add(Long.parseLong(row.get(column)));
                } else if (type.equals("FLOAT")) {
                    retval.add(Double.parseDouble(row.get(column)));
                } else if (type.equals("VARCHAR")) {
                    // TODO We need to cut value if it exceeds the limit defined in the table schema
                    retval.add(row.get(column));
                } else if (type.equals("CHAR")) {
                    // TODO We need to cut value if it exceeds the limit defined in the table schema
                    retval.add(row.get(column));
                } else if (type.equals("DATE")) {
                    final int year = Integer.parseInt(row.get(column).substring(0, 4));
                    final int month = Integer.parseInt(row.get(column).substring(5, 7));
                    final int day = Integer.parseInt(row.get(column).substring(8, 10));
                    retval.add(new MyDate(year, month, day));
                }
            } catch (Exception e) {
                throw new ExternalTableException(
                        "Line: " + line + ", column: " + column + "\n"
                                + "Error conversion '" + row.get(column) + "' to type '" + type + "'."
                );
            }
            column++;
        }

        return retval;
    }

    @Override
	public boolean hasNext() {
		throw new UnsupportedOperationException("hasNext method is not supported in HDFSCsvExternal in this stage");
	}

	@Override
	public void reset()
	{
		throw new UnsupportedOperationException("Reset method is not supported in HTTPCsvExternal in this stage");
	}

    public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception {
        final Long id = prev.get(this);
        if (id != null) {
            OperatorUtils.serializeReference(id, out);
            return;
        }

        OperatorUtils.writeType(HrdbmsType.HDFSCSVEXTERNALTABLE, out);
        prev.put(this, OperatorUtils.writeID(out));
        OperatorUtils.serializeCSVExternalParams(params, out, prev);
    }


	public static HDFSCsvExternal deserializeKnown(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final HDFSCsvExternal value = (HDFSCsvExternal)unsafe.allocateInstance(HDFSCsvExternal.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.params = OperatorUtils.deserializeCSVExternalParams(in, prev);
		return value;
	}

	private void readBlock(LocatedBlock block)
	{
        try {
            // it should be a better way to choose node
            DatanodeInfo chosenNode = block.getLocations()[0];
            String var9 = chosenNode.getXferAddr(false);
            InetSocketAddress targetAddr = NetUtils.createSocketAddr(var9);
            BlockReader blockReader = (new BlockReaderFactory(dfsClient.getConf())).setInetSocketAddress(targetAddr).setRemotePeerFactory(dfsClient).setDatanodeInfo(chosenNode).setStorageType(block.getStorageTypes()[0]).setFileName(/* this.src */ path.toUri().getPath()).setBlock(block.getBlock()).setBlockToken(block.getBlockToken()).setStartOffset(0).setVerifyChecksum(true).setClientName(dfsClient.getClientName()).setLength(block.getBlock().getNumBytes()).setCachingStrategy(dfsClient.getDefaultReadCachingStrategy()).setAllowShortCircuitLocalReads(true).setClientCacheContext(dfsClient.getClientContext())/*.setUserGroupInformation(dfsClient.ugi)*/.setConfiguration(conf).build();
            // TODO Size of Byte buffer is equal to the size of a block. We may need to fix the code as it is not an optimal solution.
            byte[] buf = new byte[(int) block.getBlockSize()];
            ByteBuffer bb = ByteBuffer.wrap(buf);
            input = wrapByteBuffer(bb);
            try {
                int cnt;
                long bytesRead = 0;
                while ((cnt = blockReader.read(bb)) > 0) {
                    bytesRead += cnt;
                }
                if (bytesRead != block.getBlock().getNumBytes()) {
                    throw new IOException("Recorded block size is " + block.getBlock().getNumBytes() +
                            ", but datanode returned " + bytesRead + " bytes");
                }
            } finally {
                try {blockReader.close(); } catch (Exception e1) {}
            }
		} catch (Exception e) {
            throw new ExternalTableException("Block reading error: " + e.getMessage());
		}
	}

    private static BufferedReader wrapByteArray(byte[] byteArr) {
        return wrapByteArray(byteArr, 0, byteArr.length);
    }
    private static BufferedReader wrapByteArray(byte[] byteArr, int offset, int length) {
        ByteArrayInputStream stream = new ByteArrayInputStream(byteArr, offset, length);
        InputStreamReader sr = new InputStreamReader(stream);
        return new BufferedReader(sr);
    }
    private static BufferedReader wrapByteBuffer(ByteBuffer byteBuffer) {
        return wrapByteArray(byteBuffer.array());
    }

}