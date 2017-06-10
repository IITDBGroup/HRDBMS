package com.exascale.optimizer.externalTable;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.HrdbmsType;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.OperatorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** External table implementation for reading from an HDFS URL */
public class HDFSCsvExternal extends HTTPCsvExternal implements MultiThreadedExternalTableType
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
    private ConcurrentHashMap<Integer, ProcessingBlock> processingBlocks;
    private Path path;
    private Configuration conf;
    private Long blockId;
    private int currentBlockIndex;
    private int node;
    private int device;
    private int numNodes;
    private int numDevices;
    private int line;

    private class ProcessingBlock
    {
        private boolean firstLine = false;
        private boolean lastLine = false;
        private String previousLine;
        private String nextBlockFistLine;
        private LocatedBlock block;
        private int blockIndex;

        int getBlockIndex() {
            return blockIndex;
        }

        void setBlockIndex(int blockIndex) {
            this.blockIndex = blockIndex;
        }

        LocatedBlock getBlock() {
            return block;
        }

        void setBlock(LocatedBlock block) {
            this.block = block;
        }

        boolean isFirstLine() {
            return firstLine;
        }

        void setFirstLine(boolean firstLine) {
            this.firstLine = firstLine;
        }

        boolean isLastLine() {
            return lastLine;
        }

        void setLastLine(boolean lastLine) {
            this.lastLine = lastLine;
        }

        String getPreviousLine() {
            return previousLine;
        }

        void setPreviousLine(String previousLine) {
            this.previousLine = previousLine;
        }

        String getNextBlockFistLine() {
            return nextBlockFistLine;
        }

        void setNextBlockFistLine(String completedFistLine) {
            this.nextBlockFistLine = completedFistLine;
        }
    }


    /** Parameters defined in SYS.EXTERNALTABLES */
    protected CsvExternalParams params;

    public void setNumNodes(int numNodes)
    {
        this.numNodes = numNodes;
    }

    public void setNumDevices(int numDevices)
    {
        this.numDevices = numDevices;
    }

    public void setDevice(int device)
    {
        this.device = device;
    }

    public void setNode(int node)
    {
        this.node = node;
    }

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
    public HDFSCsvExternal clone()
    {
        HDFSCsvExternal retval;
        try
        {
            retval = new HDFSCsvExternal();
        }
        catch (final Exception e)
        {
            HRDBMSWorker.logger.error("", e);
            return null;
        }
        retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
        retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
        retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
        retval.schema = schema;
        retval.name = name;
        retval.node = node;
        retval.numNodes = numNodes;
        retval.numDevices = numDevices;
        retval.dfsClient = dfsClient;
        retval.blocks = blocks;
        retval.processingBlocks = processingBlocks;
        retval.path = path;
        retval.conf = conf;
        retval.blockId = blockId;
        retval.currentBlockIndex = currentBlockIndex;
        retval.params = (CsvExternalParams) params;

        return retval;
    }

    @Override
	public void start()
	{
		try {
			conf = new Configuration();
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
			path = new Path(params.getLocation());
			DistributedFileSystem fs = (DistributedFileSystem) path.getFileSystem(conf);
			HdfsDataInputStream inputStream = (HdfsDataInputStream) fs.open(path);
			blocks = inputStream.getAllBlocks();
			dfsClient = fs.getClient();
            processingBlocks = initBlocks();
            input = null;
            this.currentBlockIndex = -1;
            this.line = 0;
        } catch (Exception e) {
			throw new ExternalTableException("Unable to download CSV file " + params.getLocation());
		}
	}

    private ConcurrentHashMap<Integer, ProcessingBlock> initBlocks()
    {
        ConcurrentHashMap<Integer, ProcessingBlock> processingBlocks = new ConcurrentHashMap<>();
        ProcessingBlock pb;
        int key = 0;
        for (LocatedBlock block : blocks) {
            pb = new ProcessingBlock();
            pb.setBlock(block);
            pb.setBlockIndex(key);
            processingBlocks.put(key, pb);
            key++;
        }
        return processingBlocks;
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
        String currentLine, nextLine;
        ArrayList row;

        try {
            // reading first block
            if (input == null) {
                currentBlockIndex = readBlock(-1);
            }
            if (currentBlockIndex == -1) {
                return null;
            }
            line++;
            ProcessingBlock processingBlock = this.processingBlocks.get(currentBlockIndex);

            // output the first line of the next block if it is read together with the last line of the previous block
            if (processingBlock.getNextBlockFistLine() != null) {
                row = convertCsvLineToObject(processingBlock.getNextBlockFistLine());
                processingBlock.setNextBlockFistLine(null);
                return row;
            }

            // reading next block
            if (processingBlock.isLastLine()) {
                currentBlockIndex = readBlock(currentBlockIndex);
                if (currentBlockIndex == -1) {
                    return null;
                }
                processingBlock = this.processingBlocks.get(currentBlockIndex);
            }

            currentLine = input.readLine();

            // end of the block
            if (currentLine == null) {
                processingBlock.setLastLine(true);
                if (processingBlock.getPreviousLine() == null) {
                    return null;
                } else {
                    if (this.currentBlockIndex + 1 == blocks.size()) {
                        // last line of the last block
                        return convertCsvLineToObject(processingBlock.getPreviousLine());
                    } else {
                        // the end of the block, but the next block is available
                        nextLine = readNextBlockFistLine(currentBlockIndex);
                        if (isCorrectSize(processingBlock.getPreviousLine() + nextLine)) {
                            row = convertCsvLineToObject(processingBlock.getPreviousLine() + nextLine);
                        } else if (isCorrectSize(processingBlock.getPreviousLine()) && isCorrectSize(nextLine)) {
                            row = convertCsvLineToObject(processingBlock.getPreviousLine());
                            processingBlock.setNextBlockFistLine(nextLine);
                        } else {
                            throw new ExternalTableException(
                                    "Something wrong with reading the end of HDFS block."
                                    + "First part of line: " + processingBlock.getPreviousLine() + "."
                                    + "Second part of line: " + nextLine + "."
                            );
                        }
                        processingBlock.setPreviousLine(null);
                        return row;
                    }
                }
            }

            // fist line of the block
            else if (processingBlock.isFirstLine()) {
                processingBlock.setFirstLine(false);
                if (this.currentBlockIndex != 0) {
                    // skip line that belongs to two consecutive blocks
                    // as it is already read during reading previous block
                    currentLine = input.readLine();
                }
                row = convertCsvLineToObject(currentLine);
                nextLine = input.readLine();
                processingBlock.setPreviousLine(nextLine);
                return row;
            }

            // line inside of HDFS block
            else if (!processingBlock.isFirstLine()) {
                row = convertCsvLineToObject(processingBlock.getPreviousLine());
                processingBlock.setPreviousLine(currentLine);
                return row;
            }

        } catch (Exception e) {
            throw new ExternalTableException(e);
        }
        return null;
    }

    private boolean isCorrectSize(final String inputLine)
    {
        ArrayList<String> row = new ArrayList<>(Arrays.asList(inputLine.split(params.getDelimiter())));
        return (row.size() + 1 == pos2Col.size());
    }

    /** Convert csv line into table row. */
    protected ArrayList<Object> convertCsvLineToObject(final String inputLine)
    {
        final ArrayList<Object> retval = new ArrayList<>();
        ArrayList<String> row = new ArrayList<>(Arrays.asList(inputLine.split(params.getDelimiter())));
        if (row.size() + 1 != pos2Col.size()) {
            throw new ExternalTableException(
                    "Line: " + inputLine
                            + ".\nSize of external table does not match column count in CSV file '" + params.getLocation() + "'."
                            + "\nColumns in csv file: " + row.size()
                            + "\nColumns defined in external table schema: " + pos2Col.size()
            );
        }

        int column = 0;
        for (final Map.Entry<Integer, String> entry : pos2Col.entrySet()) {
            String type = cols2Types.get(entry.getValue());
            try {
                if (entry.getKey() == row.size()) {
                    retval.add(blockId);
                } else if (type.equals("INT")) {
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
                        "Line: " + inputLine + ", column: " + column + "\n"
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
		throw new UnsupportedOperationException("Reset method is not supported in HDFSCsvExternal in this stage");
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

	private int readBlock(int curBlockIndex)
	{
        if (curBlockIndex + 1 >= blocks.size()) {
            return -1;
        }
        curBlockIndex++;
        LocatedBlock block = blocks.get(curBlockIndex);
        if (curBlockIndex % (numNodes * numDevices) != node * numDevices + device) {
            return readBlock(curBlockIndex);
        }

        // TODO Size of Byte buffer is equal to the size of a block. We may need to fix the code
        //      as it can be not an optimal solution.
        byte[] buf = new byte[(int) block.getBlockSize()];
        this.input = readBlock(block, buf);

		if (curBlockIndex == 0) {
            skipHeader();
        }
        ProcessingBlock processingBlock = processingBlocks.get(curBlockIndex);
        if (processingBlock.isFirstLine()) {
            throw new ExternalTableException("First line of block with id " + blockId + " has been already read!");
        }
        processingBlock.setFirstLine(true);
        this.currentBlockIndex = curBlockIndex;
        blockId =  block.getBlock().getBlockId();
		return curBlockIndex;
	}

    private String readNextBlockFistLine(int curBlockIndex) throws  IOException
    {
        if (curBlockIndex + 1 == blocks.size()) {
            return null;
        }
        LocatedBlock block = blocks.get(curBlockIndex + 1);
        // TODO we need to put the length of row instead of hardcoded value
        byte[] buf = new byte[8192];
        input = readBlock(block, buf);
        return input.readLine();
    }

    private BufferedReader readBlock(LocatedBlock block, byte[] buf)
    {
        BufferedReader input;
        try {
            // it should be a better way to choose node
            DatanodeInfo chosenNode = block.getLocations()[0];
            String var9 = chosenNode.getXferAddr(false);
            InetSocketAddress targetAddr = NetUtils.createSocketAddr(var9);
            BlockReader blockReader = (new BlockReaderFactory(dfsClient.getConf())).setInetSocketAddress(targetAddr).setRemotePeerFactory(dfsClient).setDatanodeInfo(chosenNode).setStorageType(block.getStorageTypes()[0]).setFileName(/* this.src */ path.toUri().getPath()).setBlock(block.getBlock()).setBlockToken(block.getBlockToken()).setStartOffset(0).setVerifyChecksum(true).setClientName(dfsClient.getClientName()).setLength(block.getBlock().getNumBytes()).setCachingStrategy(dfsClient.getDefaultReadCachingStrategy()).setAllowShortCircuitLocalReads(true).setClientCacheContext(dfsClient.getClientContext())/*.setUserGroupInformation(dfsClient.ugi)*/.setConfiguration(conf).build();
            ByteBuffer bb = ByteBuffer.wrap(buf);
            HRDBMSWorker.logger.debug(String.format("Thread %d Node %d read block %d of size %d", Thread.currentThread().getId(), node, blockId, block.getBlockSize()));
            input = wrapByteBuffer(bb);
            try {
                int cnt;
                long bytesRead = 0;
                while ((cnt = blockReader.read(bb)) > 0) {
                    bytesRead += cnt;
                }
                if (bytesRead != buf.length) {
                    throw new IOException("Byte array's size is " + buf.length +
                            ", but it has been read " + bytesRead + " bytes");
                }
            } finally {
                try { blockReader.close(); } catch (Exception e1) {}
            }
		} catch (Exception e) {
            throw new ExternalTableException(e);
		}
        return input;
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