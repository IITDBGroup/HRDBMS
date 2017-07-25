package com.exascale.optimizer.load;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.PartitionMetaData;
import com.exascale.threads.HRDBMSThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/** Load data from a file on the coordinator without using an external table as intermediary */
public class ReadThread extends HRDBMSThread
{
    private final File file;
    protected final Map<Integer, Integer> pos2Length;
    protected final List<String> indexes;
    protected boolean ok = true;
    protected volatile long num = 0;
    protected final Map<String, Integer> cols2Pos;
    protected final List<List<String>> keys;
    protected final List<List<String>> types;
    protected final List<List<Boolean>> orders;
    protected final Map<Integer, String> pos2Col;
    protected final Map<String, String> cols2Types;
    protected final int type;
    protected final LoadOperator loadOperator;
    private final List<String> types2 = new ArrayList<String>();

    public ReadThread(LoadOperator loadOperator, final File file, final Map<Integer, Integer> pos2Length, final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final int type)
    {
        this.loadOperator = loadOperator;
        this.file = file;
        this.pos2Length = pos2Length;
        this.indexes = indexes;
        this.cols2Pos = loadOperator.getCols2Pos();
        this.keys = keys;
        this.types = types;
        this.orders = orders;
        this.pos2Col = loadOperator.getPos2Col();
        this.cols2Types = loadOperator.getCols2Types();
        this.type = type;
        for (final String col : pos2Col.values()) {
            types2.add(cols2Types.get(col));
        }
    }

    public long getNum()
    {
        return num;
    }

    public boolean getOK()
    {
        return ok;
    }

    @Override
    public void run()
    {
        FlushMasterThread master = null;
        try
        {
            final BufferedReader in = new BufferedReader(new FileReader(file), 64 * 1024);
            Object o = next(in);  // Read a row from the file.
            final PartitionMetaData pmeta = new PartitionMetaData(loadOperator.getSchema(), loadOperator.getTable(), loadOperator.getTransaction());
            final int numNodes = MetaData.numWorkerNodes;
            while (!(o instanceof DataEndMarker))
            {
                final List<Object> row = (List<Object>)o;
                num++;
                // Check the length of character columns against the schema
                for (final Map.Entry entry : pos2Length.entrySet())
                {
                    if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
                    {
                        ok = false;
                        return;
                    }
                }
                final List<Integer> nodes = MetaData.determineNode(loadOperator.getSchema(), loadOperator.getTable(), row, loadOperator.getTransaction(), pmeta, cols2Pos, numNodes);
                final int device = MetaData.determineDevice(row, pmeta, cols2Pos);

                loadOperator.getLock().readLock().lock();
                for (final Integer node : nodes)
                {
                    loadOperator.getPlan().addNode(node);
                    final long key = (((long)node) << 32) + device;
                    // Assign this row to be sent to a particular device on a particular node.
                    loadOperator.getMap().multiPut(key, row);
                }
                loadOperator.getLock().readLock().unlock();
                // Periodically flush pending inserts from the queue (loadOperator.getMap)
                if (loadOperator.getMap().totalSize() > LoadOperator.MAX_BATCH)
                {
                    if (master != null)
                    {
                        master.join();
                        if (!master.getOK())
                        {
                            throw new Exception("Error flushing inserts");
                        }
                    }

                    master = FlushMasterThread.flush(loadOperator, indexes, keys, types, orders, type);
                }

                o = next(in);
            }

            if (master != null)
            {
                master.join();
                if (!master.getOK())
                {
                    throw new Exception("Error flushing inserts");
                }
            }

            if (loadOperator.getMap().totalSize() > 0)
            {
                master = FlushMasterThread.flush(loadOperator, indexes, keys, types, orders, true, type);
                master.join();
                if (!master.getOK())
                {
                    throw new Exception("Error flushing inserts");
                }
            }

            int count;
            loadOperator.getLock().readLock().lock();
            count = loadOperator.getWaitThreads().size();
            loadOperator.getLock().readLock().unlock();

            // Wait for the master flush thread to empty out into the child flush threads.
            while (count > 0 && loadOperator.getMap().totalSize() == 0)
            {
                master = FlushMasterThread.flush(loadOperator, indexes, keys, types, orders, true, type);
                master.join();
                if (!master.getOK())
                {
                    throw new Exception("Error flushing inserts");
                }

                loadOperator.getLock().readLock().lock();
                count = loadOperator.getWaitThreads().size();
                loadOperator.getLock().readLock().unlock();
            }

            // Wait for any child flush threads to finish their work.
            synchronized (loadOperator.getFlushThreads())
            {
                for (final FlushThread thread : loadOperator.getFlushThreads())
                {
                    if (thread.started())
                    {
                        thread.join();
                        if (!thread.getOK())
                        {
                            throw new Exception("Error flushing inserts");
                        }
                    }
                }
            }
        }
        catch (final Exception e)
        {
            ok = false;
            HRDBMSWorker.logger.debug("", e);
        }
    }

    private Object next(final BufferedReader in) throws Exception
    {
        final String line = in.readLine();
        if (line == null)
        {
            return new DataEndMarker();
        }

        final List<Object> row = new ArrayList<Object>();
        final FastStringTokenizer tokens = new FastStringTokenizer(line, loadOperator.getDelimiter(), false);
        int i = 0;
        while (tokens.hasMoreTokens())
        {
            final String token = tokens.nextToken();
            final String type = types2.get(i);
            i++;

            if (type.equals("CHAR"))
            {
                row.add(token);
            }
            else if (type.equals("INT"))
            {
                row.add(Integer.parseInt(token));
            }
            else if (type.equals("LONG"))
            {
                row.add(Long.parseLong(token));
            }
            else if (type.equals("FLOAT"))
            {
                row.add(Utils.parseDouble(token));
            }
            else if (type.equals("DATE"))
            {
                row.add(new MyDate(Integer.parseInt(token.substring(0, 4)), Integer.parseInt(token.substring(5, 7)), Integer.parseInt(token.substring(8, 10))));
            }
        }

        return row;
    }
}
