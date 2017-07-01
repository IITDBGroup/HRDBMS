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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class ReadThread extends HRDBMSThread
{
    private final File file;
    protected final HashMap<Integer, Integer> pos2Length;
    protected final ArrayList<String> indexes;
    protected boolean ok = true;
    protected volatile long num = 0;
    protected final HashMap<String, Integer> cols2Pos;
    PartitionMetaData spmd;
    protected final ArrayList<ArrayList<String>> keys;
    protected final ArrayList<ArrayList<String>> types;
    protected final ArrayList<ArrayList<Boolean>> orders;
    protected final TreeMap<Integer, String> pos2Col;
    protected final HashMap<String, String> cols2Types;
    protected final int type;
    protected final LoadOperator loadOperator;
    private final ArrayList<String> types2 = new ArrayList<String>();

    public ReadThread(LoadOperator loadOperator, final File file, final HashMap<Integer, Integer> pos2Length, final ArrayList<String> indexes, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final int type)
    {
        this.loadOperator = loadOperator;
        this.file = file;
        this.pos2Length = pos2Length;
        this.indexes = indexes;
        this.cols2Pos = loadOperator.getCols2Pos();
        this.spmd = spmd;
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
            Object o = next(in);
            final PartitionMetaData pmeta = new PartitionMetaData(loadOperator.getSchema(), loadOperator.getTable(), loadOperator.getTransaction());
            final int numNodes = MetaData.numWorkerNodes;
            while (!(o instanceof DataEndMarker))
            {
                final ArrayList<Object> row = (ArrayList<Object>)o;
                num++;
                for (final Map.Entry entry : pos2Length.entrySet())
                {
                    if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
                    {
                        ok = false;
                        return;
                    }
                }
                final ArrayList<Integer> nodes = MetaData.determineNode(loadOperator.getSchema(), loadOperator.getTable(), row, loadOperator.getTransaction(), pmeta, cols2Pos, numNodes);
                final int device = MetaData.determineDevice(row, pmeta, cols2Pos);

                loadOperator.getLock().readLock().lock();
                for (final Integer node : nodes)
                {
                    loadOperator.getPlan().addNode(node);
                    final long key = (((long)node) << 32) + device;
                    loadOperator.getMap().multiPut(key, row);
                }
                loadOperator.getLock().readLock().unlock();
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

                    master = FlushMasterThread.flush(loadOperator, indexes, spmd, keys, types, orders, type);
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
                master = FlushMasterThread.flush(loadOperator, indexes, spmd, keys, types, orders, true, type);
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

            while (count > 0 && loadOperator.getMap().totalSize() == 0)
            {
                master = FlushMasterThread.flush(loadOperator, indexes, spmd, keys, types, orders, true, type);
                master.join();
                if (!master.getOK())
                {
                    throw new Exception("Error flushing inserts");
                }

                loadOperator.getLock().readLock().lock();
                count = loadOperator.getWaitThreads().size();
                loadOperator.getLock().readLock().unlock();
            }

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

        final ArrayList<Object> row = new ArrayList<Object>();
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
