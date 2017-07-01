package com.exascale.optimizer.load;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.PartitionMetaData;
import com.exascale.optimizer.externalTable.ExternalTableScanOperator;
import com.exascale.optimizer.externalTable.ExternalTableType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ExternalReadThread extends ReadThread {
    private ExternalTableScanOperator op;

    public ExternalReadThread(final LoadOperator loadOperator, final HashMap<Integer, Integer> pos2Length, final ArrayList<String> indexes, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final int type) {
        super(loadOperator, null, pos2Length, indexes, spmd, keys, types, orders, type);
    }

    @Override
    public void run()
    {
        FlushMasterThread master = null;
        try
        {
            ExternalTableType extTable = loadOperator.getMeta().getExternalTable(loadOperator.getSchema(), loadOperator.getExternalTable(), loadOperator.getTransaction());
            op = new ExternalTableScanOperator(extTable, loadOperator.getSchema(), loadOperator.getExternalTable(), loadOperator.getMeta(), loadOperator.getTransaction());
            op.start();
            Object o = op.next(loadOperator);
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

                o = op.next(loadOperator);
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
            op.close();
        }
        catch (final Exception e)
        {
            ok = false;
            HRDBMSWorker.logger.debug("", e);
        }
    }
}