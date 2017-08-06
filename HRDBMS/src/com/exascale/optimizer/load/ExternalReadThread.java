package com.exascale.optimizer.load;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.PartitionMetaData;
import com.exascale.optimizer.externalTable.ExternalTableScanOperator;

import java.util.List;
import java.util.Map;

/** Read data from an external table on the workers */
public class ExternalReadThread extends ReadThread {
    private ExternalTableScanOperator op;
    private PartitionMetaData pmeta;

    public ExternalReadThread(final LoadOperator loadOperator, final Map<Integer, Integer> pos2Length, final List<String> indexes, final List<List<String>> keys,
                              final List<List<String>> types, final List<List<Boolean>> orders, final int type, final PartitionMetaData pmeta) {
        super(loadOperator, null, pos2Length, indexes, keys, types, orders, type);
        op = loadOperator.getChild();
        this.pmeta = pmeta;
    }

    @Override
    public void run()
    {
        FlushMasterThread master = null;
        try
        {
            Object o = op.next(loadOperator);
            final int numNodes = MetaData.numWorkerNodes;
            while (!(o instanceof DataEndMarker))
            {
                final List<Object> row = (List<Object>)o;
                num++;
                // Skip loading string values that are longer than the char column length.
                for (final Map.Entry entry : pos2Length.entrySet())
                {
                    if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
                    {
                        ok = false;
                        return;
                    }
                }
                // Determine which nodes need to load the row (reason it's nodeS is due to replicas).
                List<Integer> nodes = MetaData.determineNode(loadOperator.getSchema(), loadOperator.getTable(), row, loadOperator.getTransaction(), pmeta, cols2Pos, numNodes);
                final int device = MetaData.determineDevice(row, pmeta, cols2Pos);

                loadOperator.getLock().readLock().lock();
                for (final Integer node : nodes)
                {   // Copy the same data to all the nodes where it should be replicated.  Replicated across nodes, not across devices.
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

                    master = FlushMasterThread.flush(loadOperator, indexes, keys, types, orders, type);
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
            HRDBMSWorker.logger.debug("c.e.o.l.ExternalReadThread error", e);
        }
    }
}