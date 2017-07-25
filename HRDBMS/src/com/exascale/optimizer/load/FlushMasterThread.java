package com.exascale.optimizer.load;

import com.exascale.optimizer.PartitionMetaData;
import com.exascale.threads.HRDBMSThread;
import org.antlr.v4.runtime.misc.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Starts up several child threads to flush data out to the workers */
public class FlushMasterThread extends HRDBMSThread {
    private final List<String> indexes;
    private boolean ok;
    private final List<List<String>> keys;
    private final List<List<String>> types;
    private final List<List<Boolean>> orders;
    private boolean force = false;
    private final int type;
    private final LoadOperator loadOperator;

    public FlushMasterThread(final LoadOperator loadOperator, final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final boolean force, final int type)
    {
        this.loadOperator = loadOperator;
        this.indexes = indexes;
        this.keys = keys;
        this.types = types;
        this.orders = orders;
        this.force = force;
        this.type = type;
    }

    public FlushMasterThread(final LoadOperator loadOperator, final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final int type)
    {
        this(loadOperator, indexes, keys, types, orders, false, type);
    }

    public boolean getOK()
    {
        return ok;
    }

    @Override
    public void run()
    {
        ok = true;
        if (!force && loadOperator.getMap().totalSize() <= LoadOperator.MAX_BATCH)
        {
            return;
        }

        ArrayList<FlushThread> threads = null;

        {
            loadOperator.getLock().writeLock().lock();
            {
                if (!force && loadOperator.getMap().totalSize() <= LoadOperator.MAX_BATCH)
                {
                    loadOperator.getLock().writeLock().unlock();
                    return;
                }

                threads = new ArrayList<>();
                for (final Object o : loadOperator.getMap().getKeySet())
                {
                    final long key = (Long)o;
                    final List<List<Object>> list = loadOperator.getMap().get(key);
                    threads.add(new FlushThread(list, indexes, key, keys, types, orders, type, loadOperator));
                }

                loadOperator.getMap().clear();

                final List<FlushThread> temp = new ArrayList<FlushThread>();
                for (final FlushThread thread : threads)
                {
                    final AtomicInteger ai = loadOperator.getWaitTill().get(new Pair(thread.getNode(), thread.getDevice()));

                    if (ai != null)
                    {
                        final int newCount = ai.incrementAndGet();
                        if (newCount > 2)
                        {
                            ai.decrementAndGet();
                            temp.add(thread);
                        }
                        else
                        {
                            thread.start();
                        }
                    }
                    else
                    {
                        loadOperator.getWaitTill().put(new Pair(thread.getNode(), thread.getDevice()), new AtomicInteger(1));
                        thread.start();
                    }
                }

                ArrayList<FlushThread> clone = new ArrayList<FlushThread>();
                for (final FlushThread thread : loadOperator.getWaitThreads())
                {
                    final AtomicInteger ai = loadOperator.getWaitTill().get(new Pair(thread.getNode(), thread.getDevice()));

                    if (ai != null)
                    {
                        final int newCount = ai.incrementAndGet();
                        if (newCount > 2)
                        {
                            ai.decrementAndGet();
                            clone.add(thread);
                        }
                        else
                        {
                            thread.start();
                        }
                    }
                    else
                    {
                        loadOperator.getWaitTill().put(new Pair(thread.getNode(), thread.getDevice()), new AtomicInteger(1));
                        thread.start();
                    }
                }

                loadOperator.setWaitThreads(clone);
                loadOperator.getWaitThreads().addAll(temp);

                while (loadOperator.getWaitThreads().size() > LoadOperator.MAX_QUEUED)
                {
                    try
                    {
                        Thread.sleep(1);
                    }
                    catch (final InterruptedException e)
                    {
                    }

                    // HRDBMSWorker.logger.debug("# of waiting threads = " +
                    // waitThreads.size()); //DEBUG

                    clone = new ArrayList<FlushThread>();
                    for (final FlushThread thread : loadOperator.getWaitThreads())
                    {
                        final AtomicInteger ai = loadOperator.getWaitTill().get(new Pair(thread.getNode(), thread.getDevice()));

                        if (ai != null)
                        {
                            final int newCount = ai.incrementAndGet();
                            if (newCount > 2)
                            {
                                ai.decrementAndGet();
                                clone.add(thread);
                            }
                            else
                            {
                                thread.start();
                            }
                        }
                        else
                        {
                            loadOperator.getWaitTill().put(new Pair(thread.getNode(), thread.getDevice()), new AtomicInteger(1));
                            thread.start();
                        }
                    }

                    loadOperator.setWaitThreads(clone);
                }
            }
            loadOperator.getLock().writeLock().unlock();
        }

        synchronized (loadOperator.getFlushThreads())
        {
            loadOperator.getFlushThreads().addAll(threads);
        }
    }

    static FlushMasterThread flush(final LoadOperator loadOperator, final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final boolean force, final int type)
    {
        final FlushMasterThread master = new FlushMasterThread(loadOperator, indexes, keys, types, orders, force, type);
        master.start();
        return master;
    }

    static FlushMasterThread flush(final LoadOperator loadOperator, final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final int type)
    {
        final FlushMasterThread master = new FlushMasterThread(loadOperator, indexes, keys, types, orders, type);
        master.start();
        return master;
    }
}