package com.exascale.optimizer.load;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.OperatorUtils;
import com.exascale.threads.HRDBMSThread;
import org.antlr.v4.runtime.misc.Pair;

import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/** Sends data to be loaded to the worker over the network. */
public class FlushThread extends HRDBMSThread
{
    private List<ArrayList<Object>> list;
    private ArrayList<String> indexes;
    private boolean ok = true;
    private final long key;
    private HashMap<String, Integer> cols2Pos;
    private ArrayList<ArrayList<String>> keys;
    private ArrayList<ArrayList<String>> types;
    private ArrayList<ArrayList<Boolean>> orders;
    private final TreeMap<Integer, String> pos2Col;
    private final HashMap<String, String> cols2Types;
    private final int type;
    private final LoadOperator loadOperator;

    public FlushThread(final List<ArrayList<Object>> list, final ArrayList<String> indexes, final long key, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final int type, final LoadOperator loadOperator)
    {
        this.list = list;
        this.indexes = indexes;
        this.key = key;
        this.cols2Pos = loadOperator.getCols2Pos();
        this.keys = keys;
        this.types = types;
        this.orders = orders;
        this.pos2Col = loadOperator.getPos2Col();
        this.cols2Types = loadOperator.getCols2Types();
        this.type = type;
        this.loadOperator = loadOperator;
    }

    public int getDevice()
    {
        return (int)(key & 0xFFFFFFFFL);
    }

    public int getNode()
    {
        return (int)(key >> 32);
    }

    public boolean getOK()
    {
        return ok;
    }

    @Override
    public void run()
    {
        // send schema, table, tx, indexes, list, and cols2Pos
        Socket sock = null;
        try
        {
            final int node = (int)(key >> 32);
            final int device = (int)(key & 0x00000000FFFFFFFF);
            final String hostname = MetaData.getHostNameForNode(node, loadOperator.getTransaction());
            int i = 0;
            while (true)
            {
                try
                {
                    sock = new Socket();
                    sock.setReceiveBufferSize(4194304);
                    sock.setSendBufferSize(4194304);
                    sock.connect(new InetSocketAddress(hostname, LoadOperator.PORT_NUMBER));
                    break;
                }
                catch (final ConnectException e)
                {
                    i++;
                    if (i == 60)
                    {
                        sock.close();
                        throw e;
                    }
                    Thread.sleep(5000);
                }
            }
            // The other side of this connection is read in ConnectionWorker.load()
            final OutputStream out = sock.getOutputStream();
            final byte[] outMsg = "LOAD            ".getBytes(StandardCharsets.UTF_8);
            outMsg[8] = 0;
            outMsg[9] = 0;
            outMsg[10] = 0;
            outMsg[11] = 0;
            outMsg[12] = 0;
            outMsg[13] = 0;
            outMsg[14] = 0;
            outMsg[15] = 0;
            out.write(outMsg);
            out.write(OperatorUtils.longToBytes(loadOperator.getTransaction().number()));
            out.write(OperatorUtils.intToBytes(device));
            out.write(OperatorUtils.stringToBytes(loadOperator.getSchema()));
            out.write(OperatorUtils.stringToBytes(loadOperator.getTable()));
            out.write(OperatorUtils.intToBytes(type));
            out.write(OperatorUtils.rsToBytes(list));
            list = null;
            final ObjectOutputStream objOut = new ObjectOutputStream(out);
            objOut.writeObject(indexes);
            objOut.writeObject(keys);
            objOut.writeObject(types);
            objOut.writeObject(orders);
            objOut.writeObject(cols2Pos);
            objOut.writeObject(pos2Col);
            objOut.writeObject(cols2Types);
            objOut.flush();
            out.flush();
            OperatorUtils.getConfirmation(sock);
            loadOperator.getWaitTill().get(new Pair(getNode(), getDevice())).decrementAndGet();

            objOut.close();
            sock.close();
            indexes = null;
            cols2Pos = null;
            keys = null;
            types = null;
            orders = null;
        }
        catch (final Exception e)
        {
            try
            {
                if (sock != null)
                {
                    sock.close();
                }
            }
            catch (final Exception f)
            {
            }
            ok = false;
            HRDBMSWorker.logger.debug("", e);
        }
    }
}