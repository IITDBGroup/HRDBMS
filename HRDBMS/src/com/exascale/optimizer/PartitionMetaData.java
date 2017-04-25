package com.exascale.optimizer;

import com.exascale.managers.PlanCacheManager;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
import com.exascale.tables.Transaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

/** POJO representation of SYS.PARTITIONING metadata */
public class PartitionMetaData implements Serializable
{
    private static final int NODEGROUP_NONE = -3;
    private static final int NODE_ANY = -2;
    public static final int NODE_ALL = -1;
    private static final int DEVICE_ALL = -1;
    public static final String NONE = "NONE", ANY = "ANY", ALL = "ALL", HASH = "HASH", RANGE = "RANGE";
    private ArrayList<Integer> nodeGroupSet;
    private ArrayList<String> nodeGroupHash;
    private ArrayList<Object> nodeGroupRange;
    private int numNodeGroups;
    private String nodeGroupRangeCol;
    private HashMap<Integer, ArrayList<Integer>> nodeGroupHashMap;
    private ArrayList<Integer> nodeSet;
    private int numNodes;
    private ArrayList<String> nodeHash;
    private ArrayList<Object> nodeRange;
    private String nodeRangeCol;
    private int numDevices;
    private ArrayList<Integer> deviceSet;
    private ArrayList<String> deviceHash;
    private ArrayList<Object> deviceRange;
    private String deviceRangeCol;
    private final String schema;
    private final String table;
    private final Transaction tx;
    private final String ngExp, nExp, dExp;

    public PartitionMetaData(final String schema, final String table, final String ngExp, final String nExp, final String dExp, final Transaction tx, final HashMap<String, String> cols2Types) throws Exception
    {
        this.schema = schema;
        this.table = table;
        this.tx = tx;
        this.ngExp = ngExp;
        this.nExp = nExp;
        this.dExp = dExp;
        setNGData2(ngExp, cols2Types);
        setNData2(nExp, cols2Types);
        setDData2(dExp, cols2Types);
    }

    public PartitionMetaData(final String schema, final String table, final Transaction tx) throws Exception
    {
        this.schema = schema;
        this.table = table;
        this.tx = tx;
        ArrayList<Object> row = MetaData.getPartitioningCache(schema + "." + table);
        if (row == null)
        {
            row = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
            MetaData.putPartitioningCache(schema + "." + table, row);
        }
        ngExp = (String)row.get(0);
        nExp = (String)row.get(1);
        dExp = (String)row.get(2);
        setNGData(ngExp);
        setNData(nExp);
        setDData(dExp);
    }

    public boolean allDevices()
    {
        return deviceSet.get(0) == DEVICE_ALL;
    }

    public boolean allNodes()
    {
        return nodeSet.get(0) == NODE_ALL;
    }

    public boolean anyNode()
    {
        return nodeSet.get(0) == NODE_ANY;
    }

    public boolean deviceIsHash()
    {
        return deviceHash != null;
    }

    public ArrayList<Integer> deviceSet()
    {
        return deviceSet;
    }

    public ArrayList<String> getDeviceHash()
    {
        return deviceHash;
    }

    public String getDeviceRangeCol()
    {
        return deviceRangeCol;
    }

    public ArrayList<Object> getDeviceRanges()
    {
        return deviceRange;
    }

    public String getDExp()
    {
        return dExp;
    }

    public String getNExp()
    {
        return nExp;
    }

    public String getNGExp()
    {
        return ngExp;
    }

    public ArrayList<String> getNodeGroupHash()
    {
        return nodeGroupHash;
    }

    public HashMap<Integer, ArrayList<Integer>> getNodeGroupHashMap()
    {
        return nodeGroupHashMap;
    }

    public String getNodeGroupRangeCol()
    {
        return nodeGroupRangeCol;
    }

    public ArrayList<Object> getNodeGroupRanges()
    {
        return nodeGroupRange;
    }

    public ArrayList<String> getNodeHash()
    {
        return nodeHash;
    }

    public String getNodeRangeCol()
    {
        return nodeRangeCol;
    }

    public ArrayList<Object> getNodeRanges()
    {
        return nodeRange;
    }

    public int getNumDevices()
    {
        return numDevices;
    }

    public int getNumNodeGroups()
    {
        return numNodeGroups;
    }

    public int getNumNodes()
    {
        return numNodes;
    }

    public String getSchema()
    {
        return schema;
    }

    public int getSingleDevice()
    {
        if (deviceSet.get(0) == DEVICE_ALL)
        {
            return 0;
        }

        return deviceSet.get(0);
    }

    public int getSingleNode()
    {
        return nodeSet.get(0);
    }

    public int getSingleNodeGroup()
    {
        return nodeGroupSet.get(0);
    }

    public String getTable()
    {
        return table;
    }

    public boolean isSingleDeviceSet()
    {
        return numDevices == 1;
    }

    public boolean isSingleNodeGroupSet()
    {
        return numNodeGroups == 1;
    }

    public boolean isSingleNodeSet()
    {
        return numNodes == 1;
    }

    public boolean nodeGroupIsHash()
    {
        return nodeGroupHash != null;
    }

    public ArrayList<Integer> nodeGroupSet()
    {
        return nodeGroupSet;
    }

    public boolean nodeIsHash()
    {
        return nodeHash != null;
    }

    public ArrayList<Integer> nodeSet()
    {
        return nodeSet;
    }

    public boolean noNodeGroupSet()
    {
        return nodeGroupSet.get(0) == NODEGROUP_NONE;
    }

    private void otherNG(final String exp) throws Exception
    {
        final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
        String set = tokens.nextToken().substring(1);
        set = set.substring(0, set.length() - 1);
        StringTokenizer tokens2 = new StringTokenizer(set, "{}", false);
        nodeGroupSet = new ArrayList<Integer>();
        numNodeGroups = 0;
        int setNum = 0;
        nodeGroupHashMap = new HashMap<Integer, ArrayList<Integer>>();
        while (tokens2.hasMoreTokens())
        {
            final String nodesInGroup = tokens2.nextToken();
            nodeGroupSet.add(setNum);
            numNodeGroups++;

            final ArrayList<Integer> nodeListForGroup = new ArrayList<Integer>();
            final FastStringTokenizer tokens3 = new FastStringTokenizer(nodesInGroup, "|", false);
            while (tokens3.hasMoreTokens())
            {
                final String token = tokens3.nextToken();
                nodeListForGroup.add(Integer.parseInt(token));
            }
            nodeGroupHashMap.put(setNum, nodeListForGroup);
            setNum++;
        }

        if (numNodeGroups == 1)
        {
            return;
        }

        final String type = tokens.nextToken();
        if (type.equals(HASH))
        {
            set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            tokens2 = new StringTokenizer(set, "|", false);
            nodeGroupHash = new ArrayList<String>();
            while (tokens2.hasMoreTokens())
            {
                nodeGroupHash.add(tokens2.nextToken());
            }
        }
        else if (type.equals(RANGE))
        {
            nodeGroupRangeCol = tokens.nextToken();
            set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol, tx);
        }
        else
        {
            throw new Exception("Node group type was not range or hash");
        }
    }

    private void setDData(final String exp) throws Exception
    {
        final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
        final String first = tokens.nextToken();

        if (first.equals(ALL))
        {
            deviceSet = new ArrayList<Integer>(1);
            deviceSet.add(DEVICE_ALL);
            numDevices = MetaData.getNumDevices();
        }
        else
        {
            setDNotAll(first);
        }

        if (numDevices == 1)
        {
            return;
        }

        final String type = tokens.nextToken();
        if (type.equals(HASH))
        {
            String set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
            deviceHash = new ArrayList<String>(tokens2.allTokens().length);
            while (tokens2.hasMoreTokens())
            {
                deviceHash.add(tokens2.nextToken());
            }
        }
        else
        {
            setDNotHash(tokens);
        }
    }

    private void setDData2(final String exp, final HashMap<String, String> cols2Types) throws Exception
    {
        final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
        final String first = tokens.nextToken();

        if (first.equals(ALL))
        {
            deviceSet = new ArrayList<Integer>(1);
            deviceSet.add(DEVICE_ALL);
            numDevices = MetaData.getNumDevices();
        }
        else
        {
            String set = first.substring(1);
            set = set.substring(0, set.length() - 1);
            final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
            deviceSet = new ArrayList<Integer>(tokens2.allTokens().length);
            numDevices = 0;
            while (tokens2.hasMoreTokens())
            {
                final int device = Integer.parseInt(tokens2.nextToken());
                if (device >= MetaData.getNumDevices())
                {
                    throw new Exception("Invalid device number: " + device);
                }
                deviceSet.add(device);
                numDevices++;
            }
        }

        if (numDevices == 1)
        {
            return;
        }

        final String type = tokens.nextToken();
        if (type.equals(HASH))
        {
            String set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
            deviceHash = new ArrayList<String>(tokens2.allTokens().length);
            while (tokens2.hasMoreTokens())
            {
                deviceHash.add(tokens2.nextToken());
            }
        }
        else if (type.equals(RANGE))
        {
            deviceRangeCol = tokens.nextToken();
            String set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            final String type2 = cols2Types.get(deviceRangeCol);
            if (type2 == null)
            {
                throw new Exception("Device range column does not exist");
            }
            deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol, tx, type2);
            if (deviceRange.size() != numDevices - 1)
            {
                throw new Exception("Wrong number of device ranges");
            }
        }
        else
        {
            throw new Exception("Device type is not hash or range");
        }
    }

    private void setDNotAll(final String first)
    {
        String set = first.substring(1);
        set = set.substring(0, set.length() - 1);
        final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
        deviceSet = new ArrayList<Integer>(tokens2.allTokens().length);
        numDevices = 0;
        while (tokens2.hasMoreTokens())
        {
            deviceSet.add(Utils.parseInt(tokens2.nextToken()));
            numDevices++;
        }
    }

    private void setDNotHash(final FastStringTokenizer tokens) throws Exception
    {
        deviceRangeCol = tokens.nextToken();
        String set = tokens.nextToken().substring(1);
        set = set.substring(0, set.length() - 1);
        deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol, tx);
    }

    private void setNData(final String exp) throws Exception
    {
        final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
        final String first = tokens.nextToken();

        if (first.equals(ANY))
        {
            nodeSet = new ArrayList<Integer>(1);
            nodeSet.add(NODE_ANY);
            numNodes = 1;
            return;
        }

        if (first.equals(ALL))
        {
            nodeSet = new ArrayList<Integer>(1);
            nodeSet.add(NODE_ALL);
            numNodes = MetaData.numWorkerNodes;
        }
        else
        {
            setNNotAA(exp, first);
        }

        if (numNodes == 1)
        {
            return;
        }

        final String type = tokens.nextToken();
        if (type.equals(HASH))
        {
            String set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
            nodeHash = new ArrayList<String>(tokens2.allTokens().length);
            while (tokens2.hasMoreTokens())
            {
                nodeHash.add(tokens2.nextToken());
            }
        }
        else
        {
            setNNotHash(tokens);
        }
    }

    private void setNData2(final String exp, final HashMap<String, String> cols2Types) throws Exception
    {
        final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
        final String first = tokens.nextToken();

        if (first.equals(ANY))
        {
            nodeSet = new ArrayList<Integer>(1);
            nodeSet.add(NODE_ANY);
            numNodes = 1;
            if (nodeGroupSet.get(0) != PartitionMetaData.NODEGROUP_NONE)
            {
                throw new Exception("Can't use nodegroups with a table using ANY node partitioning");
            }
            return;
        }

        if (first.equals(ALL))
        {
            nodeSet = new ArrayList<Integer>(1);
            nodeSet.add(NODE_ALL);
            numNodes = MetaData.numWorkerNodes;
        }
        else
        {
            String set = first.substring(1);
            set = set.substring(0, set.length() - 1);
            final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
            nodeSet = new ArrayList<Integer>(tokens2.allTokens().length);
            numNodes = 0;
            while (tokens2.hasMoreTokens())
            {
                final int node = Integer.parseInt(tokens2.nextToken());
                if (node >= MetaData.numWorkerNodes)
                {
                    throw new Exception("Invalid node number: " + node);
                }
                nodeSet.add(node);
                numNodes++;
            }
        }

        if (numNodes == 1)
        {
            return;
        }

        final String type = tokens.nextToken();
        if (type.equals(HASH))
        {
            String set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
            nodeHash = new ArrayList<String>(tokens2.allTokens().length);
            while (tokens2.hasMoreTokens())
            {
                final String col = tokens2.nextToken();
                if (!cols2Types.containsKey(col))
                {
                    throw new Exception("Hash column " + col + " does not exist in " + cols2Types);
                }
                nodeHash.add(col);
            }
        }
        else if (type.equals(RANGE))
        {
            nodeRangeCol = tokens.nextToken();
            final String type2 = cols2Types.get(nodeRangeCol);
            if (type2 == null)
            {
                throw new Exception("Node range column does not exist");
            }
            String set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol, tx, type2);
            if (nodeRange.size() != numNodes - 1)
            {
                throw new Exception("Wrong number of node ranges");
            }
        }
        else
        {
            throw new Exception("Node type is not hash or range");
        }
    }

    private void setNGData(final String exp) throws Exception
    {
        if (exp.equals(NONE))
        {
            nodeGroupSet = new ArrayList<Integer>(1);
            nodeGroupSet.add(NODEGROUP_NONE);
            return;
        }

        otherNG(exp);
    }

    private void setNGData2(final String exp, final HashMap<String, String> cols2Types) throws Exception
    {
        if (exp.equals(NONE))
        {
            nodeGroupSet = new ArrayList<Integer>(1);
            nodeGroupSet.add(NODEGROUP_NONE);
            return;
        }

        final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
        String set = tokens.nextToken().substring(1);
        set = set.substring(0, set.length() - 1);
        StringTokenizer tokens2 = new StringTokenizer(set, "{}", false);
        nodeGroupSet = new ArrayList<Integer>();
        numNodeGroups = 0;
        int setNum = 0;
        nodeGroupHashMap = new HashMap<Integer, ArrayList<Integer>>();
        int expectedNumNodesInGroup = -1;
        while (tokens2.hasMoreTokens())
        {
            final String nodesInGroup = tokens2.nextToken();
            int nodeCount = 0;
            nodeGroupSet.add(setNum);
            numNodeGroups++;

            final ArrayList<Integer> nodeListForGroup = new ArrayList<Integer>();
            final FastStringTokenizer tokens3 = new FastStringTokenizer(nodesInGroup, "|", false);
            while (tokens3.hasMoreTokens())
            {
                final String token = tokens3.nextToken();
                final int node = Integer.parseInt(token);
                if (node >= MetaData.numWorkerNodes)
                {
                    throw new Exception("Invalid node number: " + node);
                }
                nodeListForGroup.add(node);
                nodeCount++;
            }
            nodeGroupHashMap.put(setNum, nodeListForGroup);
            setNum++;
            if (expectedNumNodesInGroup == -1)
            {
                expectedNumNodesInGroup = nodeCount;
            }
            else if (expectedNumNodesInGroup != nodeCount)
            {
                throw new Exception("Expected " + expectedNumNodesInGroup + " nodes in node group but found " + nodeCount + " nodes");
            }
        }

        if (numNodeGroups == 1)
        {
            return;
        }

        final String type = tokens.nextToken();
        if (type.equals(HASH))
        {
            set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            tokens2 = new StringTokenizer(set, "|", false);
            nodeGroupHash = new ArrayList<String>();
            while (tokens2.hasMoreTokens())
            {
                final String col = tokens2.nextToken();
                if (!cols2Types.containsKey(col))
                {
                    throw new Exception("Hash column " + col + " does not exist");
                }
                nodeGroupHash.add(col);
            }
        }
        else if (type.equals(RANGE))
        {
            nodeGroupRangeCol = tokens.nextToken();
            final String type2 = cols2Types.get(nodeGroupRangeCol);
            if (type2 == null)
            {
                throw new Exception("Node group range column does not exist");
            }
            set = tokens.nextToken().substring(1);
            set = set.substring(0, set.length() - 1);
            nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol, tx, type2);
            if (nodeGroupRange.size() != numNodeGroups - 1)
            {
                throw new Exception("Wrong number of node group ranges");
            }
        }
        else
        {
            throw new Exception("Node group type was not range or hash");
        }
    }

    private void setNNotAA(final String exp, final String first)
    {
        String set = first.substring(1);
        set = set.substring(0, set.length() - 1);
        final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
        nodeSet = new ArrayList<Integer>(tokens2.allTokens().length);
        numNodes = 0;
        while (tokens2.hasMoreTokens())
        {
            nodeSet.add(Integer.parseInt(tokens2.nextToken()));
            numNodes++;
        }
    }

    private void setNNotHash(final FastStringTokenizer tokens) throws Exception
    {
        nodeRangeCol = tokens.nextToken();
        String set = tokens.nextToken().substring(1);
        set = set.substring(0, set.length() - 1);
        nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol, tx);
    }

    private static ArrayList<Object> convertRangeStringToObject(final String set, final String schema, final String table, final String rangeCol, final Transaction tx) throws Exception
    {
        String type = MetaData.getColTypeCache(schema + "." + table + "." + rangeCol);
        if (type == null)
        {
            type = PlanCacheManager.getColType().setParms(schema, table, rangeCol).execute(tx);
            MetaData.putColTypeCache(schema + "." + table + "." + rangeCol, type);
        }
        final ArrayList<Object> retval = new ArrayList<Object>();
        final StringTokenizer tokens = new StringTokenizer(set, "{}|");
        while (tokens.hasMoreTokens())
        {
            final String token = tokens.nextToken();
            if (type.equals("INT"))
            {
                retval.add(Integer.parseInt(token));
            }
            else if (type.equals("BIGINT"))
            {
                retval.add(Long.parseLong(token));
            }
            else if (type.equals("DOUBLE"))
            {
                retval.add(Double.parseDouble(token));
            }
            else if (type.equals("VARCHAR"))
            {
                retval.add(token);
            }
            else if (type.equals("DATE"))
            {
                final int year = Integer.parseInt(token.substring(0, 4));
                final int month = Integer.parseInt(token.substring(5, 7));
                final int day = Integer.parseInt(token.substring(8, 10));
                retval.add(new MyDate(year, month, day));
            }
        }

        return retval;
    }

    private static ArrayList<Object> convertRangeStringToObject(final String set, final String schema, final String table, final String rangeCol, final Transaction tx, final String type) throws Exception
    {
        final ArrayList<Object> retval = new ArrayList<Object>();
        final StringTokenizer tokens = new StringTokenizer(set, "{}|");
        while (tokens.hasMoreTokens())
        {
            final String token = tokens.nextToken();
            if (type.equals("INT"))
            {
                retval.add(Integer.parseInt(token));
            }
            else if (type.equals("BIGINT"))
            {
                retval.add(Long.parseLong(token));
            }
            else if (type.equals("DOUBLE"))
            {
                retval.add(Double.parseDouble(token));
            }
            else if (type.equals("VARCHAR"))
            {
                retval.add(token);
            }
            else if (type.equals("DATE"))
            {
                final int year = Integer.parseInt(token.substring(0, 4));
                final int month = Integer.parseInt(token.substring(5, 7));
                final int day = Integer.parseInt(token.substring(8, 10));
                retval.add(new MyDate(year, month, day));
            }
        }

        return retval;
    }
}