package com.exascale.optimizer;

import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

import javax.naming.OperationNotSupportedException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public abstract class AbstractTableScanOperator implements Operator, Serializable {
    protected transient MetaData meta;
    protected transient HashMap<Operator, Operator> opParents = new HashMap<Operator, Operator>();
    protected ArrayList<Operator> parents = new ArrayList<Operator>();
    protected HashMap<Operator, CNFFilter> orderedFilters = new HashMap<Operator, CNFFilter>();
    protected HashMap<String, String> cols2Types;
    protected HashMap<String, Integer> cols2Pos;
    protected TreeMap<Integer, String> pos2Col;
    protected Operator parent;
    protected int node;
    protected String name;
    protected String schema;
    protected String alias = "";
    protected boolean set = false;
    protected transient PartitionMetaData partMeta; // OK now that clone won't

    public AbstractTableScanOperator(final String schema, final String name, final MetaData meta, final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types) {
        this.meta = meta;
        this.name = name;
        this.schema = schema;
        this.cols2Types = (HashMap<String, String>)cols2Types.clone();
        this.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
        this.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
    }

    public AbstractTableScanOperator(final String schema, final String name, final MetaData meta, final Transaction tx) throws Exception {
        this.meta = meta;
        this.name = name;
        this.schema = schema;
        cols2Types = MetaData.getCols2TypesForTable(schema, name, tx);
        cols2Pos = MetaData.getCols2PosForTable(schema, name, tx);
        pos2Col = MetaData.cols2PosFlip(cols2Pos);
    }

    @Override
    public void add(Operator op) throws Exception
    {
        throw new OperationNotSupportedException("This table scan operator does not support children");
    }

    @Override
    public ArrayList<Operator> children()
    {
        final ArrayList<Operator> retval = new ArrayList<Operator>(0);
        return retval;
    }

    @Override
    public int getChildPos()
    {
        return 0;
    }

    @Override
    public HashMap<String, Integer> getCols2Pos()
    {
        return cols2Pos;
    }

    @Override
    public HashMap<String, String> getCols2Types()
    {
        return cols2Types;
    }

    @Override
    public MetaData getMeta()
    {
        return meta;
    }

    @Override
    public int getNode()
    {
        return node;
    }

    @Override
    public TreeMap<Integer, String> getPos2Col()
    {
        return pos2Col;
    }

    @Override
    public ArrayList<String> getReferences()
    {
        final ArrayList<String> retval = new ArrayList<String>(0);
        return retval;
    }

    @Override
    public void nextAll(Operator op) throws Exception
    {
    }

    @Override
    public long numRecsReceived()
    {
        return 0;
    }

    @Override
    public boolean receivedDEM()
    {
        return true;
    }

    @Override
    public void registerParent(final Operator op)
    {
        parents.add(op);
        if (opParents.containsKey(op))
        {
            orderedFilters.put(op, orderedFilters.get(opParents.get(op)));
            opParents.put(op.parent(), op);
        }
    }

    @Override
    public void removeChild(Operator op)
    {
    }

    @Override
    public void removeParent(final Operator op)
    {
        parents.remove(op);
    }

    @Override
    public void reset() throws Exception
    {
    }

    public boolean metaDataSet()
    {
        return set;
    }

    public void setMetaData(final Transaction t) throws Exception
    {
        set = true;
        partMeta = meta.getPartMeta(schema, name, t);
    }


    public void setAlias(final String alias)
    {
        this.alias = alias;
        final TreeMap<Integer, String> newPos2Col = new TreeMap<Integer, String>();
        final HashMap<String, Integer> newCols2Pos = new HashMap<String, Integer>();
        final HashMap<String, String> newCols2Types = new HashMap<String, String>();
        for (final Map.Entry entry : pos2Col.entrySet())
        {
            String val = (String)entry.getValue();
            val = val.substring(val.indexOf('.') + 1);
            newPos2Col.put((Integer)entry.getKey(), alias + "." + val);
            newCols2Pos.put(alias + "." + val, (Integer)entry.getKey());
        }

        for (final Map.Entry entry : cols2Types.entrySet())
        {
            String val = (String)entry.getKey();
            val = val.substring(val.indexOf('.') + 1);
            newCols2Types.put(alias + "." + val, (String)entry.getValue());
        }

        pos2Col = newPos2Col;
        cols2Pos = newCols2Pos;
        cols2Types = newCols2Types;
    }


    @Override
    public void setChildPos(final int pos)
    {
    }

    @Override
    public void setNode(final int node)
    {
        this.node = node;
    }

    @Override
    public void setPlan(final Plan plan)
    {
    }

    @Override
    public void start() throws Exception
    {
    }

    @Override
    public String toString()
    {
        return "AbstractTableScanOperator";
    }

    @Override
    public Operator clone() { return null; };

    public ArrayList<Operator> parents()
    {
        if (parents.size() == 0)
        {
            final ArrayList<Operator> retval = new ArrayList<Operator>();
            {
                retval.add(null);
            }
            return retval;
        }
        return parents;
    }

}