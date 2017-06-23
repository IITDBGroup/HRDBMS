package com.exascale.optimizer.externalTable;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HrdbmsType;
import com.exascale.optimizer.*;
import com.exascale.tables.Transaction;
import com.exascale.threads.ThreadPoolThread;

public final class ExternalTableScanOperator extends TableScanOperator
{
	private static sun.misc.Unsafe unsafe;

    private int numNodes;

	static {
		try {
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe) f.get(null);
		} catch (Exception e) {
			unsafe = null;
		}
	}

    public ExternalTableScanOperator(ExternalTableType tableImpl, final String schema, final String name, final MetaData meta, final Transaction tx) throws Exception {
		super(schema, name, meta, tx);
		this.tableImpl = tableImpl;
        this.numNodes = MetaData.numWorkerNodes;
    }

    public ExternalTableScanOperator(ExternalTableType tableImpl, final String schema, final String name, final MetaData meta, final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final TreeMap<Integer, String> tablePos2Col, final HashMap<String, String> tableCols2Types, final HashMap<String, Integer> tableCols2Pos) throws Exception
    {
        super(schema, name, meta, cols2Pos, pos2Col, cols2Types, tablePos2Col, tableCols2Types, tableCols2Pos);
        this.tableImpl = tableImpl;
        this.numNodes = MetaData.numWorkerNodes;
    }

	public static ExternalTableScanOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception {
        final ExternalTableScanOperator value = (ExternalTableScanOperator) unsafe.allocateInstance(ExternalTableScanOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
        value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
        value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
        value.pos2Col = OperatorUtils.deserializeTM(in, prev);
        value.name = OperatorUtils.readString(in, prev);
        value.schema = OperatorUtils.readString(in, prev);
        value.parents = OperatorUtils.deserializeALOp(in, prev);
        value.startDone = OperatorUtils.readBool(in);
        value.orderedFilters = OperatorUtils.deserializeHMOpCNF(in, prev);
        value.meta = new MetaData();
        value.neededPos = OperatorUtils.deserializeALI(in, prev);
        value.fetchPos = OperatorUtils.deserializeALI(in, prev);
        value.midPos2Col = OperatorUtils.deserializeStringArray(in, prev);
        value.midCols2Types = OperatorUtils.deserializeStringHM(in, prev);
        value.set = OperatorUtils.readBool(in);
        value.devices = OperatorUtils.deserializeALI(in, prev);
        value.node = OperatorUtils.readInt(in);
        value.numNodes = OperatorUtils.readInt(in);
        value.phase2Done = OperatorUtils.readBool(in);
        value.device2Child = OperatorUtils.deserializeHMIntOp(in, prev);
        value.children = OperatorUtils.deserializeALOp(in, prev);
        value.indexOnly = OperatorUtils.readBool(in);
        value.tx = new Transaction(OperatorUtils.readLong(in));
        value.alias = OperatorUtils.readString(in, prev);
        value.getRID = OperatorUtils.readBool(in);
        value.tableCols2Types = OperatorUtils.deserializeStringHM(in, prev);
        value.tablePos2Col = OperatorUtils.deserializeTM(in, prev);
        value.tableCols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
        value.releaseLocks = OperatorUtils.readBool(in);
        value.sample = OperatorUtils.readBool(in);
        value.sPer = OperatorUtils.readLong(in);
        value.scanIndex = OperatorUtils.deserializeIndex(in, prev);
        value.received = new AtomicLong(0);
        value.demReceived = false;
        value.tType = OperatorUtils.readInt(in);
        String implClass = OperatorUtils.readString(in, prev);
        if (implClass.equals(HDFSCsvExternal.class.getSimpleName())) {
            value.tableImpl = OperatorUtils.deserializeHDFSCsvExternal(in, prev);
        } else if (implClass.equals(HTTPCsvExternal.class.getSimpleName())) {
            value.tableImpl = OperatorUtils.deserializeCSVExternal(in, prev);
        } else {
            throw new Exception("Unknown External Table implementation");
        }

        return value;
	}

	@Override
	public ExternalTableScanOperator clone() {
		ExternalTableScanOperator retval;
		try
		{
			retval = new ExternalTableScanOperator(tableImpl, schema, name, meta, cols2Pos, pos2Col, cols2Types, tablePos2Col, tableCols2Types, tableCols2Pos);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			return null;
		}
		if (neededPos != null)
		{
			retval.neededPos = (ArrayList<Integer>)neededPos.clone();
		}
		if (fetchPos != null)
		{
			retval.fetchPos = (ArrayList<Integer>)fetchPos.clone();
		}
		if (midPos2Col != null)
		{
			retval.midPos2Col = midPos2Col.clone();
		}
		if (midCols2Types != null)
		{
			retval.midCols2Types = (HashMap<String, String>)midCols2Types.clone();
		}
		retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
		retval.set = set;
		retval.partMeta = partMeta;
		retval.phase2Done = phase2Done;
		retval.node = node;
        retval.numNodes = numNodes;
		retval.sample = sample;
		retval.sPer = sPer;
		retval.scanIndex = scanIndex;
		retval.tType = tType;
		if (devices != null)
		{
			retval.devices = (ArrayList<Integer>)devices.clone();
		}
		retval.indexOnly = indexOnly;
		if (alias != null && !alias.equals(""))
		{
			retval.setAlias(alias);
		}
		retval.getRID = getRID;
		retval.tx = tx;
		return retval;
	}

	@Override
    public void setNeededCols(ArrayList<String> needed)
    {
        // method body intentionally left blank
		// to prevent column disorder in SELECT from external source
    }


	@Override
    public void start() throws Exception
    {
		tableImpl.setCols2Pos(cols2Pos);
		tableImpl.setCols2Types(cols2Types);
		tableImpl.setPos2Col(pos2Col);
		tableImpl.setSchema(schema);
		tableImpl.setName(name);

		if (tableImpl instanceof MultiThreadedExternalTableType) {
			((MultiThreadedExternalTableType) tableImpl).setNode(node);
			((MultiThreadedExternalTableType) tableImpl).setNumNodes(numNodes);
			((MultiThreadedExternalTableType) tableImpl).setNumDevices(devices.size());
		}

		tableImpl.start();

		if (tableImpl instanceof MultiThreadedExternalTableType) {
            super.start();
		}

    }

    @Override
	public void close() throws Exception
	{
		super.close();
		tableImpl.close();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.ETSO, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeString(name, out, prev);
		OperatorUtils.writeString(schema, out, prev);
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.serializeHMOpCNF(orderedFilters, out, prev);
		// recreate meta
		OperatorUtils.serializeALI(neededPos, out, prev);
		OperatorUtils.serializeALI(fetchPos, out, prev);
		OperatorUtils.serializeStringArray(midPos2Col, out, prev);
		OperatorUtils.serializeStringHM(midCols2Types, out, prev);
		OperatorUtils.writeBool(set, out);
		OperatorUtils.serializeALI(devices, out, prev);
		OperatorUtils.writeInt(node, out);
        OperatorUtils.writeInt(numNodes, out);
		OperatorUtils.writeBool(phase2Done, out);
		OperatorUtils.serializeHMIntOp(device2Child, out, prev);
		OperatorUtils.serializeALOp(children, out, prev);
		OperatorUtils.writeBool(indexOnly, out);
		OperatorUtils.writeLong(tx.number(), out); // notice type
		OperatorUtils.writeString(alias, out, prev);
		OperatorUtils.writeBool(getRID, out);
		OperatorUtils.serializeStringHM(tableCols2Types, out, prev);
		OperatorUtils.serializeTM(tablePos2Col, out, prev);
		OperatorUtils.serializeStringIntHM(tableCols2Pos, out, prev);
		OperatorUtils.writeBool(releaseLocks, out);
		OperatorUtils.writeBool(sample, out);
		OperatorUtils.writeLong(sPer, out);
		OperatorUtils.serializeIndex(scanIndex, out, prev);
		OperatorUtils.writeInt(tType, out);
        if (tableImpl instanceof HDFSCsvExternal) {
            OperatorUtils.writeString(HDFSCsvExternal.class.getSimpleName(), out, prev);
            OperatorUtils.serializeHDFSCsvExternal((HDFSCsvExternal) tableImpl, out, prev);
        } else if (tableImpl instanceof HTTPCsvExternal) {
            OperatorUtils.writeString(HTTPCsvExternal.class.getSimpleName(), out, prev);
            OperatorUtils.serializeCsvExternal((HTTPCsvExternal) tableImpl, out, prev);
        } else {
            throw new Exception("Unknown External Table implementation");
        }
	}

	@Override
	public synchronized Object next(final Operator op) throws Exception
	{
		if (tableImpl instanceof MultiThreadedExternalTableType) {
			return super.next(op);
		} else {
			do {
				List<?> row = tableImpl.next();
				if (row == null) {
					return new DataEndMarker();
				}

				CNFFilter filter = orderedFilters.get(parents.get(0));
				if (filter != null) {
					if (filter.passes((ArrayList<Object>) row)) {
						return row;
					}
				} else {
					return row;
				}
			} while (true);
		}
	}
}