package com.exascale.optimizer.externalTable;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.*;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.*;
import com.exascale.tables.Transaction;

public final class ExternalTableScanOperator extends AbstractTableScanOperator
{
	private static sun.misc.Unsafe unsafe;
	private ExternalTableType tableImpl = null;

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
	}

	public ExternalTableScanOperator(ExternalTableType tableImpl, final String schema, final String name, final MetaData meta, final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types) {
		super(schema, name, meta, cols2Pos, pos2Col, cols2Types);
		this.tableImpl = tableImpl;
	}

	public static ExternalTableScanOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception {
		ExternalTableScanOperator value = (ExternalTableScanOperator) unsafe.allocateInstance(ExternalTableScanOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.node = OperatorUtils.readInt(in);
		return value;
	}

	@Override
	public ExternalTableScanOperator clone() {
		ExternalTableScanOperator retval = null;
		try {
			retval = new ExternalTableScanOperator(tableImpl, schema, name, meta, cols2Pos, pos2Col, cols2Types);
		} catch (Exception e) {
			HRDBMSWorker.logger.error("clone", e);
		}
		return retval;
	}

    @Override
    public void start() throws Exception
    {
		tableImpl.setCols2Pos(cols2Pos);
		tableImpl.setCols2Types(cols2Types);
		tableImpl.setPos2Col(pos2Col);
		tableImpl.setSchema(schema);
		tableImpl.setName(name);
		tableImpl.start();
    }

    @Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		tableImpl.close();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public void registerParent(Operator op)
	{
		if(parents.isEmpty()) {
			super.registerParent(op);
		}
		else
		{
			throw new UnsupportedOperationException("ExternalTableScanOperator only supports 1 parent.");
		}
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(23, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.writeInt(node, out);
	}

	@Override
	public String toString()
	{
		return "ExternalTableScanOperator";
	}

	@Override
	public Object next(final Operator op) throws Exception
	{
        List<?> row = tableImpl.next();
		if (row == null) {
			return new DataEndMarker();
		} else {
			return row;
		}
	}
}