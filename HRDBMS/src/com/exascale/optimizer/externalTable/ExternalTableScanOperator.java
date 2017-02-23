package com.exascale.optimizer.externalTable;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.*;
import com.exascale.tables.Transaction;

public final class ExternalTableScanOperator extends AbstractTableScanOperator
{
	private static sun.misc.Unsafe unsafe;

	private static int fakeRowsLimit = 5;
	private static int fakeRowsCounter = 1;

	static {
		try {
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe) f.get(null);
		} catch (Exception e) {
			unsafe = null;
		}
	}

	public ExternalTableScanOperator(final String schema, final String name, final MetaData meta, final Transaction tx) throws Exception {
		super(schema, name, meta, tx);
	}

	public ExternalTableScanOperator(final String schema, final String name, final MetaData meta, final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types) {
		super(schema, name, meta, cols2Pos, pos2Col, cols2Types);
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
			retval = new ExternalTableScanOperator(schema, name, meta, cols2Pos, pos2Col, cols2Types);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
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
		if (fakeRowsLimit < fakeRowsCounter) {
			return new DataEndMarker();
		} else {
			fakeRowsCounter++;
            ArrayList fakeRow = new ArrayList();
            fakeRow.add("A");
            fakeRow.add("B");
            fakeRow.add("C");
			return fakeRow;
		}
	}
}