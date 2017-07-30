package com.exascale.optimizer.load;

import com.exascale.misc.HrdbmsType;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.NetworkReceiveOperator;
import com.exascale.optimizer.OperatorUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class LoadReceiveOperator extends NetworkReceiveOperator {
    private static sun.misc.Unsafe unsafe;
    private final LoadOperator loadOperator;

    static
    {
        try
        {
            final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (sun.misc.Unsafe)f.get(null);
            final Field fieldToUpdate = String.class.getDeclaredField("value");
            // get unsafe offset to this field
            offset = unsafe.objectFieldOffset(fieldToUpdate);
        }
        catch (final Exception e)
        {
            unsafe = null;
        }
    }

    public LoadReceiveOperator(final MetaData meta, final LoadOperator loadOperator) {
        super(meta);
        this.loadOperator = loadOperator;
    }

    public static LoadReceiveOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
    {
        final LoadReceiveOperator value = (LoadReceiveOperator)unsafe.allocateInstance(LoadReceiveOperator.class);
        NetworkReceiveOperator.deserialize(in, prev, value);
        return value;
    }

    @Override
    public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
    {
        final Long id = prev.get(this);
        if (id != null)
        {
            OperatorUtils.serializeReference(id, out);
            return;
        }

        OperatorUtils.writeType(HrdbmsType.LOADRO, out);
        serializeProtected(out, prev);
    }

    /** Once the workers have executed the load, the coordinator needs to wrap up by clustering the table and creating the indexes. */
    protected void preClose() throws Exception {
        // Note that we cluster the data on loading, but don't keep it up to date with new inserts
		MetaData.cluster(loadOperator.getSchema(), loadOperator.getTable(), loadOperator.getTransaction(), loadOperator.getPos2Col(), loadOperator.getCols2Types(), loadOperator.getTableType());
        final List<String> indexNames = new ArrayList<>();
        for (final String s : loadOperator.getIndexes()) {
            final int start = s.indexOf('.') + 1;
            final int end = s.indexOf('.', start);
            indexNames.add(s.substring(start, end));
        }

		for (final String index : indexNames) {
			MetaData.populateIndex(loadOperator.getSchema(), index, loadOperator.getTable(), loadOperator.getTransaction(), loadOperator.getCols2Pos());
		}

        super.preClose();
    }
}