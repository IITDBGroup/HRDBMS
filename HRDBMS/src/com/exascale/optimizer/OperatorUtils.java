package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.HrdbmsType;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.externalTable.*;
import com.exascale.optimizer.load.LoadOperator;
import com.exascale.optimizer.load.LoadReceiveOperator;

public class OperatorUtils
{
	public static int MAX_NEIGHBOR_NODES = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
	private static AtomicLong id = new AtomicLong(0);

	//TODO - CONVERT THIS TO USE THE com.exascale.misc.HrdbmsType enum everywhere.
	// 0 - reference
	// 1 - AntiJoin
	// 2 - HMStringString
	// 3 - String
	// 4 - HMStringInt
	// 5 - TreeMap
	// 6 - ALS
	// 7 - ALI
	// 8 - HSHM
	// 9 - HMF
	// 10 - ALIndx
	// 11 - ALHSHM
	// 12 - Double null
	// 13 - Double
	// 14 - Long null
	// 15 - Long
	// 16 - Integer null
	// 17 - Integer
	// 18 - MyDate null
	// 19 - MyDate
	// 20 - Case
	// 21 - Concat
	// 22 - DateMath
	// 23 - DEMOperator
	// 24 - DummyOperator
	// 25 - Except
	// 26 - ExtendObject
	// 27 - Extend
	// 28 - FST null
	// 29 - ADS
	// 30 - IndexOperator
	// 31 - Intersect
	// 32 - ALOp
	// 33 - Multi
	// 34 - ALAgOp
	// 35 - NRO
	// 36 - NSO
	// 37 - Project
	// 38 - Rename
	// 39 - Reorder
	// 40 - Root
	// 41 - Select
	// 42 - ALF
	// 43 - HSO
	// 44 - Sort
	// 45 - ALB
	// 46 - IntArray
	// 47 - Substring
	// 48 - Top
	// 49 - Union
	// 50 - Year
	// 51 - Avg
	// 52 - CountDistinct
	// 53 - Count
	// 54 - Max
	// 55 - Min
	// 56 - Sum
	// 57 - SJO
	// 58 - Product
	// 59 - NL
	// 60 - HJO
	// 61 - CNF null
	// 62 - FST
	// 63 - CNF
	// 64 - Index
	// 65 - Filter
	// 66 - StringArray
	// 67 - ALALF
	// 68 - HSS
	// 69 - Filter null
	// 70 - Bool null
	// 71 - Bool class
	// 72 - NHAS
	// 73 - NHRAM
	// 74 - NHRO
	// 75 - NRAM
	// 76 - NSMO
	// 77 - NSRR
	// 78 - TSO
	// 79 - HMOpCNF
	// 80 - HMIntOp
	// 81 - Operator null
	// 82 - Index null;
	// 83 - BoolArray
	// 84 - ALALO
	// 85 - Routing
	// 86 - Null String
	// 87 - ALRAIK
	// 88 - RAIK

	public static int bytesToInt(final byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	public static long bytesToLong(final byte[] val)
	{
		final long ret = java.nio.ByteBuffer.wrap(val).getLong();
		return ret;
	}

	public static short bytesToShort(final byte[] val)
	{
		final short ret = java.nio.ByteBuffer.wrap(val).getShort();
		return ret;
	}

	public static Deque<String> deserializeADS(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Deque<String>)readReference(in, prev);
		}

		if (!HrdbmsType.ADS.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ADS but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final ArrayDeque<String> retval = new ArrayDeque<String>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.push(readString(in, prev));
			i++;
		}

		return retval;
	}

	public static AggregateOperator deserializeAgOp(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		switch(type) {
			case REFERENCE:
				return (AggregateOperator) readReference(in, prev);
			case AVG:
				return AvgOperator.deserialize(in, prev);
			case COUNTDISTINCT:
				return CountDistinctOperator.deserialize(in, prev);
			case COUNT:
				return CountOperator.deserialize(in, prev);
			case MAX:
				return MaxOperator.deserialize(in, prev);
			case MIN:
				return MinOperator.deserialize(in, prev);
			case SUM:
				return SumOperator.deserialize(in, prev);
			default:
				throw new Exception("Unknown type in deserializeAgOp(): " + type);
		}
	}

	public static List<AggregateOperator> deserializeALAgOp(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<AggregateOperator>)readReference(in, prev);
		}

		if (!HrdbmsType.ALAGOP.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type 34 but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<AggregateOperator> retval = new ArrayList<AggregateOperator>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeAgOp(in, prev));
			i++;
		}

		return retval;
	}

	public static List<List<Boolean>> deserializeALALB(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<List<Boolean>>)readReference(in, prev);
		}

		if (!HrdbmsType.ALALB.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALALB but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<List<Boolean>> retval = new ArrayList<List<Boolean>>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeALB(in, prev));
			i++;
		}

		return retval;
	}

	public static List<List<Filter>> deserializeALALF(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<List<Filter>>)readReference(in, prev);
		}

		if (!HrdbmsType.ALALF.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALALF but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<List<Filter>> retval = new ArrayList<List<Filter>>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeALF(in, prev));
			i++;
		}

		return retval;
	}

	public static List<List<Object>> deserializeALALO(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<List<Object>>)readReference(in, prev);
		}

		if (!HrdbmsType.ALALO.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALALO but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<List<Object>> retval = new ArrayList<List<Object>>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeALO(in, prev));
			i++;
		}

		return retval;
	}

	public static List<List<String>> deserializeALALS(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<List<String>>)readReference(in, prev);
		}

		if (!HrdbmsType.ALALS.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALALS but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<List<String>> retval = new ArrayList<List<String>>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeALS(in, prev));
			i++;
		}

		return retval;
	}

	public static List<Boolean> deserializeALB(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Boolean>)readReference(in, prev);
		}

		if (!HrdbmsType.ALB.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALB but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Boolean> retval = new ArrayList<Boolean>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readBool(in));
			i++;
		}

		return retval;
	}

	public static List<Filter> deserializeALF(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Filter>)readReference(in, prev);
		}

		if (!HrdbmsType.ALF.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALF but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Filter> retval = new ArrayList<Filter>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(Filter.deserialize(in, prev)); // have not read type
			i++;
		}

		return retval;
	}

	public static List<Set<Map<Filter, Filter>>> deserializeALHSHM(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Set<Map<Filter, Filter>>>)readReference(in, prev);
		}

		if (HrdbmsType.ALHSHM.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALHSHM but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Set<Map<Filter, Filter>>> retval = new ArrayList<>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeHSHM(in, prev));
			i++;
		}

		return retval;
	}

	public static List<Integer> deserializeALI(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Integer>)readReference(in, prev);
		}

		if (!HrdbmsType.ALI.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALI but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Integer> retval = new ArrayList<Integer>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readInt(in));
			i++;
		}

		return retval;
	}

	public static List<Index> deserializeALIndx(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Index>)readReference(in, prev);
		}

		if (!HrdbmsType.ALINDX.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALINDX but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Index> retval = new ArrayList<Index>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(Index.deserialize(in, prev)); // has not read type
			i++;
		}

		return retval;
	}

	public static List<Object> deserializeALO(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Object>)readReference(in, prev);
		}

		if (!HrdbmsType.ALO.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALO but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Object> retval = new ArrayList<Object>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			type = getType(in);
			switch (type)
			{
				case REFERENCE:
					retval.add(readReference(in, prev));
					break;
				case STRING:
					retval.add(readStringKnown(in, prev));
					break;
				case DOUBLE:
					retval.add(readDoubleClassKnown(in));
					break;
				case LONG:
					retval.add(readLongClassKnown(in));
					break;
				case INTEGER:
					retval.add(readIntClassKnown(in));
					break;
				case MYDATE:
					retval.add(readDateKnown(in));
					break;
				case DOUBLENULL:
				case LONGNULL:
				case INTEGERNULL:
				case MYDATENULL:
					retval.add(null);
					break;
				default:
					throw new Exception("Unknown type when deserializing ArrayList<Object>: " + type);
			}

			i++;
		}

		return retval;
	}

	public static List<Operator> deserializeALOp(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<Operator>)readReference(in, prev);
		}

		if (!HrdbmsType.ALOP.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALOP but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<Operator> retval = new ArrayList<>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeOperator(in, prev));
			i++;
		}

		return retval;
	}

	public static List<RIDAndIndexKeys> deserializeALRAIK(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<RIDAndIndexKeys>)readReference(in, prev);
		}

		if (!HrdbmsType.ALRAIK.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALRAIK but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<RIDAndIndexKeys> retval = new ArrayList<>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(RIDAndIndexKeys.deserialize(in, prev)); // have not read
																// type
			i++;
		}

		return retval;
	}

	public static List<String> deserializeALS(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (List<String>)readReference(in, prev);
		}

		if (!HrdbmsType.ALS.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type ALS but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final List<String> retval = new ArrayList<>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readString(in, prev));
			i++;
		}

		return retval;
	}

	public static boolean[] deserializeBoolArray(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (boolean[])readReference(in, prev);
		}

		if (!HrdbmsType.BOOLARRAY.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type BOOLARRAY but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final boolean[] retval = new boolean[size];
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval[i] = readBool(in);
			i++;
		}

		return retval;
	}

	public static CNFFilter deserializeCNF(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (CNFFilter)readReference(in, prev);
		}

		if (HrdbmsType.CNFNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.CNF.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type CNF but received " + type);
		}

		return CNFFilter.deserializeKnown(in, prev); // already read type
	}

	public static Filter deserializeFilter(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Filter)readReference(in, prev);
		}

		if (HrdbmsType.FILTERNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.FILTER.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type FILTER but received " + type);
		}

		return Filter.deserializeKnown(in, prev); // type already read
	}

    public static HTTPCsvExternal deserializeCSVExternal(final InputStream in, final Map<Long, Object> prev) throws Exception
    {
        final HrdbmsType type = getType(in);
        if (HrdbmsType.REFERENCE.equals(type))
        {
            return (HTTPCsvExternal)readReference(in, prev);
        }
        if (!HrdbmsType.CSVEXTERNALTABLE.equals(type))
        {
            throw new Exception("Corrupted stream. Expected type EXTERNALTABLE but received " + type);
        }
        return HTTPCsvExternal.deserializeKnown(in, prev); // type already read
    }

    public static CsvExternalParams deserializeCSVExternalParams(final InputStream in, final Map<Long, Object> prev) throws Exception
    {
        final HrdbmsType type = getType(in);
        if (HrdbmsType.REFERENCE.equals(type))
        {
            return (CsvExternalParams)readReference(in, prev);
        }
        if (!HrdbmsType.CSVEXTERNALPARAMS.equals(type))
        {
            throw new Exception("Corrupted stream. Expected type EXTERNALTABLE but received " + type);
        }
        return CsvExternalParams.deserializeKnown(in, prev); // type already read
    }

	public static HDFSCsvExternal deserializeHDFSCsvExternal(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (HDFSCsvExternal)readReference(in, prev);
		}
		if (!HrdbmsType.HDFSCSVEXTERNALTABLE.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type EXTERNALTABLE but received " + type);
		}
		return HDFSCsvExternal.deserializeKnown(in, prev); // type already read
	}

    public static FastStringTokenizer deserializeFST(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (FastStringTokenizer)readReference(in, prev);
		}

		if (HrdbmsType.FSTNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.FST.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type FST but received " + type);
		}

		return FastStringTokenizer.deserializeKnown(in, prev); // already read
		// type
	}

	public static Map<Filter, Filter> deserializeHMF(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<Filter, Filter>)readReference(in, prev);
		}

		if (!HrdbmsType.HMF.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type 9 but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Map<Filter, Filter> retval = new HashMap<>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			final Filter f = Filter.deserialize(in, prev); // has not read type
															// yet
			retval.put(f, f);
			i++;
		}

		return retval;
	}

	public static Map<Integer, Operator> deserializeHMIntOp(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<Integer, Operator>)readReference(in, prev);
		}

		if (!HrdbmsType.HMINTOP.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HMINTOP but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Map<Integer, Operator> retval = new HashMap<Integer, Operator>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			final Integer key = readInt(in);
			final Operator value = deserializeOperator(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static Map<Operator, CNFFilter> deserializeHMOpCNF(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<Operator, CNFFilter>)readReference(in, prev);
		}

		if (!HrdbmsType.HMOPCNF.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HMOPCNF but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Map<Operator, CNFFilter> retval = new HashMap<>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			final Operator key = deserializeOperator(in, prev);
			final CNFFilter value = deserializeCNF(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static Set<Map<Filter, Filter>> deserializeHSHM(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Set<Map<Filter, Filter>>)readReference(in, prev);
		}

		if (!HrdbmsType.HSHM.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HSHM but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Set<Map<Filter, Filter>> retval = new HashSet<>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeHMF(in, prev));
			i++;
		}

		return retval;
	}

	public static Set<Object> deserializeHSO(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Set<Object>)readReference(in, prev);
		}

		if (!HrdbmsType.HSO.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HSO but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Set<Object> retval = new HashSet<Object>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			type = getType(in);
			switch (type)
			{
				case REFERENCE:
					retval.add(readReference(in, prev));
					break;
				case STRING:
					retval.add(readStringKnown(in, prev));
					break;
				case DOUBLE:
					retval.add(readDoubleClassKnown(in));
					break;
				case LONG:
					retval.add(readLongClassKnown(in));
					break;
				case INTEGER:
					retval.add(readIntClassKnown(in));
					break;
				case MYDATE:
					retval.add(readDateKnown(in));
					break;
				case DOUBLENULL:
				case LONGNULL:
				case INTEGERNULL:
				case MYDATENULL:
					retval.add(null);
					break;
				default:
					throw new Exception("Unknown type in HSO: " + type);
			}

			i++;
		}

		return retval;
	}

	public static Set<String> deserializeHSS(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Set<String>)readReference(in, prev);
		}

		if (!HrdbmsType.HSS.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HSS but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Set<String> retval = new HashSet<String>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readString(in, prev));
			i++;
		}

		return retval;
	}

	public static Index deserializeIndex(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Index)readReference(in, prev);
		}

		if (HrdbmsType.INDEXNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.INDEX.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type INDEX but received " + type);
		}

		return Index.deserializeKnown(in, prev);
	}

	public static int[] deserializeIntArray(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (int[])readReference(in, prev);
		}

		if (!HrdbmsType.INTARRAY.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type INTARRAY but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final int[] retval = new int[size];
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval[i] = readInt(in);
			i++;
		}

		return retval;
	}

	public static Map<Integer, Integer> deserializeMapIntInt(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<Integer, Integer>)readReference(in, prev);
		}

		if (!HrdbmsType.HMINTINT.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HMINTINT but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Map<Integer, Integer> retval = new HashMap<>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.put(readInt(in), readInt(in));
			i++;
		}

		return retval;
	}

	public static Operator deserializeOperator(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		switch (type)
		{
			case REFERENCE:
				return (Operator)readReference(in, prev);
			case ANTIJOIN:
				return AntiJoinOperator.deserialize(in, prev);
			case CASE:
				return CaseOperator.deserialize(in, prev);
			case CONCATE:
				return ConcatOperator.deserialize(in, prev);
			case DATEMATH:
				return DateMathOperator.deserialize(in, prev);
			case DEMOPERATOR:
				return DEMOperator.deserialize(in, prev);
			case DUMMYOPERATOR:
				return DummyOperator.deserialize(in, prev);
			case EXCEPT:
				return ExceptOperator.deserialize(in, prev);
			case EXTENDOBJECT:
				return ExtendObjectOperator.deserialize(in, prev);
			case EXTEND:
				return ExtendOperator.deserialize(in, prev);
			case INDEXOPERATOR:
				return IndexOperator.deserialize(in, prev);
			case INTERSECT:
				return IntersectOperator.deserialize(in, prev);
			case MULTI:
				return MultiOperator.deserialize(in, prev);
			case NRO:
				return NetworkReceiveOperator.deserialize(in, prev);
			case NSO:
				return NetworkSendOperator.deserialize(in, prev);
			case PROJECT:
				return ProjectOperator.deserialize(in, prev);
			case RENAME:
				return RenameOperator.deserialize(in, prev);
			case REORDER:
				return ReorderOperator.deserialize(in, prev);
			case ROOT:
				return RootOperator.deserialize(in, prev);
			case SELECT:
				return SelectOperator.deserialize(in, prev);
			case SORT:
				return SortOperator.deserialize(in, prev);
			case SUBSTRING:
				return SubstringOperator.deserialize(in, prev);
			case TOP:
				return TopOperator.deserialize(in, prev);
			case UNION:
				return UnionOperator.deserialize(in, prev);
			case YEAR:
				return YearOperator.deserialize(in, prev);
			case SJO:
				return SemiJoinOperator.deserialize(in, prev);
			case PRODUCT:
				return ProductOperator.deserialize(in, prev);
			case NL:
				return NestedLoopJoinOperator.deserialize(in, prev);
			case HJO:
				return HashJoinOperator.deserialize(in, prev);
			case NHAS:
				return NetworkHashAndSendOperator.deserialize(in, prev);
			case NHRAM:
				return NetworkHashReceiveAndMergeOperator.deserialize(in, prev);
			case NHRO:
				return NetworkHashReceiveOperator.deserialize(in, prev);
			case NRAM:
				return NetworkReceiveAndMergeOperator.deserialize(in, prev);
			case NSMO:
				return NetworkSendMultipleOperator.deserialize(in, prev);
			case NSRR:
				return NetworkSendRROperator.deserialize(in, prev);
			case TSO:
				return TableScanOperator.deserialize(in, prev);
			case ETSO:
				return ExternalTableScanOperator.deserialize(in, prev);
			case OPERATORNULL:
				return null;
			case ROUTING:
				return RoutingOperator.deserialize(in, prev);
			case LOADO:
				return LoadOperator.deserialize(in, prev);
			case LOADRO:
				return LoadReceiveOperator.deserialize(in, prev);
			default:
				throw new Exception("Unknown type in deserialize operator: " + type);
		}
	}

	public static String[] deserializeStringArray(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (String[])readReference(in, prev);
		}

		if (!HrdbmsType.STRINGARRAY.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type STRINGARRAY but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final String[] retval = new String[size];
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval[i] = readString(in, prev);
			i++;
		}

		return retval;
	}

	public static Map<String, String> deserializeStringHM(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<String, String>)readReference(in, prev);
		}

		if (!HrdbmsType.HMSTRINGSTRING.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type HMSTRINGSTRING, found type " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			// HRDBMSWorker.logger.debug("OU deserialized null String, String
			// HashMap");
			return null;
		}

		final int size = readShort(in);
		final Map<String, String> retval = new HashMap<String, String>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			final String key = readString(in, prev);
			final String value = readString(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static Map<String, Integer> deserializeStringIntHM(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<String, Integer>)readReference(in, prev);
		}

		if (!HrdbmsType.HMSTRINGINT.equals(type))
		{
			throw new Exception("Corrupted stream.  Expected type HMSTRINGINT but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Map<String, Integer> retval = new HashMap<String, Integer>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			final String key = readString(in, prev);
			final int value = readInt(in);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static Map<Integer, String> deserializeTM(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (Map<Integer, String>)readReference(in, prev);
		}

		if (!HrdbmsType.TREEMAP.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type TREEMAP but received " + type);
		}

		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final Map<Integer, String> retval = new TreeMap<Integer, String>();
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			final int key = readInt(in);
			final String value = readString(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static HrdbmsType getType(final InputStream in) throws Exception
	{
		return HrdbmsType.fromInt(in.read());
	}

	public static void read(final byte[] data, final InputStream in) throws Exception
	{
		int count = 0;
		while (count < data.length)
		{
			final int temp = in.read(data, count, data.length - count);
			if (temp == -1)
			{
				throw new Exception("Early EOF");
			}

			count += temp;
		}
	}

	public static boolean readBool(final InputStream in) throws Exception
	{
		final int val = in.read();
		return (val != 0);
	}

	public static Boolean readBoolClass(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.BOOLNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.BOOLCLASS.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type BOOLCLASS but received " + type);
		}

		return readBool(in);
	}

	public static MyDate readDate(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.MYDATENULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.MYDATE.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type MYDATE but received " + type);
		}

		final byte[] data = new byte[4];
		read(data, in);
		final int l = bytesToInt(data);
		return new MyDate(l);
	}

	public static MyDate readDateKnown(final InputStream in) throws Exception
	{
		final byte[] data = new byte[4];
		read(data, in);
		final int l = bytesToInt(data);
		return new MyDate(l);
	}

	public static Double readDoubleClass(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.DOUBLENULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.DOUBLE.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type DOUBLE but received " + type);
		}

		final byte[] data = new byte[8];
		read(data, in);
		final long l = bytesToLong(data);
		return Double.longBitsToDouble(l);
	}

	public static Double readDoubleClassKnown(final InputStream in) throws Exception
	{
		final byte[] data = new byte[8];
		read(data, in);
		final long l = bytesToLong(data);
		return Double.longBitsToDouble(l);
	}

	public static int readInt(final InputStream in) throws Exception
	{
		final byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static Integer readIntClass(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.INTEGERNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.INTEGER.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type INTEGER but received " + type);
		}

		final byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static Integer readIntClassKnown(final InputStream in) throws Exception
	{
		final byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static long readLong(final InputStream in) throws Exception
	{
		final byte[] data = new byte[8];
		read(data, in);
		return bytesToLong(data);
	}

	public static Long readLongClass(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.LONGNULL.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.LONG.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type LONG but received " + type);
		}

		final byte[] data = new byte[8];
		read(data, in);
		return bytesToLong(data);
	}

	public static Long readLongClassKnown(final InputStream in) throws Exception
	{
		final byte[] data = new byte[8];
		read(data, in);
		return bytesToLong(data);
	}

	public static Object readObject(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		switch (type)
		{
			case REFERENCE:
				return readReference(in, prev);
			case STRING:
				return readStringKnown(in, prev);
			case DOUBLE:
				return readDoubleClassKnown(in);
			case LONG:
				return readLongClassKnown(in);
			case INTEGER:
				return readIntClassKnown(in);
			case MYDATE:
				return readDateKnown(in);
			case DOUBLENULL:
			case LONGNULL:
			case INTEGERNULL:
			case MYDATENULL:
				return null;
			default:
				throw new Exception("Unexpected type in readObject(): " + type);
		}
	}

	public static Object readReference(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final long id = readLong(in);
		final Object obj = prev.get(id);
		if (obj == null)
		{
			throw new Exception("During deserialization we had an unresolved reference to ID = " + id);
		}

		return obj;
	}

	public static int readShort(final InputStream in) throws Exception
	{
		final byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static String readString(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final HrdbmsType type = getType(in);
		if (HrdbmsType.REFERENCE.equals(type))
		{
			return (String)readReference(in, prev);
		}

		if (HrdbmsType.NULLSTRING.equals(type))
		{
			return null;
		}

		if (!HrdbmsType.STRING.equals(type))
		{
			throw new Exception("Corrupted stream. Expected type STRING but received " + type);
		}

		final long id = readLong(in);
		final int size = readShort(in);
		final byte[] data = new byte[size];
		read(data, in);
		final String retval = new String(data, StandardCharsets.UTF_8);
		prev.put(id, retval);
		return retval;
	}

	public static String readStringKnown(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		final int size = readShort(in);
		final byte[] data = new byte[size];
		read(data, in);
		final String retval = new String(data, StandardCharsets.UTF_8);
		prev.put(id, retval);
		return retval;
	}

	public static void serializeADS(final Deque<String> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ADS, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ADS, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		final Iterator<String> iter = als.descendingIterator();
		while (iter.hasNext())
		{
			writeString(iter.next(), out, prev);
		}

		return;
	}

	public static void serializeALAgOp(final List<AggregateOperator> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALAGOP, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALAGOP, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final AggregateOperator entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALALB(final List<? extends List<Boolean>> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALALB, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALALB, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final List<Boolean> entry : als)
		{
			serializeALB(entry, out, prev);
		}

		return;
	}

	public static void serializeALALF(final List<? extends List<Filter>> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALALF, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALALF, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final List<Filter> entry : als)
		{
			serializeALF(entry, out, prev);
		}

		return;
	}

	public static void serializeALALO(final List<? extends List<Object>> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALALO, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALALO, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final List<Object> entry : als)
		{
			serializeALO(entry, out, prev);
		}

		return;
	}

	public static void serializeALALS(final List<? extends List<String>> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALALS, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALALS, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final List<String> entry : als)
		{
			serializeALS(entry, out, prev);
		}

		return;
	}

	public static void serializeALB(final List<Boolean> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALB, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALB, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Boolean entry : als)
		{
			writeBool(entry, out);
		}

		return;
	}

	public static void serializeALF(final List<Filter> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALF, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALF, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Filter entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALHSHM(final List<? extends Set<? extends Map<Filter, Filter>>> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALHSHM, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALHSHM, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Set<? extends Map<Filter, Filter>> entry : als)
		{
			serializeHSHM(entry, out, prev);
		}

		return;
	}

	public static void serializeALI(final List<Integer> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALI, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALI, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Integer entry : als)
		{
			writeInt(entry, out);
		}

		return;
	}

	public static void serializeALIndx(final List<Index> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALINDX, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALINDX, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Index entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALO(final List<Object> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALO, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALO, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Object entry : als)
		{
			if (entry instanceof String)
			{
				writeString((String)entry, out, prev);
			}
			else if (entry instanceof Double)
			{
				writeDoubleClass((Double)entry, out, prev);
			}
			else if (entry instanceof Long)
			{
				writeLongClass((Long)entry, out, prev);
			}
			else if (entry instanceof Integer)
			{
				writeIntClass((Integer)entry, out, prev);
			}
			else if (entry instanceof MyDate)
			{
				writeDate((MyDate)entry, out, prev);
			}
			else
			{
				throw new Exception("Unknown type " + entry.getClass() + " in OperatorUtils.serializeALO()");
			}
		}

		return;
	}

	public static void serializeALOp(final List<Operator> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALOP, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALOP, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Operator entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALOp(final List<Operator> als, final OutputStream out, final IdentityHashMap<Object, Long> prev, final boolean flag) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALOP, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALOP, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Operator entry : als)
		{
			((NetworkSendOperator)entry).serialize(out, prev, false);
		}

		return;
	}

	public static void serializeALRAIK(final List<RIDAndIndexKeys> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALRAIK, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALRAIK, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final RIDAndIndexKeys entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALS(final List<String> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.ALS, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.ALS, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final String entry : als)
		{
			writeString(entry, out, prev);
		}

		return;
	}

	public static void serializeBoolArray(final boolean[] als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.BOOLARRAY, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.BOOLARRAY, out);
		prev.put(als, writeID(out));
		writeShort(als.length, out);
		for (final boolean entry : als)
		{
			writeBool(entry, out);
		}

		return;
	}

	/**
	 * Serialize CSV External Table Implementation
	 */
	public static void serializeCsvExternal(final HTTPCsvExternal csv, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (csv == null)
		{
			writeType(HrdbmsType.CSVEXTERNALTABLE, out);
			return;
		}
		csv.serialize(out, prev);
	}

    /**
     * Serialize CSV External Table Implementation
     */
    public static void serializeHDFSCsvExternal(final HDFSCsvExternal csv, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
    {
        if (csv == null)
        {
            writeType(HrdbmsType.HDFSCSVEXTERNALTABLE, out);
            return;
        }
        csv.serialize(out, prev);
    }

	/**
	 * Serialize CSV External Table Parameters
	 */
	public static void serializeCSVExternalParams(final CsvExternalParams params, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (params == null)
		{
			writeType(HrdbmsType.CSVEXTERNALPARAMS, out);
			return;
		}
		params.serialize(out, prev);
	}

	public static void serializeCNF(final CNFFilter d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.CNFNULL, out);
			return;
		}

		d.serialize(out, prev);
	}

	public static void serializeFilter(final Filter als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.FILTERNULL, out);
			return;
		}

		als.serialize(out, prev);
	}

	public static void serializeFST(final FastStringTokenizer d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.FSTNULL, out);
			return;
		}

		d.serialize(out, prev);
	}

	public static void serializeHMF(final Map<Filter, Filter> hmf, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hmf == null)
		{
			writeType(HrdbmsType.HMF, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hmf);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HMF, out);
		prev.put(hmf, writeID(out));
		writeShort(hmf.size(), out);
		for (final Filter entry : hmf.keySet())
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeHMIntOp(final Map<Integer, Operator> hm, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(HrdbmsType.HMINTOP, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HMINTOP, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (final Map.Entry<Integer, Operator> entry : hm.entrySet())
		{
			final Integer key = entry.getKey();
			final Operator value = entry.getValue();

			writeInt(key, out);
			value.serialize(out, prev);
		}

		return;
	}

	public static void serializeHMOpCNF(final Map<Operator, CNFFilter> hm, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(HrdbmsType.HMOPCNF, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HMOPCNF, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (final Map.Entry<Operator, CNFFilter> entry : hm.entrySet())
		{
			final Operator key = entry.getKey();
			final CNFFilter value = entry.getValue();

			key.serialize(out, prev);
			value.serialize(out, prev);
		}

		return;
	}

	public static void serializeHSHM(final Set<? extends Map<Filter, Filter>> hshm, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hshm == null)
		{
			writeType(HrdbmsType.HSHM, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hshm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HSHM, out);
		prev.put(hshm, writeID(out));
		writeShort(hshm.size(), out);
		for (final Map<Filter, Filter> entry : hshm)
		{
			serializeHMF(entry, out, prev);
		}

		return;
	}

	public static void serializeHSO(final Set<Object> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.HSO, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HSO, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final Object entry : als)
		{
			if (entry instanceof String)
			{
				writeString((String)entry, out, prev);
			}
			else if (entry instanceof Double)
			{
				writeDoubleClass((Double)entry, out, prev);
			}
			else if (entry instanceof Long)
			{
				writeLongClass((Long)entry, out, prev);
			}
			else if (entry instanceof Integer)
			{
				writeIntClass((Integer)entry, out, prev);
			}
			else if (entry instanceof MyDate)
			{
				writeDate((MyDate)entry, out, prev);
			}
			else
			{
				throw new Exception("Unknown type " + entry.getClass() + " in OperatorUtils.serializeHSO()");
			}
		}

		return;
	}

	public static void serializeHSS(final Set<String> als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.HSS, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HSS, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (final String entry : als)
		{
			writeString(entry, out, prev);
		}

		return;
	}

	public static void serializeIndex(final Index i, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (i == null)
		{
			writeType(HrdbmsType.INDEXNULL, out);
			return;
		}
		else
		{
			i.serialize(out, prev);
		}
	}

	public static void serializeIntArray(final int[] als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.INTARRAY, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.INTARRAY, out);
		prev.put(als, writeID(out));
		writeShort(als.length, out);
		for (final int entry : als)
		{
			writeInt(entry, out);
		}

		return;
	}

	public static void serializeMapIntInt(final Map<Integer, Integer> map, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (map == null)
		{
			writeType(HrdbmsType.HMINTINT, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(map);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HMINTINT, out);
		prev.put(map, writeID(out));
		writeShort(map.size(), out);
		for (final Map.Entry<Integer, Integer> entry : map.entrySet())
		{
			writeInt(entry.getKey(), out);
			writeInt(entry.getValue(), out);
		}

		return;
	}

	public static void serializeOperator(final Operator op, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (op == null)
		{
			writeType(HrdbmsType.OPERATORNULL, out);
			return;
		}
		else
		{
			op.serialize(out, prev);
		}
	}

	public static void serializeReference(final long id, final OutputStream out) throws Exception
	{
		writeType(HrdbmsType.REFERENCE, out);
		writeLong(id, out);
	}

	public static void serializeStringArray(final String[] als, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(HrdbmsType.STRINGARRAY, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.STRINGARRAY, out);
		prev.put(als, writeID(out));
		writeShort(als.length, out);
		for (final String entry : als)
		{
			writeString(entry, out, prev);
		}

		return;
	}

	public static void serializeStringHM(final Map<String, String> hm, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(HrdbmsType.HMSTRINGSTRING, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HMSTRINGSTRING, out);
		prev.put(hm, writeID(out));

		writeShort(hm.size(), out);
		for (final Map.Entry<String, String> entry : hm.entrySet())
		{
			final String key = entry.getKey();
			final String value = entry.getValue();

			writeString(key, out, prev);
			writeString(value, out, prev);
		}

		return;
	}

	public static void serializeStringIntHM(final Map<String, Integer> hm, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(HrdbmsType.HMSTRINGINT, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.HMSTRINGINT, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (final Map.Entry<String, Integer> entry : hm.entrySet())
		{
			final String key = entry.getKey();
			final int value = entry.getValue();

			writeString(key, out, prev);
			writeInt(value, out);
		}

		return;
	}

	public static void serializeTM(final Map<Integer, String> hm, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(HrdbmsType.TREEMAP, out);
			writeLong(-1, out);
			return;
		}

		final Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.TREEMAP, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (final Map.Entry<Integer, String> entry : hm.entrySet())
		{
			final String value = entry.getValue();
			final int key = entry.getKey();

			writeInt(key, out);
			writeString(value, out, prev);
		}

		return;
	}

	public static void writeBool(final boolean b, final OutputStream out) throws Exception
	{
		if (b)
		{
			out.write(1);
		}
		else
		{
			out.write(0);
		}
	}

	public static void writeBoolClass(final Boolean d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.BOOLNULL, out);
			return;
		}

		writeType(HrdbmsType.BOOLCLASS, out);
		writeBool(d, out);
		return;
	}

	public static void writeDate(final MyDate d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.MYDATENULL, out);
			return;
		}

		writeType(HrdbmsType.MYDATE, out);
		out.write(intToBytes(d.getTime()));
		return;
	}

	public static void writeDoubleClass(final Double d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.DOUBLENULL, out);
			return;
		}

		writeType(HrdbmsType.DOUBLE, out);
		out.write(longToBytes(Double.doubleToRawLongBits(d)));
		return;
	}

	public static long writeID(final OutputStream out) throws Exception
	{
		final long retval = id.incrementAndGet();
		writeLong(retval, out);
		return retval;
	}

	public static void writeInt(final int i, final OutputStream out) throws Exception
	{
		out.write(intToBytes(i));
	}

	public static void writeIntClass(final Integer d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.INTEGERNULL, out);
			return;
		}

		writeType(HrdbmsType.INTEGER, out);
		out.write(intToBytes(d));
		return;
	}

	public static void writeLong(final long l, final OutputStream out) throws Exception
	{
		out.write(longToBytes(l));
	}

	public static void writeLongClass(final Long d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(HrdbmsType.LONGNULL, out);
			return;
		}

		writeType(HrdbmsType.LONG, out);
		out.write(longToBytes(d));
		return;
	}

	public static void writeObject(final Object d, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d instanceof String)
		{
			writeString((String)d, out, prev);
		}
		else if (d instanceof Double)
		{
			writeDoubleClass((Double)d, out, prev);
		}
		else if (d instanceof Long)
		{
			writeLongClass((Long)d, out, prev);
		}
		else if (d instanceof Integer)
		{
			writeIntClass((Integer)d, out, prev);
		}
		else if (d instanceof MyDate)
		{
			writeDate((MyDate)d, out, prev);
		}
		else
		{
			throw new Exception("Unknown type " + d.getClass() + " in OperatorUtils.writeObject()");
		}

		return;
	}

	public static void writeShort(final int i, final OutputStream out) throws Exception // now
																						// writes
																						// int
	{
		out.write(intToBytes(i));
	}

	public static void writeString(final String s, final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (s == null)
		{
			writeType(HrdbmsType.NULLSTRING, out);
			return;
		}

		final Long id = prev.get(s);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(HrdbmsType.STRING, out);
		prev.put(s, writeID(out));
		final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		writeShort(bytes.length, out);
		out.write(bytes);
		return;
	}

	public static void writeType(final HrdbmsType type, final OutputStream out) throws Exception
	{
		out.write(type.ordinal());
	}

	public static byte[] intToBytes(final int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	public static byte[] longToBytes(final long val)
	{
		final byte[] buff = new byte[8];
		buff[0] = (byte)(val >> 56);
		buff[1] = (byte)((val & 0x00FF000000000000L) >> 48);
		buff[2] = (byte)((val & 0x0000FF0000000000L) >> 40);
		buff[3] = (byte)((val & 0x000000FF00000000L) >> 32);
		buff[4] = (byte)((val & 0x00000000FF000000L) >> 24);
		buff[5] = (byte)((val & 0x0000000000FF0000L) >> 16);
		buff[6] = (byte)((val & 0x000000000000FF00L) >> 8);
		buff[7] = (byte)((val & 0x00000000000000FFL));
		return buff;
	}

	public static byte[] stringToBytes(final String string) {
		byte[] data = null;
		try
		{
			data = string.getBytes(StandardCharsets.UTF_8);
		}
		catch (final Exception e)
		{
		}
		final byte[] len = intToBytes(data.length);
		final byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}

	public static byte[] rsToBytes(final List<List<Object>> rows) throws Exception
	{
		final ByteBuffer[] results = new ByteBuffer[rows.size()];
		int rIndex = 0;
		final List<byte[]> bytes = new ArrayList<byte[]>();
		for (final List<Object> val : rows)
		{
			bytes.clear();
			int size = val.size() + 8;
			final byte[] header = new byte[size];
			int i = 8;
			for (final Object o : val)
			{
				if (o instanceof Long)
				{
					header[i] = (byte)0;
					size += 8;
				}
				else if (o instanceof Integer)
				{
					header[i] = (byte)1;
					size += 4;
				}
				else if (o instanceof Double)
				{
					header[i] = (byte)2;
					size += 8;
				}
				else if (o instanceof MyDate)
				{
					header[i] = (byte)3;
					size += 4;
				}
				else if (o instanceof String)
				{
					header[i] = (byte)4;
					final byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
					size += (4 + b.length);
					bytes.add(b);
				}
				// else if (o instanceof AtomicLong)
				// {
				// header[i] = (byte)6;
				// size += 8;
				// }
				// else if (o instanceof AtomicBigDecimal)
				// {
				// header[i] = (byte)7;
				// size += 8;
				// }
				else if (o instanceof ArrayList)
				{
					if (((List)o).size() != 0)
					{
						final Exception e = new Exception("Non-zero size ArrayList in toBytes()");
						throw e;
					}
					header[i] = (byte)8;
				}
				else
				{
					throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
				}

				i++;
			}

			final byte[] retval = new byte[size];
			// System.out.println("In toBytes(), row has " + val.size() +
			// " columns, object occupies " + size + " bytes");
			System.arraycopy(header, 0, retval, 0, header.length);
			i = 8;
			final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
			retvalBB.putInt(size - 4);
			retvalBB.putInt(val.size());
			retvalBB.position(header.length);
			int bOff = 0;
			for (final Object o : val)
			{
				if (retval[i] == 0)
				{
					retvalBB.putLong((Long)o);
					// if ((Long)o < 0)
					// {
					// HRDBMSWorker.logger.debug("Negative long value in
					// rsToBytes: "
					// + o);
					// }
				}
				else if (retval[i] == 1)
				{
					retvalBB.putInt((Integer)o);
					// if ((Integer)o < 0)
					// {
					// HRDBMSWorker.logger.debug("Negative int value in
					// rsToBytes: "
					// + o);
					// }
				}
				else if (retval[i] == 2)
				{
					retvalBB.putDouble((Double)o);
				}
				else if (retval[i] == 3)
				{
					retvalBB.putInt(((MyDate)o).getTime());
				}
				else if (retval[i] == 4)
				{
					final byte[] temp = bytes.get(bOff++);
					retvalBB.putInt(temp.length);
					retvalBB.put(temp);
				}
				// else if (retval[i] == 6)
				// {
				// retvalBB.putLong(((AtomicLong)o).get());
				// }
				// else if (retval[i] == 7)
				// {
				// retvalBB.putDouble(((AtomicBigDecimal)o).get().doubleValue());
				// }
				else if (retval[i] == 8)
				{
				}

				i++;
			}

			results[rIndex++] = retvalBB;
		}

		int count = 0;
		for (final ByteBuffer bb : results)
		{
			count += bb.capacity();
		}
		final byte[] retval = new byte[count + 4];
		final ByteBuffer temp = ByteBuffer.allocate(4);
		temp.asIntBuffer().put(results.length);
		System.arraycopy(temp.array(), 0, retval, 0, 4);
		int retvalPos = 4;
		for (final ByteBuffer bb : results)
		{
			final byte[] ba = bb.array();
			System.arraycopy(ba, 0, retval, retvalPos, ba.length);
			retvalPos += ba.length;
		}

		return retval;
	}

	/** Reads an ack from the passed socket */
	public static void getConfirmation(final Socket sock) throws Exception
	{
		final InputStream in = sock.getInputStream();
		final byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					in.close();
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				in.close();
				throw new Exception();
			}
		}

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}

		try
		{
			in.close();
		}
		catch (final Exception e)
		{
		}
	}

	/** Arranges worker nodes into a tree */
	public static List<Object> makeTree(final List<Integer> nodes)
	{
		final int max = MAX_NEIGHBOR_NODES;
		if (nodes.size() <= max)
		{
			final List<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}

		final List<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < max)
		{
			retval.add(nodes.get(i));
			i++;
		}

		final int remaining = nodes.size() - i;
		final int perNode = remaining / max + 1;

		int j = 0;
		final int size = nodes.size();
		while (i < size)
		{
			final int first = (Integer)retval.get(j);
			retval.remove(j);
			final List<Integer> list = new ArrayList<Integer>(perNode + 1);
			list.add(first);
			int k = 0;
			while (k < perNode && i < size)
			{
				list.add(nodes.get(i));
				i++;
				k++;
			}

			retval.add(j, list);
			j++;
		}

		if (((List<Integer>)retval.get(0)).size() <= max)
		{
			return retval;
		}

		// more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			final List<Integer> list = (List<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}

		return retval;
	}
}