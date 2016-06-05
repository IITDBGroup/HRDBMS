package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MyDate;

public class OperatorUtils
{
	private static AtomicLong id = new AtomicLong(0);

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

	public static int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	public static long bytesToLong(byte[] val)
	{
		final long ret = java.nio.ByteBuffer.wrap(val).getLong();
		return ret;
	}

	public static short bytesToShort(byte[] val)
	{
		final short ret = java.nio.ByteBuffer.wrap(val).getShort();
		return ret;
	}

	public static ArrayDeque<String> deserializeADS(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayDeque<String>)readReference(in, prev);
		}

		if (type != 29)
		{
			throw new Exception("Corrupted stream. Expected type 29 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayDeque<String> retval = new ArrayDeque<String>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.push(readString(in, prev));
			i++;
		}

		return retval;
	}

	public static AggregateOperator deserializeAgOp(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (AggregateOperator)readReference(in, prev);
		}

		if (type == 51)
		{
			return AvgOperator.deserialize(in, prev);
		}

		if (type == 52)
		{
			return CountDistinctOperator.deserialize(in, prev);
		}

		if (type == 53)
		{
			return CountOperator.deserialize(in, prev);
		}

		if (type == 54)
		{
			return MaxOperator.deserialize(in, prev);
		}

		if (type == 55)
		{
			return MinOperator.deserialize(in, prev);
		}

		if (type == 56)
		{
			return SumOperator.deserialize(in, prev);
		}

		throw new Exception("Unknown type in deserializeAgOp(): " + type);
	}

	public static ArrayList<AggregateOperator> deserializeALAgOp(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<AggregateOperator>)readReference(in, prev);
		}

		if (type != 34)
		{
			throw new Exception("Corrupted stream. Expected type 34 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<AggregateOperator> retval = new ArrayList<AggregateOperator>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeAgOp(in, prev));
			i++;
		}

		return retval;
	}

	public static ArrayList<ArrayList<Filter>> deserializeALALF(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<ArrayList<Filter>>)readReference(in, prev);
		}

		if (type != 67)
		{
			throw new Exception("Corrupted stream. Expected type 67 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<ArrayList<Filter>> retval = new ArrayList<ArrayList<Filter>>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeALF(in, prev));
			i++;
		}

		return retval;
	}

	public static ArrayList<Boolean> deserializeALB(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<Boolean>)readReference(in, prev);
		}

		if (type != 45)
		{
			throw new Exception("Corrupted stream. Expected type 45 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<Boolean> retval = new ArrayList<Boolean>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readBool(in));
			i++;
		}

		return retval;
	}

	public static ArrayList<Filter> deserializeALF(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<Filter>)readReference(in, prev);
		}

		if (type != 42)
		{
			throw new Exception("Corrupted stream. Expected type 42 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<Filter> retval = new ArrayList<Filter>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(Filter.deserialize(in, prev)); // have not read type
			i++;
		}

		return retval;
	}

	public static ArrayList<HashSet<HashMap<Filter, Filter>>> deserializeALHSHM(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<HashSet<HashMap<Filter, Filter>>>)readReference(in, prev);
		}

		if (type != 11)
		{
			throw new Exception("Corrupted stream. Expected type 11 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<HashSet<HashMap<Filter, Filter>>> retval = new ArrayList<HashSet<HashMap<Filter, Filter>>>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeHSHM(in, prev));
			i++;
		}

		return retval;
	}

	public static ArrayList<Integer> deserializeALI(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<Integer>)readReference(in, prev);
		}

		if (type != 7)
		{
			throw new Exception("Corrupted stream. Expected type 7 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<Integer> retval = new ArrayList<Integer>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readInt(in));
			i++;
		}

		return retval;
	}

	public static ArrayList<Index> deserializeALIndx(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<Index>)readReference(in, prev);
		}

		if (type != 10)
		{
			throw new Exception("Corrupted stream. Expected type 10 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<Index> retval = new ArrayList<Index>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(Index.deserialize(in, prev)); // has not read type
			i++;
		}

		return retval;
	}

	public static ArrayList<Object> deserializeALO(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<Object>)readReference(in, prev);
		}

		if (type != 12)
		{
			throw new Exception("Corrupted stream. Expected type 12 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<Object> retval = new ArrayList<Object>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			type = getType(in);
			switch (type)
			{
				case 0:
					retval.add(readReference(in, prev));
					break;
				case 3:
					retval.add(readStringKnown(in, prev));
					break;
				case 13:
					retval.add(readDoubleClassKnown(in));
					break;
				case 15:
					retval.add(readLongClassKnown(in));
					break;
				case 17:
					retval.add(readIntClassKnown(in));
					break;
				case 19:
					retval.add(readDateKnown(in));
					break;
				case 12:
				case 14:
				case 16:
				case 18:
					retval.add(null);
					break;
				default:
					throw new Exception("Unknown type when deserializing ArrayList<Object>: " + type);
			}

			i++;
		}

		return retval;
	}

	public static ArrayList<Operator> deserializeALOp(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<Operator>)readReference(in, prev);
		}

		if (type != 32)
		{
			throw new Exception("Corrupted stream. Expected type 32 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<Operator> retval = new ArrayList<Operator>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeOperator(in, prev));
			i++;
		}

		return retval;
	}

	public static ArrayList<String> deserializeALS(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (ArrayList<String>)readReference(in, prev);
		}

		if (type != 6)
		{
			throw new Exception("Corrupted stream. Expected type 6 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		ArrayList<String> retval = new ArrayList<String>(size);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readString(in, prev));
			i++;
		}

		return retval;
	}

	public static CNFFilter deserializeCNF(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (CNFFilter)readReference(in, prev);
		}

		if (type == 61)
		{
			return null;
		}

		if (type != 63)
		{
			throw new Exception("Corrupted stream. Expected type 63 but received " + type);
		}

		return CNFFilter.deserializeKnown(in, prev); // already read type
	}

	public static Filter deserializeFilter(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (Filter)readReference(in, prev);
		}

		if (type == 69)
		{
			return null;
		}

		if (type != 65)
		{
			throw new Exception("Corrupted stream. Expected type 65 but received " + type);
		}

		return Filter.deserializeKnown(in, prev); // type already read
	}

	public static FastStringTokenizer deserializeFST(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (FastStringTokenizer)readReference(in, prev);
		}

		if (type == 28)
		{
			return null;
		}

		if (type != 62)
		{
			throw new Exception("Corrupted stream. Expected type 62 but received " + type);
		}

		return FastStringTokenizer.deserializeKnown(in, prev); // already read
		// type
	}

	public static HashMap<Filter, Filter> deserializeHMF(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashMap<Filter, Filter>)readReference(in, prev);
		}

		if (type != 9)
		{
			throw new Exception("Corrupted stream. Expected type 9 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashMap<Filter, Filter> retval = new HashMap<Filter, Filter>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			Filter f = Filter.deserialize(in, prev); // has not read type yet
			retval.put(f, f);
			i++;
		}

		return retval;
	}

	public static HashMap<Integer, Operator> deserializeHMIntOp(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashMap<Integer, Operator>)readReference(in, prev);
		}

		if (type != 80)
		{
			throw new Exception("Corrupted stream. Expected type 80 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashMap<Integer, Operator> retval = new HashMap<Integer, Operator>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			Integer key = readInt(in);
			Operator value = deserializeOperator(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static HashMap<Operator, CNFFilter> deserializeHMOpCNF(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashMap<Operator, CNFFilter>)readReference(in, prev);
		}

		if (type != 79)
		{
			throw new Exception("Corrupted stream. Expected type 79 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashMap<Operator, CNFFilter> retval = new HashMap<Operator, CNFFilter>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			Operator key = deserializeOperator(in, prev);
			CNFFilter value = deserializeCNF(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static HashSet<HashMap<Filter, Filter>> deserializeHSHM(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashSet<HashMap<Filter, Filter>>)readReference(in, prev);
		}

		if (type != 8)
		{
			throw new Exception("Corrupted stream. Expected type 8 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashSet<HashMap<Filter, Filter>> retval = new HashSet<HashMap<Filter, Filter>>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(deserializeHMF(in, prev));
			i++;
		}

		return retval;
	}

	public static HashSet<Object> deserializeHSO(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashSet<Object>)readReference(in, prev);
		}

		if (type != 43)
		{
			throw new Exception("Corrupted stream. Expected type 43 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashSet<Object> retval = new HashSet<Object>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			type = getType(in);
			switch (type)
			{
				case 0:
					retval.add(readReference(in, prev));
					break;
				case 3:
					retval.add(readStringKnown(in, prev));
					break;
				case 13:
					retval.add(readDoubleClassKnown(in));
					break;
				case 15:
					retval.add(readLongClassKnown(in));
					break;
				case 17:
					retval.add(readIntClassKnown(in));
					break;
				case 19:
					retval.add(readDateKnown(in));
					break;
				case 12:
				case 14:
				case 16:
				case 18:
					retval.add(null);
					break;
				default:
					throw new Exception("Unknown type in HSO: " + type);
			}

			i++;
		}

		return retval;
	}

	public static HashSet<String> deserializeHSS(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashSet<String>)readReference(in, prev);
		}

		if (type != 68)
		{
			throw new Exception("Corrupted stream. Expected type 68 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashSet<String> retval = new HashSet<String>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval.add(readString(in, prev));
			i++;
		}

		return retval;
	}

	public static Index deserializeIndex(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (Index)readReference(in, prev);
		}

		if (type == 82)
		{
			return null;
		}

		if (type != 64)
		{
			throw new Exception("Corrupted stream. Expected type 64 but received " + type);
		}

		return Index.deserializeKnown(in, prev);
	}

	public static int[] deserializeIntArray(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (int[])readReference(in, prev);
		}

		if (type != 46)
		{
			throw new Exception("Corrupted stream. Expected type 46 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		int[] retval = new int[size];
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval[i] = readInt(in);
			i++;
		}

		return retval;
	}

	public static Operator deserializeOperator(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		switch (type)
		{
			// 0 - reference
			case 0:
				return (Operator)readReference(in, prev);
			// 1 - AntiJoin
			case 1:
				return AntiJoinOperator.deserialize(in, prev);
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
			case 20:
				return CaseOperator.deserialize(in, prev);
			// 21 - Concat
			case 21:
				return ConcatOperator.deserialize(in, prev);
			// 22 - DateMath
			case 22:
				return DateMathOperator.deserialize(in, prev);
			// 23 - DEMOperator
			case 23:
				return DEMOperator.deserialize(in, prev);
			// 24 - DummyOperator
			case 24:
				return DummyOperator.deserialize(in, prev);
			// 25 - Except
			case 25:
				return ExceptOperator.deserialize(in, prev);
			// 26 - ExtendObject
			case 26:
				return ExtendObjectOperator.deserialize(in, prev);
			// 27 - Extend
			case 27:
				return ExtendOperator.deserialize(in, prev);
			// 28 - FST null
			// 29 - ADS
			// 30 - IndexOperator
			case 30:
				return IndexOperator.deserialize(in, prev);
			// 31 - Intersect
			case 31:
				return IntersectOperator.deserialize(in, prev);
			// 32 - ALOp
			// 33 - Multi
			case 33:
				return MultiOperator.deserialize(in, prev);
			// 34 - ALAgOp
			// 35 - NRO
			case 35:
				return NetworkReceiveOperator.deserialize(in, prev);
			// 36 - NSO
			case 36:
				return NetworkSendOperator.deserialize(in, prev);
			// 37 - Project
			case 37:
				return ProjectOperator.deserialize(in, prev);
			// 38 - Rename
			case 38:
				return RenameOperator.deserialize(in, prev);
			// 39 - Reorder
			case 39:
				return ReorderOperator.deserialize(in, prev);
			// 40 - Root
			case 40:
				return RootOperator.deserialize(in, prev);
			// 41 - Select
			case 41:
				return SelectOperator.deserialize(in, prev);
			// 42 - ALF
			// 43 - HSO
			// 44 - Sort
			case 44:
				return SortOperator.deserialize(in, prev);
			// 45 - ALB
			// 46 - IntArray
			// 47 - Substring
			case 47:
				return SubstringOperator.deserialize(in, prev);
			// 48 - Top
			case 48:
				return TopOperator.deserialize(in, prev);
			// 49 - Union
			case 49:
				return UnionOperator.deserialize(in, prev);
			// 50 - Year
			case 50:
				return YearOperator.deserialize(in, prev);
			// 51 - Avg
			// 52 - CountDistinct
			// 53 - Count
			// 54 - Max
			// 55 - Min
			// 56 - Sum
			// 57 - SJO
			case 57:
				return SemiJoinOperator.deserialize(in, prev);
			// 58 - Product
			case 58:
				return ProductOperator.deserialize(in, prev);
			// 59 - NL
			case 59:
				return NestedLoopJoinOperator.deserialize(in, prev);
			// 60 - HJO
			case 60:
				return HashJoinOperator.deserialize(in, prev);
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
			case 72:
				return NetworkHashAndSendOperator.deserialize(in, prev);
			// 73 - NHRAM
			case 73:
				return NetworkHashReceiveAndMergeOperator.deserialize(in, prev);
			// 74 - NHRO
			case 74:
				return NetworkHashReceiveOperator.deserialize(in, prev);
			// 75 - NRAM
			case 75:
				return NetworkReceiveAndMergeOperator.deserialize(in, prev);
			// 76 - NSMO
			case 76:
				return NetworkSendMultipleOperator.deserialize(in, prev);
			// 77 - NSRR
			case 77:
				return NetworkSendRROperator.deserialize(in, prev);
			// 78 - TSO
			case 78:
				return TableScanOperator.deserialize(in, prev);
			// 79 - HMOpCNF
			// 80 - HMIntOp
			case 81:
				return null;
			default:
				throw new Exception("Unknown type in deserialize operator: " + type);
		}
	}

	public static String[] deserializeStringArray(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (String[])readReference(in, prev);
		}

		if (type != 66)
		{
			throw new Exception("Corrupted stream. Expected type 66 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		String[] retval = new String[size];
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			retval[i] = readString(in, prev);
			i++;
		}

		return retval;
	}

	public static HashMap<String, String> deserializeStringHM(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashMap<String, String>)readReference(in, prev);
		}

		if (type != 2)
		{
			throw new Exception("Corrupted stream. Expected type 2, found type " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashMap<String, String> retval = new HashMap<String, String>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			String key = readString(in, prev);
			String value = readString(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static HashMap<String, Integer> deserializeStringIntHM(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (HashMap<String, Integer>)readReference(in, prev);
		}

		if (type != 4)
		{
			throw new Exception("Corrupted stream.  Expected type 4 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		HashMap<String, Integer> retval = new HashMap<String, Integer>(4 * size / 3 + 1);
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			String key = readString(in, prev);
			int value = readInt(in);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static TreeMap<Integer, String> deserializeTM(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (TreeMap<Integer, String>)readReference(in, prev);
		}

		if (type != 5)
		{
			throw new Exception("Corrupted stream. Expected type 5 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		TreeMap<Integer, String> retval = new TreeMap<Integer, String>();
		prev.put(id, retval);
		int i = 0;
		while (i < size)
		{
			int key = readInt(in);
			String value = readString(in, prev);
			retval.put(key, value);
			i++;
		}

		return retval;
	}

	public static int getType(InputStream in) throws Exception
	{
		return in.read();
	}

	public static void read(byte[] data, InputStream in) throws Exception
	{
		int count = 0;
		while (count < data.length)
		{
			int temp = in.read(data, count, data.length - count);
			if (temp == -1)
			{
				throw new Exception("Early EOF");
			}

			count += temp;
		}
	}

	public static boolean readBool(InputStream in) throws Exception
	{
		int val = in.read();
		return (val != 0);
	}

	public static Boolean readBoolClass(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 70)
		{
			return null;
		}

		if (type != 71)
		{
			throw new Exception("Corrupted stream. Expected type 71 but received " + type);
		}

		return readBool(in);
	}

	public static MyDate readDate(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 18)
		{
			return null;
		}

		if (type != 19)
		{
			throw new Exception("Corrupted stream. Expected type 19 but received " + type);
		}

		byte[] data = new byte[4];
		read(data, in);
		int l = bytesToInt(data);
		return new MyDate(l);
	}

	public static MyDate readDateKnown(InputStream in) throws Exception
	{
		byte[] data = new byte[4];
		read(data, in);
		int l = bytesToInt(data);
		return new MyDate(l);
	}

	public static Double readDoubleClass(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 12)
		{
			return null;
		}

		if (type != 13)
		{
			throw new Exception("Corrupted stream. Expected type 13 but received " + type);
		}

		byte[] data = new byte[8];
		read(data, in);
		long l = bytesToLong(data);
		return Double.longBitsToDouble(l);
	}

	public static Double readDoubleClassKnown(InputStream in) throws Exception
	{
		byte[] data = new byte[8];
		read(data, in);
		long l = bytesToLong(data);
		return Double.longBitsToDouble(l);
	}

	public static int readInt(InputStream in) throws Exception
	{
		byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static Integer readIntClass(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 16)
		{
			return null;
		}

		if (type != 17)
		{
			throw new Exception("Corrupted stream. Expected type 17 but received " + type);
		}

		byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static Integer readIntClassKnown(InputStream in) throws Exception
	{
		byte[] data = new byte[4];
		read(data, in);
		return bytesToInt(data);
	}

	public static long readLong(InputStream in) throws Exception
	{
		byte[] data = new byte[8];
		read(data, in);
		return bytesToLong(data);
	}

	public static Long readLongClass(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 14)
		{
			return null;
		}

		if (type != 15)
		{
			throw new Exception("Corrupted stream. Expected type 15 but received " + type);
		}

		byte[] data = new byte[8];
		read(data, in);
		return bytesToLong(data);
	}

	public static Long readLongClassKnown(InputStream in) throws Exception
	{
		byte[] data = new byte[8];
		read(data, in);
		return bytesToLong(data);
	}

	public static Object readObject(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		switch (type)
		{
			case 0:
				return readReference(in, prev);
			case 3:
				return readStringKnown(in, prev);
			case 13:
				return readDoubleClassKnown(in);
			case 15:
				return readLongClassKnown(in);
			case 17:
				return readIntClassKnown(in);
			case 19:
				return readDateKnown(in);
			case 12:
			case 14:
			case 16:
			case 18:
				return null;
			default:
				throw new Exception("Unexpected type in readObject(): " + type);
		}
	}

	public static Object readReference(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		long id = readLong(in);
		Object obj = prev.get(id);
		if (obj == null)
		{
			throw new Exception("During deserialization we had an unresolved reference to ID = " + id);
		}

		return obj;
	}

	public static short readShort(InputStream in) throws Exception
	{
		byte[] data = new byte[2];
		read(data, in);
		return bytesToShort(data);
	}

	public static String readString(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		int type = getType(in);
		if (type == 0)
		{
			return (String)readReference(in, prev);
		}

		if (type != 3)
		{
			throw new Exception("Corrupted stream. Expected type 3 but received " + type);
		}

		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		byte[] data = new byte[size];
		read(data, in);
		String retval = new String(data, StandardCharsets.UTF_8);
		prev.put(id, retval);
		return retval;
	}

	public static String readStringKnown(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		long id = readLong(in);
		if (id == -1)
		{
			return null;
		}

		int size = readShort(in);
		byte[] data = new byte[size];
		read(data, in);
		String retval = new String(data, StandardCharsets.UTF_8);
		prev.put(id, retval);
		return retval;
	}

	public static void serializeADS(ArrayDeque<String> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(29, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(29, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		Iterator<String> iter = als.descendingIterator();
		while (iter.hasNext())
		{
			writeString(iter.next(), out, prev);
		}

		return;
	}

	public static void serializeALAgOp(ArrayList<AggregateOperator> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(34, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(34, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (AggregateOperator entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALALF(ArrayList<ArrayList<Filter>> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(7, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(67, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (ArrayList<Filter> entry : als)
		{
			serializeALF(entry, out, prev);
		}

		return;
	}

	public static void serializeALB(ArrayList<Boolean> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(45, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(45, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Boolean entry : als)
		{
			writeBool(entry, out);
		}

		return;
	}

	public static void serializeALF(ArrayList<Filter> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(42, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(42, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Filter entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALHSHM(ArrayList<HashSet<HashMap<Filter, Filter>>> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(11, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(11, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (HashSet<HashMap<Filter, Filter>> entry : als)
		{
			serializeHSHM(entry, out, prev);
		}

		return;
	}

	public static void serializeALI(ArrayList<Integer> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(7, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(7, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Integer entry : als)
		{
			writeInt(entry, out);
		}

		return;
	}

	public static void serializeALIndx(ArrayList<Index> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(10, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(10, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Index entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALO(ArrayList<Object> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(12, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(12, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Object entry : als)
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

	public static void serializeALOp(ArrayList<Operator> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(32, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(32, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Operator entry : als)
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeALOp(ArrayList<Operator> als, OutputStream out, IdentityHashMap<Object, Long> prev, boolean flag) throws Exception
	{
		if (als == null)
		{
			writeType(32, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(32, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Operator entry : als)
		{
			((NetworkSendOperator)entry).serialize(out, prev, false);
		}

		return;
	}

	public static void serializeALS(ArrayList<String> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(6, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(6, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (String entry : als)
		{
			writeString(entry, out, prev);
		}

		return;
	}

	public static void serializeCNF(CNFFilter d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(61, out);
			return;
		}

		d.serialize(out, prev);
	}

	public static void serializeFilter(Filter als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(69, out);
			return;
		}

		als.serialize(out, prev);
	}

	public static void serializeFST(FastStringTokenizer d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(28, out);
			return;
		}

		d.serialize(out, prev);
	}

	public static void serializeHMF(HashMap<Filter, Filter> hmf, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hmf == null)
		{
			writeType(9, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hmf);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(9, out);
		prev.put(hmf, writeID(out));
		writeShort(hmf.size(), out);
		for (Filter entry : hmf.keySet())
		{
			entry.serialize(out, prev);
		}

		return;
	}

	public static void serializeHMIntOp(HashMap<Integer, Operator> hm, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(80, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(80, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (Map.Entry<Integer, Operator> entry : hm.entrySet())
		{
			Integer key = entry.getKey();
			Operator value = entry.getValue();

			writeInt(key, out);
			value.serialize(out, prev);
		}

		return;
	}

	public static void serializeHMOpCNF(HashMap<Operator, CNFFilter> hm, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(79, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(79, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (Map.Entry<Operator, CNFFilter> entry : hm.entrySet())
		{
			Operator key = entry.getKey();
			CNFFilter value = entry.getValue();

			key.serialize(out, prev);
			value.serialize(out, prev);
		}

		return;
	}

	public static void serializeHSHM(HashSet<HashMap<Filter, Filter>> hshm, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hshm == null)
		{
			writeType(8, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hshm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(8, out);
		prev.put(hshm, writeID(out));
		writeShort(hshm.size(), out);
		for (HashMap<Filter, Filter> entry : hshm)
		{
			serializeHMF(entry, out, prev);
		}

		return;
	}

	public static void serializeHSO(HashSet<Object> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(43, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(43, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (Object entry : als)
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

	public static void serializeHSS(HashSet<String> als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(68, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(68, out);
		prev.put(als, writeID(out));
		writeShort(als.size(), out);
		for (String entry : als)
		{
			writeString(entry, out, prev);
		}

		return;
	}

	public static void serializeIndex(Index i, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (i == null)
		{
			writeType(82, out);
			return;
		}
		else
		{
			i.serialize(out, prev);
		}
	}

	public static void serializeIntArray(int[] als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(46, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(46, out);
		prev.put(als, writeID(out));
		writeShort(als.length, out);
		for (int entry : als)
		{
			writeInt(entry, out);
		}

		return;
	}

	public static void serializeOperator(Operator op, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (op == null)
		{
			writeType(81, out);
			return;
		}
		else
		{
			op.serialize(out, prev);
		}
	}

	public static void serializeReference(long id, OutputStream out) throws Exception
	{
		writeType(0, out);
		writeLong(id, out);
	}

	public static void serializeStringArray(String[] als, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (als == null)
		{
			writeType(66, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(als);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(66, out);
		prev.put(als, writeID(out));
		writeShort(als.length, out);
		for (String entry : als)
		{
			writeString(entry, out, prev);
		}

		return;
	}

	public static void serializeStringHM(HashMap<String, String> hm, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(2, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(2, out);
		prev.put(hm, writeID(out));

		writeShort(hm.size(), out);
		for (Map.Entry<String, String> entry : hm.entrySet())
		{
			String key = entry.getKey();
			String value = entry.getValue();

			writeString(key, out, prev);
			writeString(value, out, prev);
		}

		return;
	}

	public static void serializeStringIntHM(HashMap<String, Integer> hm, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(4, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(4, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (Map.Entry<String, Integer> entry : hm.entrySet())
		{
			String key = entry.getKey();
			int value = entry.getValue();

			writeString(key, out, prev);
			writeInt(value, out);
		}

		return;
	}

	public static void serializeTM(TreeMap<Integer, String> hm, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (hm == null)
		{
			writeType(5, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(hm);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(5, out);
		prev.put(hm, writeID(out));
		writeShort(hm.size(), out);
		for (Map.Entry<Integer, String> entry : hm.entrySet())
		{
			String value = entry.getValue();
			int key = entry.getKey();

			writeInt(key, out);
			writeString(value, out, prev);
		}

		return;
	}

	public static void writeBool(boolean b, OutputStream out) throws Exception
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

	public static void writeBoolClass(Boolean d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(70, out);
			return;
		}

		writeType(71, out);
		writeBool(d, out);
		return;
	}

	public static void writeDate(MyDate d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(18, out);
			return;
		}

		writeType(19, out);
		out.write(intToBytes(d.getTime()));
		return;
	}

	public static void writeDoubleClass(Double d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(12, out);
			return;
		}

		writeType(13, out);
		out.write(longToBytes(Double.doubleToRawLongBits(d)));
		return;
	}

	public static long writeID(OutputStream out) throws Exception
	{
		long retval = id.incrementAndGet();
		writeLong(retval, out);
		return retval;
	}

	public static void writeInt(int i, OutputStream out) throws Exception
	{
		out.write(intToBytes(i));
	}

	public static void writeIntClass(Integer d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(16, out);
			return;
		}

		writeType(17, out);
		out.write(intToBytes(d));
		return;
	}

	public static void writeLong(long l, OutputStream out) throws Exception
	{
		out.write(longToBytes(l));
	}

	public static void writeLongClass(Long d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (d == null)
		{
			writeType(14, out);
			return;
		}

		writeType(15, out);
		out.write(longToBytes(d));
		return;
	}

	public static void writeObject(Object d, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
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

	public static void writeShort(int i, OutputStream out) throws Exception
	{
		out.write(shortToBytes((short)i));
	}

	public static void writeString(String s, OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		if (s == null)
		{
			writeType(3, out);
			writeLong(-1, out);
			return;
		}

		Long id = prev.get(s);
		if (id != null)
		{
			serializeReference(id, out);
			return;
		}

		writeType(3, out);
		prev.put(s, writeID(out));
		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		writeShort(bytes.length, out);
		out.write(bytes);
		return;
	}

	public static void writeType(int type, OutputStream out) throws Exception
	{
		out.write(type);
	}

	private static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	private static byte[] longToBytes(long val)
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

	private static byte[] shortToBytes(int val)
	{
		final byte[] buff = new byte[2];
		buff[0] = (byte)((val & 0x0000FF00) >> 8);
		buff[1] = (byte)((val & 0x000000FF));
		return buff;
	}
}
