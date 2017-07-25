package com.exascale.optimizer;

import java.io.OutputStream;
import java.util.*;

import com.exascale.threads.ThreadPoolThread;

public interface AggregateOperator
{
	public AggregateOperator clone();

	public AggregateResultThread getHashThread(Map<String, Integer> cols2Pos) throws Exception;

	public String getInputColumn();

	public AggregateResultThread newProcessingThread(List<List<Object>> rows, Map<String, Integer> cols2Pos);

	public String outputColumn();

	public String outputType();

	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception;

	public void setInput(String col);

	public void setInputColumn(String col);

	public void setNumGroups(long groups);

	public abstract class AggregateResultThread extends ThreadPoolThread
	{
		public void close()
		{
		}

		public Object getResult()
		{
			return null;
		}

		public Object getResult(final List<Object> keys) throws Exception
		{
			return null;
		}

		public void put(final List<Object> row, final List<Object> groupKeys) throws Exception
		{
		}

		@Override
		public void run()
		{
		}
	}
}
