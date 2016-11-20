package com.exascale.mapred;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LoadOutputCommitter extends OutputCommitter
{
	private final LoadRecordWriter writer;

	public LoadOutputCommitter(final String table, final String portString, final String hrdbmsHome)
	{
		writer = LoadRecordWriter.get(table, portString, hrdbmsHome);
	}

	@Override
	public void abortTask(final TaskAttemptContext arg0) throws IOException
	{
	}

	@Override
	public void commitTask(final TaskAttemptContext arg0) throws IOException
	{
		if (writer.rows.size() != 0)
		{
			writer.flush();
		}

		if (writer.thread != null)
		{
			while (true)
			{
				try
				{
					writer.thread.join();
					if (!writer.thread.getOK())
					{
						throw new IOException();
					}
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
		}
	}

	@Override
	public boolean needsTaskCommit(final TaskAttemptContext arg0) throws IOException
	{
		return true;
	}

	@Override
	public void setupJob(final JobContext arg0) throws IOException
	{
	}

	@Override
	public void setupTask(final TaskAttemptContext arg0) throws IOException
	{
	}
}
