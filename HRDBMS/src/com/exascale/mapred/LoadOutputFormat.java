package com.exascale.mapred;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LoadOutputFormat extends OutputFormat
{

	@Override
	public void checkOutputSpecs(final JobContext arg0) throws IOException, InterruptedException
	{
	}

	@Override
	public OutputCommitter getOutputCommitter(final TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		final String jobName = arg0.getJobName();
		final String tableName = jobName.substring(5);
		final String portString = arg0.getConfiguration().get("hrdbms.port");
		final String hrdbmsHome = arg0.getConfiguration().get("hrdbms.home");
		return new LoadOutputCommitter(tableName, portString, hrdbmsHome);
	}

	@Override
	public RecordWriter getRecordWriter(final TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		final String jobName = arg0.getJobName();
		final String tableName = jobName.substring(5);
		final String portString = arg0.getConfiguration().get("hrdbms.port");
		final String hrdbmsHome = arg0.getConfiguration().get("hrdbms.home");
		return LoadRecordWriter.get(tableName, portString, hrdbmsHome);
	}
}
