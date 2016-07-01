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
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException
	{
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		String jobName = arg0.getJobName();
		String tableName = jobName.substring(5);
		String portString = arg0.getConfiguration().get("hrdbms.port");
		String hrdbmsHome = arg0.getConfiguration().get("hrdbms.home");
		return new LoadOutputCommitter(tableName, portString, hrdbmsHome);
	}

	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		String jobName = arg0.getJobName();
		String tableName = jobName.substring(5);
		String portString = arg0.getConfiguration().get("hrdbms.port");
		String hrdbmsHome = arg0.getConfiguration().get("hrdbms.home");
		return LoadRecordWriter.get(tableName, portString, hrdbmsHome);
	}
}
