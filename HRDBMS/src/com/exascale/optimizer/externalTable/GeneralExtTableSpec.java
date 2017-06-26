package com.exascale.optimizer.externalTable;

public class GeneralExtTableSpec
{
	private final String sourceList;
	private final String anyString;
	private final String filePathIdentifier;
	
	public GeneralExtTableSpec(String sourceList, String anyString, String filePathIdentifier)
	{
		this.sourceList = sourceList;
		this.anyString = anyString;
		this.filePathIdentifier = filePathIdentifier;
	}

	public String getSourceList()
	{
		return sourceList;
	}

	public String getAnyString()
	{
		return anyString;
	}

	public String getFilePathIdentifier()
	{
		return filePathIdentifier;
	}
}
