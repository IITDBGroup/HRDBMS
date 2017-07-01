package com.exascale.optimizer.externalTable;

/** A short cut syntax for loading from a CSV.  Recommended to use the Java Class ext table spec with HTTPCsvExternal instead. */
@Deprecated
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
