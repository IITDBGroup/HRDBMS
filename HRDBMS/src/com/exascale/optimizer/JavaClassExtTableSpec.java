package com.exascale.optimizer;

import java.util.Map;

public class JavaClassExtTableSpec
{
	private final String javaClassName;
	private final Map<String, String> keyValueList;
	
	public JavaClassExtTableSpec(String javaClassName, Map<String, String> keyValueList)
	{
		this.javaClassName = javaClassName;
		this.keyValueList = keyValueList;
	}

	public String getJavaClassName()
	{
		return javaClassName;
	}

	public Map<String, String> getKeyValueList()
	{
		return keyValueList;
	}
}
