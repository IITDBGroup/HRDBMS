package com.exascale.optimizer;

import java.util.Map;
import java.util.Properties;

public class JavaClassExtTableSpec
{
	private final String javaClassName;
	private final Properties keyValueList;
	
	public JavaClassExtTableSpec(String javaClassName, Properties keyValueList)
	{
		this.javaClassName = javaClassName;
		this.keyValueList = keyValueList;
	}

	public String getJavaClassName()
	{
		return javaClassName;
	}

	public Properties getKeyValueList()
	{
		return keyValueList;
	}
}
