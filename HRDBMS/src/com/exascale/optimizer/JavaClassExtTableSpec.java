package com.exascale.optimizer;

/** Each external table is specified by the canonical name of the implementing Java class and the parameter string */
public class JavaClassExtTableSpec
{
	private final String javaClassName, params;
	
	public JavaClassExtTableSpec(String javaClassName, String params)
	{
		this.javaClassName = javaClassName;
		this.params = params;
	}

	@Override
	public JavaClassExtTableSpec clone()
	{
		return new JavaClassExtTableSpec(javaClassName, params);
	}
	public String getJavaClassName()
	{
		return javaClassName;
	}
	public String getParams()
	{
		return params;
	}
}