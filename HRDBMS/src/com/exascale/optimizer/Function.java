package com.exascale.optimizer;

import java.util.ArrayList;

public class Function
{
	private String name;
	private ArrayList<Expression> args;
	boolean distinct = false;
	
	public Function(String name, ArrayList<Expression> args)
	{
		this.name = name;
		this.args = args;
	}
	
	public Function(String name, ArrayList<Expression> args, boolean distinct)
	{
		this.name = name;
		this.args = args;
		this.distinct = distinct;
	}
	
	public String getName()
	{
		return name;
	}
	
	public ArrayList<Expression> getArgs()
	{
		return args;
	}
	
	public boolean getDistinct()
	{
		return distinct;
	}
}
