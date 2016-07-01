package com.exascale.optimizer;

import java.util.ArrayList;

public class CreateExternalTable extends SQLStatement
{
	private final TableName table;
	private final ArrayList<ColDef> cols;
	private GeneralExtTableSpec generalExtTableSpec;
	private JavaClassExtTableSpec javaClassExtTableSpec;

	public CreateExternalTable(TableName table, ArrayList<ColDef> cols, GeneralExtTableSpec generalExtTableSpec)
	{
		this.table = table;
		this.cols = cols;
		this.generalExtTableSpec = generalExtTableSpec;
	}

	public CreateExternalTable(TableName table, ArrayList<ColDef> cols, JavaClassExtTableSpec javaClassExtTableSpec)
	{
		this.table = table;
		this.cols = cols;
		this.javaClassExtTableSpec = javaClassExtTableSpec;
	}

	public TableName getTable()
	{
		return table;
	}

	public ArrayList<ColDef> getCols()
	{
		return cols;
	}

	public GeneralExtTableSpec getGeneralExtTableSpec()
	{
		return generalExtTableSpec;
	}

	public JavaClassExtTableSpec getJavaClassExtTableSpec()
	{
		return javaClassExtTableSpec;
	}		
}
