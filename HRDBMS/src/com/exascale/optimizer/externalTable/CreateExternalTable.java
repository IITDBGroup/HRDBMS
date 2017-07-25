package com.exascale.optimizer.externalTable;

import java.util.ArrayList;
import java.util.List;

import com.exascale.optimizer.ColDef;
import com.exascale.optimizer.SQLStatement;
import com.exascale.optimizer.TableName;

public class CreateExternalTable extends SQLStatement
{
	private final TableName table;
	private final List<ColDef> cols;
	private GeneralExtTableSpec generalExtTableSpec;
	private JavaClassExtTableSpec javaClassExtTableSpec;

	public CreateExternalTable(TableName table, List<ColDef> cols, GeneralExtTableSpec generalExtTableSpec)
	{
		this.table = table;
		this.cols = cols;
		this.generalExtTableSpec = generalExtTableSpec;
	}

	public CreateExternalTable(TableName table, List<ColDef> cols, JavaClassExtTableSpec javaClassExtTableSpec)
	{
		this.table = table;
		this.cols = cols;
		this.javaClassExtTableSpec = javaClassExtTableSpec;
	}

	public TableName getTable()
	{
		return table;
	}

	public List<ColDef> getCols()
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
