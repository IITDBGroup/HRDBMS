package com.exascale.optimizer.externalTable;

import java.util.ArrayList;
import com.exascale.optimizer.ColDef;
import com.exascale.optimizer.GeneralExtTableSpec;
import com.exascale.optimizer.JavaClassExtTableSpec;
import com.exascale.optimizer.SQLStatement;
import com.exascale.optimizer.TableName;

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
