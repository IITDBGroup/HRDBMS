import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;


public class ResultSetMetaDataWritable implements ResultSetMetaData, Writable, Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7118513050948709303L;
	private int numCols;
	private String[] catalogNames;
	private String[] classNames;
	private int[] displaySizes;
	private String[] columnLabels;
	private String[] columnNames;
	private int[] columnTypes;
	private String[] typeNames;
	private int[] precision;
	private int[] scale;
	private String[] schemas;
	private String[] tables;
	private boolean[] increment;
	private boolean[] caseSensitive;
	private boolean[] currency;
	private boolean[] writable;
	private int[] nullable;
	private boolean[] readOnly;
	private boolean[] searchable;
	private boolean[] signed;
	
	private void readObject(
		     ObjectInputStream aInputStream
		   ) throws ClassNotFoundException, IOException {
		     //always perform the default de-serialization first
		     aInputStream.defaultReadObject();
		  }

		    /**
		    * This is the default implementation of writeObject.
		    * Customise if necessary.
		    */
		    private void writeObject(
		      ObjectOutputStream aOutputStream
		    ) throws IOException {
		      //perform the default serialization for all non-transient, non-static fields
		      aOutputStream.defaultWriteObject();
		    }
	
	public ResultSetMetaDataWritable()
	{
		
	}
	
	public ResultSetMetaDataWritable(ResultSetMetaData meta) throws SQLException
	{
		numCols = meta.getColumnCount();
		catalogNames = new String[numCols];
		classNames = new String[numCols];
		displaySizes = new int[numCols];
		columnLabels = new String[numCols];
		columnNames = new String[numCols];
		columnTypes = new int[numCols];
		typeNames = new String[numCols];
		precision = new int[numCols];
		scale = new int[numCols];
		schemas = new String[numCols];
		tables = new String[numCols];
		increment = new boolean[numCols];
		caseSensitive = new boolean[numCols];
		currency = new boolean[numCols];
		writable = new boolean[numCols];
		nullable = new int[numCols];
		readOnly = new boolean[numCols];
		searchable = new boolean[numCols];
		signed = new boolean[numCols];
		writable = new boolean[numCols];
		
		int i = 0;
		while (i < numCols)
		{
			catalogNames[i] = meta.getCatalogName(i+1);
			classNames[i] = meta.getColumnClassName(i+1);
			displaySizes[i] = meta.getColumnDisplaySize(i+1);
			columnLabels[i] = meta.getColumnLabel(i+1);
			columnNames[i] = meta.getColumnName(i+1);
			columnTypes[i] = meta.getColumnType(i+1);
			typeNames[i] = meta.getColumnTypeName(i+1);
			precision[i] = meta.getPrecision(i+1);
			scale[i] = meta.getScale(i+1);
			schemas[i] = meta.getSchemaName(i+1);
			tables[i] = meta.getTableName(i+1);
			increment[i] = meta.isAutoIncrement(i+1);
			caseSensitive[i] = meta.isCaseSensitive(i+1);
			currency[i] = meta.isCurrency(i+1);
			writable[i] = meta.isDefinitelyWritable(i+1);
			nullable[i] = meta.isNullable(i+1);
			readOnly[i] = meta.isReadOnly(i+1);
			searchable[i] = meta.isSearchable(i+1);
			signed[i] = meta.isSigned(i+1);
			writable[i] = meta.isWritable(i+1);
			i++;
		}
	}
	
	public void write(DataOutput out) throws IOException
	{
		if (numCols <= 0)
		{
				System.out.println("numCols is not positive in meta data writable write");
				System.exit(0);
		}
		(new VIntWritable(numCols)).write(out);
		int i = 0;
		while (i < numCols)
		{
			(new Text(catalogNames[i])).write(out);
			(new Text(classNames[i])).write(out);
			(new VIntWritable(displaySizes[i])).write(out);
			(new Text(columnLabels[i])).write(out);
			(new Text(columnNames[i])).write(out);
			(new VIntWritable(columnTypes[i])).write(out);
			(new Text(typeNames[i])).write(out);
			(new VIntWritable(precision[i])).write(out);
			(new VIntWritable(scale[i])).write(out);
			(new Text(schemas[i])).write(out);
			(new Text(tables[i])).write(out);
			(new BooleanWritable(increment[i])).write(out);
			(new BooleanWritable(caseSensitive[i])).write(out);
			(new BooleanWritable(currency[i])).write(out);
			(new BooleanWritable(writable[i])).write(out);
			(new VIntWritable(nullable[i])).write(out);
			(new BooleanWritable(readOnly[i])).write(out);
			(new BooleanWritable(searchable[i])).write(out);
			(new BooleanWritable(signed[i])).write(out);
			i++;
		}
	}
	
	public void readFields(DataInput in) throws IOException 
	{
		VIntWritable vint = new VIntWritable();
		Text text = new Text();
		BooleanWritable bool = new BooleanWritable();
		
		vint.readFields(in);
		numCols = vint.get();
		
		catalogNames = new String[numCols];
		classNames = new String[numCols];
		displaySizes = new int[numCols];
		columnLabels = new String[numCols];
		columnNames = new String[numCols];
		columnTypes = new int[numCols];
		typeNames = new String[numCols];
		precision = new int[numCols];
		scale = new int[numCols];
		schemas = new String[numCols];
		tables = new String[numCols];
		increment = new boolean[numCols];
		caseSensitive = new boolean[numCols];
		currency = new boolean[numCols];
		writable = new boolean[numCols];
		nullable = new int[numCols];
		readOnly = new boolean[numCols];
		searchable = new boolean[numCols];
		signed = new boolean[numCols];
		writable = new boolean[numCols];
		
		int i = 0;
		while (i < numCols)
		{
			//(new Text(catalogNames[i])).write(out);
			text.readFields(in);
			catalogNames[i] = text.toString();
			//(new Text(classNames[i])).write(out);
			text.readFields(in);
			classNames[i] = text.toString();
			//(new VIntWritable(displaySizes[i])).write(out);
			vint.readFields(in);
			displaySizes[i] = vint.get();
			//(new Text(columnLabels[i])).write(out);
			text.readFields(in);
			columnLabels[i] = text.toString();
			//(new Text(columnNames[i])).write(out);
			text.readFields(in);
			columnNames[i] = text.toString();
			//(new VIntWritable(columnTypes[i])).write(out);
			vint.readFields(in);
			columnTypes[i] = vint.get();
			//(new Text(typeNames[i])).write(out);
			text.readFields(in);
			typeNames[i] = text.toString();
			//(new VIntWritable(precision[i])).write(out);
			vint.readFields(in);
			precision[i] = vint.get();
			//(new VIntWritable(scale[i])).write(out);
			vint.readFields(in);
			scale[i] = vint.get();
			//(new Text(schemas[i])).write(out);
			text.readFields(in);
			schemas[i] = text.toString();
			//(new Text(tables[i])).write(out);
			text.readFields(in);
			tables[i] = text.toString();
			//(new BooleanWritable(increment[i])).write(out);
			bool.readFields(in);
			increment[i] = bool.get();
			//(new BooleanWritable(caseSensitive[i])).write(out);
			bool.readFields(in);
			caseSensitive[i] = bool.get();
			//(new BooleanWritable(currency[i])).write(out);
			bool.readFields(in);
			currency[i] = bool.get();
			//(new BooleanWritable(writable[i])).write(out);
			bool.readFields(in);
			writable[i] = bool.get();
			//(new VIntWritable(nullable[i])).write(out);
			vint.readFields(in);
			nullable[i] = vint.get();
			//(new BooleanWritable(readOnly[i])).write(out);
			bool.readFields(in);
			readOnly[i] = bool.get();
			//(new BooleanWritable(searchable[i])).write(out);
			bool.readFields(in);
			searchable[i] = bool.get();
			//(new BooleanWritable(signed[i])).write(out);
			bool.readFields(in);
			signed[i] = bool.get();
			i++;
		}
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		return null;
	}

	@Override
	public String getCatalogName(int arg0) throws SQLException {
		return catalogNames[arg0-1];
	}

	@Override
	public String getColumnClassName(int arg0) throws SQLException {
		return classNames[arg0-1];
	}

	@Override
	public int getColumnCount() throws SQLException {
		return numCols;
	}

	@Override
	public int getColumnDisplaySize(int arg0) throws SQLException {
		return displaySizes[arg0-1];
	}

	@Override
	public String getColumnLabel(int arg0) throws SQLException {
		return columnLabels[arg0-1];
	}

	@Override
	public String getColumnName(int arg0) throws SQLException {
		return columnNames[arg0-1];
	}

	@Override
	public int getColumnType(int arg0) throws SQLException {
		return columnTypes[arg0-1];
	}

	@Override
	public String getColumnTypeName(int arg0) throws SQLException {
		return typeNames[arg0-1];
	}

	@Override
	public int getPrecision(int arg0) throws SQLException {
		return precision[arg0-1];
	}

	@Override
	public int getScale(int arg0) throws SQLException {
		return scale[arg0-1];
	}

	@Override
	public String getSchemaName(int arg0) throws SQLException {
		return schemas[arg0-1];
	}

	@Override
	public String getTableName(int arg0) throws SQLException {
		return tables[arg0-1];
	}

	@Override
	public boolean isAutoIncrement(int arg0) throws SQLException {
		return increment[arg0-1];
	}

	@Override
	public boolean isCaseSensitive(int arg0) throws SQLException {
		return caseSensitive[arg0-1];
	}

	@Override
	public boolean isCurrency(int arg0) throws SQLException {
		return currency[arg0-1];
	}

	@Override
	public boolean isDefinitelyWritable(int arg0) throws SQLException {
		return writable[arg0-1];
	}

	@Override
	public int isNullable(int arg0) throws SQLException {
		return nullable[arg0-1];
	}

	@Override
	public boolean isReadOnly(int arg0) throws SQLException {
		return readOnly[arg0-1];
	}

	@Override
	public boolean isSearchable(int arg0) throws SQLException {
		return searchable[arg0-1];
	}

	@Override
	public boolean isSigned(int arg0) throws SQLException {
		return signed[arg0-1];
	}

	@Override
	public boolean isWritable(int arg0) throws SQLException {
		return writable[arg0-1];
	}
	
	public int findColumn(String in) throws SQLException
	{
		int i = 0;
		for (String name : columnNames)
		{
			if (in.equals(name))
			{
				return i+1;
			}
			i++;
		}
		
		throw new SQLException("Column named " + in + " does not exist.");
	}
}
