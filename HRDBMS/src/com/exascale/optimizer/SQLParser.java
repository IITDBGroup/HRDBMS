package com.exascale.optimizer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import com.exascale.exceptions.ParseException;
import com.exascale.misc.DateParser;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.tables.SQL;
import com.exascale.threads.ConnectionWorker;

//TODO CaseOperator
//TODO need to test and fix column and table renaming issues

public class SQLParser
{
	private SQL sql;
	private MetaData meta = new MetaData();
	private int suffix = 0;
	private int complexID = 0;
	private ArrayList<ArrayList<Object>> complex = new ArrayList<ArrayList<Object>>();
	private ConnectionWorker connection;
	private boolean doesNotUseCurrentSchema = true;
	
	public static final int TYPE_INLINE = 0;
	public static final int TYPE_GROUPBY = 1;
	public static final int TYPE_DATE = 2;
	public static final int TYPE_DAYS = 3;

	public SQLParser(String sql, ConnectionWorker connection)
	{
		this.sql = new SQL(sql);
		this.connection = connection;
	}
	
	public boolean doesNotUseCurrentSchema()
	{
		return doesNotUseCurrentSchema;
	}
	
	public Operator parse() throws Exception
	{
		ANTLRInputStream input = new ANTLRInputStream(sql.toString());
		SelectLexer lexer = new SelectLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		SelectParser parser = new SelectParser(tokens);
		ParseTree tree = parser.select();
		SelectVisitorImpl visitor = new SelectVisitorImpl();
		SQLStatement stmt = (SQLStatement)visitor.visit(tree);
		
		if (stmt instanceof Select)
		{
			Operator op = buildOperatorTreeFromSelect((Select)stmt);
			RootOperator retval = new RootOperator(meta.generateCard(op), new MetaData());
			retval.add(op);
			return retval;
		}
		
		if (stmt instanceof Insert)
		{
			Operator op = buildOperatorTreeFromInsert((Insert)stmt);
			return op;
		}
		
		if (stmt instanceof Update)
		{
			Operator op = buildOperatorTreeFromUpdate((Update)stmt);
			return op;
		}
		
		if (stmt instanceof Delete)
		{
			Operator op = buildOperatorTreeFromDelete((Delete)stmt);
			return op;
		}
		
		if (stmt instanceof CreateTable)
		{
			Operator op = buildOperatorTreeFromCreateTable((CreateTable)stmt);
			//TODO
		}
		
		if (stmt instanceof DropTable)
		{
			Operator op = buildOperatorTreeFromDropTable((DropTable)stmt);
			//TODO
		}
		
		if (stmt instanceof CreateIndex)
		{
			Operator op = buildOperatorTreeFromCreateIndex((CreateIndex)stmt);
			//TODO
		}
		
		if (stmt instanceof DropIndex)
		{
			Operator op = buildOperatorTreeFromDropIndex((DropIndex)stmt);
			//TODO
		}
		
		if (stmt instanceof CreateView)
		{
			Operator op = buildOperatorTreeFromCreateView((CreateView)stmt);
			//TODO
		}
		
		if (stmt instanceof DropView)
		{
			Operator op = buildOperatorTreeFromDropView((DropView)stmt);
			//TODO
		}
		
		return null;
	}
	
	private Operator buildOperatorTreeFromCreateView(CreateView createView) throws Exception
	{
		buildOperatorTreeFromFullSelect(createView.getSelect());
		TableName table = createView.getView();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (meta.verifyTableExistence(schema, tbl) || meta.verifyViewExistence(schema, tbl))
		{
			throw new ParseException("Table or view already exists");
		}
		
		return new CreateViewOperator(schema, tbl, createView.getText(), meta);
	}
	
	private Operator buildOperatorTreeFromDropView(DropView dropView) throws Exception
	{
		TableName table = dropView.getView();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyViewExistence(schema, tbl))
		{
			throw new ParseException("Table or view does not exist");
		}
		
		return new DropViewOperator(schema, tbl, meta);
	}
	
	private Operator buildOperatorTreeFromCreateIndex(CreateIndex createIndex) throws Exception
	{
		TableName table = createIndex.getTable();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyTableExistence(schema, tbl))
		{
			throw new ParseException("Table does not exist");
		}
		
		String index = createIndex.getIndex().getName();
		if (createIndex.getIndex().getSchema() != null)
		{
			throw new ParseException("Schemas cannot be specified for index names");
		}
		
		if (meta.verifyIndexExistence(schema, index))
		{
			throw new ParseException("Index already exists");
		}
		
		boolean unique = createIndex.getUnique();
		ArrayList<IndexDef> indexDefs = createIndex.getCols();
		for (IndexDef def : indexDefs)
		{
			String col = def.getCol().getColumn();
			if (def.getCol().getTable() != null)
			{
				throw new ParseException("Column names cannot be qualified with table names in a CREATE INDEX statement");
			}
			if (!meta.verifyColExistence(schema, tbl, col))
			{
				throw new ParseException("Column " + col + " does not exist");
			}
		}
		
		return new CreateIndexOperator(schema, tbl, index, indexDefs, unique, meta);
	}
	
	private Operator buildOperatorTreeFromDropIndex(DropIndex dropIndex) throws Exception
	{
		TableName table = dropIndex.getIndex();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyIndexExistence(schema, tbl))
		{
			throw new ParseException("Index does not exist");
		}
		
		return new DropIndexOperator(schema, tbl, meta);
	}
	
	private Operator buildOperatorTreeFromCreateTable(CreateTable createTable) throws Exception
	{
		TableName table = createTable.getTable();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (meta.verifyTableExistence(schema, tbl) || meta.verifyViewExistence(schema, tbl))
		{
			throw new ParseException("Table or view already exists");
		}
		
		ArrayList<ColDef> colDefs = createTable.getCols();
		HashSet<String> colNames = new HashSet<String>();
		HashSet<String> pks = new HashSet<String>();
		for (ColDef def : colDefs)
		{
			String col = def.getCol().getColumn();
			if (def.getCol().getTable() != null)
			{
				throw new ParseException("Column names cannot be qualified with table names in a CREATE TABLE statement");
			}
			
			if (!colNames.add(col))
			{
				throw new ParseException("CREATE TABLE statement had a duplicate column names");
			}
			
			if (def.isNullable() && def.isPK())
			{
				throw new ParseException("A column cannot be nullable and part of the primary key");
			}
			
			if (def.isPK())
			{
				pks.add(col);
			}
		}
		
		if (createTable.getPK() != null)
		{
			ArrayList<Column> cols = createTable.getPK().getCols();
			for (Column col : cols)
			{
				String c = col.getColumn();
				if (col.getTable() != null)
				{
					throw new ParseException("Column names cannot be qualified with table names in a CREATE TABLE statement");
				}
				
				pks.add(c);
			}
		}
		
		return new CreateTableOperator(schema, tbl, colDefs, new ArrayList<String>(pks), meta);
	}
	
	private Operator buildOperatorTreeFromDropTable(DropTable dropTable) throws Exception
	{
		TableName table = dropTable.getTable();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyIndexExistence(schema, tbl))
		{
			throw new ParseException("Table does not exist");
		}
		
		return new DropTableOperator(schema, tbl, meta);
	}
	
	private Operator buildOperatorTreeFromUpdate(Update update) throws Exception
	{
		TableName table = update.getTable();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyTableExistence(schema, tbl))
		{
			throw new ParseException("Table does not exist");
		}
		
		TableScanOperator scan = new TableScanOperator(schema, tbl, meta);
		Operator op = null;
		if (update.getWhere() != null)
		{
			op = buildOperatorTreeFromWhere(update.getWhere(), scan);
		}
		else
		{
			op = scan;
		}
		scan.getRID();
		ArrayList<String> cols = new ArrayList<String>();
		cols.add("_RID1");
		cols.add("_RID2");
		cols.add("_RID3");
		cols.add("_RID4");
		
		ArrayList<String> buildList = new ArrayList<String>();
		
		ArrayList<Expression> exps = null;
		if (update.getExpression().isList())
		{
			exps = update.getExpression().getList();
		}
		else
		{
			exps = new ArrayList<Expression>();
			exps.add(update.getExpression());
		}
		for (Expression exp : exps)
		{
			if (exp.isColumn())
			{
				buildList.add(exp.getColumn().getColumn());
				continue;
			}
			OperatorTypeAndName otan = buildOperatorTreeFromExpression(exp, null);
			if (otan.getType() != this.TYPE_INLINE && otan.getType() != this.TYPE_DATE)
			{
				throw new ParseException("Invalid expression in update statement");
			}
			
			if (otan.getType() == this.TYPE_DATE)
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
				String name = "._E" + suffix++;
				ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
				operator.add(op);
				op = operator;
				buildList.add(name);
			}
			else
			{
				Operator operator = (Operator)otan.getOp();
				operator.add(op);
				op = operator;
				buildList.add(otan.getName());
			}
		}
		
		if (!MetaData.verifyUpdate(schema, tbl, update.getCols(), buildList, op))
		{
			throw new ParseException("The number of columns and/or data types do not match the columns being updated");
		}
		
		cols.addAll(new HashSet<String>(buildList));
		cols.addAll(MetaData.getIndexColsForTable(schema, tbl));
		ProjectOperator project = new ProjectOperator(cols, meta);
		project.add(op);
		op = project;
		RootOperator retval = new RootOperator(meta.generateCard(op), new MetaData());
		retval.add(op);
		new Phase1(retval).optimize();
		new Phase2(retval).optimize();
		new Phase3(retval).optimize();
		new Phase4(retval).optimize();
		new Phase5(retval).optimize();
		UpdateOperator uOp = new UpdateOperator(schema, tbl, update.getCols(), buildList, meta);
		uOp.add(retval);
		return uOp;
	}
	
	private Operator buildOperatorTreeFromInsert(Insert insert) throws Exception
	{
		TableName table = insert.getTable();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyTableExistence(schema, tbl))
		{
			throw new ParseException("Table does not exist");
		}
		
		if (insert.fromSelect())
		{
			Operator op = buildOperatorTreeFromFullSelect(insert.getSelect());
			RootOperator retval = new RootOperator(meta.generateCard(op), new MetaData());
			retval.add(op);
			new Phase1(retval).optimize();
			new Phase2(retval).optimize();
			new Phase3(retval).optimize();
			new Phase4(retval).optimize();
			new Phase5(retval).optimize();
			
			if (!MetaData.verifyInsert(schema, tbl, op))
			{
				throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
			}
			
			Operator iOp = new InsertOperator(schema, tbl, meta);
			iOp.add(retval);
			return iOp;
		}
		else
		{
			Operator op = new DummyOperator(meta);
			for (Expression exp : insert.getExpressions())
			{
				OperatorTypeAndName otan = buildOperatorTreeFromExpression(exp, null);
				if (otan.getType() != this.TYPE_INLINE && otan.getType() != this.TYPE_DATE)
				{
					throw new ParseException("Invalid expression in insert statement");
				}
				
				if (otan.getType() == this.TYPE_DATE)
				{
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
					String name = "._E" + suffix++;
					ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
					operator.add(op);
					op = operator;
				}
				else
				{
					Operator operator = (Operator)otan.getOp();
					operator.add(op);
					op = operator;
				}
			}
			
			if (!MetaData.verifyInsert(schema, tbl, op))
			{
				throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
			}
			
			Operator iOp = new InsertOperator(schema, tbl, meta);
			iOp.add(op);
			return iOp;
		}
	}
	
	private Operator buildOperatorTreeFromDelete(Delete delete) throws Exception
	{
		TableName table = delete.getTable();
		String schema = null;
		String tbl = null;
		if (table.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			tbl = table.getName();
		}
		else
		{
			schema = table.getSchema();
			tbl = table.getName();
		}
		
		if (!meta.verifyTableExistence(schema, tbl))
		{
			throw new ParseException("Table does not exist");
		}
		
		if (delete.getWhere() == null)
		{
			return new MassDeleteOperator(schema, tbl, meta);
		}
		
		TableScanOperator scan = new TableScanOperator(schema, tbl, meta);
		Operator op = buildOperatorTreeFromWhere(delete.getWhere(), scan);
		scan.getRID();
		ArrayList<String> cols = new ArrayList<String>();
		cols.add("_RID1");
		cols.add("_RID2");
		cols.add("_RID3");
		cols.add("_RID4");
		cols.addAll(MetaData.getIndexColsForTable(schema, tbl));
		ProjectOperator project = new ProjectOperator(cols, meta);
		project.add(op);
		op = project;
		RootOperator retval = new RootOperator(meta.generateCard(op), new MetaData());
		retval.add(op);
		new Phase1(retval).optimize();
		new Phase2(retval).optimize();
		new Phase3(retval).optimize();
		new Phase4(retval).optimize();
		new Phase5(retval).optimize();
		DeleteOperator dOp = new DeleteOperator(schema, tbl, meta);
		dOp.add(retval);
		return dOp;
	}
	
	private Operator buildOperatorTreeFromSelect(Select select) throws ParseException
	{
		if (select.getCTEs().size() > 0)
		{
			for (CTE cte : select.getCTEs())
			{
				importCTE(cte, select.getFullSelect());
			}
		}
		return buildOperatorTreeFromFullSelect(select.getFullSelect());
	}
	
	private void importCTE(CTE cte, FullSelect select)
	{
		String name = cte.getName();
		ArrayList<Column> cols = cte.getCols();
		FullSelect cteSelect = cte.getSelect();
		
		if (select.getSubSelect() != null)
		{
			searchSubSelectForCTE(name, cols, cteSelect, select.getSubSelect());
		}
		else
		{
			searchFullSelectForCTE(name, cols, cteSelect, select.getFullSelect());
		}
		
		for (ConnectedSelect cs : select.getConnected())
		{
			if (cs.getSub() != null)
			{
				searchSubSelectForCTE(name, cols, cteSelect, cs.getSub());
			}
			else
			{
				searchFullSelectForCTE(name, cols, cteSelect, cs.getFull());
			}
		}
	}
	
	private void searchSubSelectForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, SubSelect select)
	{
		searchFromForCTE(name, cols, cteSelect, select.getFrom());
	}
	
	private void searchFromForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, FromClause from)
	{
		for (TableReference table : from.getTables())
		{
			searchTableRefForCTE(name, cols, cteSelect, table);
		}
	}
	
	private void searchTableRefForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, TableReference table)
	{
		if (table.isSingleTable())
		{
			searchSingleTableForCTE(name, cols, cteSelect, table.getSingleTable(), table);
		}
		else if (table.isSelect())
		{
			searchFullSelectForCTE(name, cols, cteSelect, table.getSelect());
		}
		else
		{
			searchTableRefForCTE(name, cols, cteSelect, table.getLHS());
			searchTableRefForCTE(name, cols, cteSelect, table.getRHS());
		}
	}
	
	private void searchSingleTableForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, SingleTable table, TableReference tref)
	{
		TableName tblName = table.getName();
		if (tblName.getSchema() == null && tblName.getName().equals(name))
		{
			//found a match
			tref.removeSingleTable();
			if (cols.size() > 0)
			{
				cteSelect.addCols(cols);
			}
			tref.addSelect(cteSelect);
			if (table.getAlias() != null)
			{
				tref.setAlias(table.getAlias());
			}
		}
	}
	
	private void searchFullSelectForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, FullSelect select)
	{
		if (select.getSubSelect() != null)
		{
			searchSubSelectForCTE(name, cols, cteSelect, select.getSubSelect());
		}
		else
		{
			searchFullSelectForCTE(name, cols, cteSelect, select.getFullSelect());
		}
		
		for (ConnectedSelect cs : select.getConnected())
		{
			if (cs.getSub() != null)
			{
				searchSubSelectForCTE(name, cols, cteSelect, cs.getSub());
			}
			else
			{
				searchFullSelectForCTE(name, cols, cteSelect, cs.getFull());
			}
		}
	}
	
	private void verifyColumnsAreTheSame(Operator lhs, Operator rhs) throws ParseException
	{
		TreeMap<Integer, String> lhsPos2Col = lhs.getPos2Col();
		TreeMap<Integer, String> rhsPos2Col = rhs.getPos2Col();
		
		if (lhsPos2Col.size() != rhsPos2Col.size())
		{
			throw new ParseException("Cannot combine table with different number of columns!");
		}
		
		int i = 0;
		for (String col : lhsPos2Col.values())
		{
			if (!lhs.getCols2Types().get(col).equals(rhs.getCols2Types().get(rhsPos2Col.get(new Integer(i)))))
			{
				throw new ParseException("Column types do not match when combining tables!");
			}
			
			i++;
		}
	}
	
	private Operator buildOperatorTreeFromFullSelect(FullSelect select) throws ParseException
	{
		Operator op;
		if (select.getSubSelect() != null)
		{
			op = buildOperatorTreeFromSubSelect(select.getSubSelect());
		}
		else
		{
			op = buildOperatorTreeFromFullSelect(select.getFullSelect());
		}
		
		//handle connectedSelects
		ArrayList<ConnectedSelect> connected = select.getConnected();
		Operator lhs = op;
		for (ConnectedSelect cs : connected)
		{
			Operator rhs;
			if (cs.getFull() != null)
			{
				rhs = buildOperatorTreeFromFullSelect(cs.getFull());
			}
			else
			{
				rhs = buildOperatorTreeFromSubSelect(cs.getSub());
			}
			
			verifyColumnsAreTheSame(lhs, rhs);
			String combo = cs.getCombo();
			if (combo.equals("UNION ALL"))
			{
				Operator newOp = new UnionOperator(false, meta);
				try
				{
					newOp.add(lhs);
					newOp.add(rhs);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
				lhs = newOp;
			}
			else if (combo.equals("UNION"))
			{
				Operator newOp = new UnionOperator(true, meta);
				try
				{
					newOp.add(lhs);
					newOp.add(rhs);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
				lhs = newOp;
			}
			else if (combo.equals("INTERSECT"))
			{
				Operator newOp = new IntersectOperator(meta);
				try
				{
					newOp.add(lhs);
					newOp.add(rhs);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
				lhs = newOp;
			}
			else if (combo.equals("EXCEPT"))
			{
				Operator newOp = new ExceptOperator(meta);
				try
				{
					newOp.add(lhs);
					newOp.add(rhs);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
				lhs = newOp;
			}
			else
			{
				throw new ParseException("Unknown table combination type " + combo);
			}
		}
		
		op = lhs;
		
		//handle orderBy
		if (select.getOrderBy() != null)
		{
			op = buildOperatorTreeFromOrderBy(select.getOrderBy(), op);
		}
		//handle fetchFirst
		if (select.getFetchFirst() != null)
		{
			op = buildOperatorTreeFromFetchFirst(select.getFetchFirst(), op);
		}
		
		return op;
	}
	
	private Operator buildOperatorTreeFromFetchFirst(FetchFirst fetchFirst, Operator op) throws ParseException
	{
		try
		{
			TopOperator top = new TopOperator(fetchFirst.getNumber(), meta);
			top.add(op);
			return top;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private void getComplexColumns(SelectClause select, SubSelect sub) throws ParseException
	{
		ArrayList<SelectListEntry> selects = select.getSelectList();
		for (SelectListEntry s : selects)
		{
			if (!s.isColumn())
			{
				//complex column
				Expression exp = s.getExpression();
				OperatorTypeAndName op = buildOperatorTreeFromExpression(exp, s.getName());
				if (op.getType() == TYPE_DAYS)
				{
					throw new ParseException("A type of DAYS is not allowed for a column in a select list");
				}
				
				if (op.getType() == TYPE_DATE)
				{
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String dateString = sdf.format(((GregorianCalendar)op.getOp()).getTime());
					
					if (s.getName() != null)
					{
						ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), "." + s.getName(), meta);
						ArrayList<Object> row = new ArrayList<Object>();
						row.add("." + s.getName());
						row.add(operator);
						row.add(TYPE_INLINE);
						row.add(complexID++);
						row.add(exp);
						row.add(-1);
						row.add(sub);
						row.add(false);
						complex.add(row);
					}
					else
					{
						String name = "._E" + suffix++;
						ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
						ArrayList<Object> row = new ArrayList<Object>();
						row.add(name);
						row.add(operator);
						row.add(TYPE_INLINE);
						row.add(complexID++);
						row.add(exp);
						row.add(-1);
						row.add(sub);
						row.add(false);
						complex.add(row);
					}
				}
				else
				{
					//colName, op, type, id, exp, prereq, sub, done
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(op.getName());
					row.add(op.getOp());
					row.add(op.getType());
					row.add(complexID++);
					row.add(exp);
					row.add(op.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
				}
			}
		}
	}
	
	private static final class OperatorTypeAndName
	{
		Object op;
		int type;
		String name;
		int prereq;
		
		public OperatorTypeAndName(Object op, int type, String name, int prereq)
		{
			this.op = op;
			this.type = type;
			this.name = name;
			this.prereq = prereq;
		}
		
		public String getName()
		{
			return name;
		}
		
		public Object getOp()
		{
			return op;
		}
		
		public int getType()
		{
			return type;
		}
		
		public int getPrereq()
		{
			return prereq;
		}
	}
	
	private OperatorTypeAndName buildOperatorTreeFromExpression(Expression exp, String name) throws ParseException
	{
		if (exp.isLiteral())
		{
			Literal l = exp.getLiteral();
			if (l.isNull())
			{
				//TODO
			}
			else
			{
				Object literal = l.getValue();
				if (literal instanceof Double || literal instanceof Long)
				{
					if (name != null)
					{
						return new OperatorTypeAndName(new ExtendOperator(literal.toString(), "." + name, meta), TYPE_INLINE, "." + name, -1);
					}
					else
					{
						name = "." + "_E" + suffix++;
						return new OperatorTypeAndName(new ExtendOperator(literal.toString(), name, meta), TYPE_INLINE, name, -1);
					}
				}
				else
				{
					if (name != null)
					{
						return new OperatorTypeAndName(new ExtendObjectOperator(literal, "." + name, meta), TYPE_INLINE, "." + name, -1);
					}
					else
					{
						name = "." + "_E" + suffix++;
						return new OperatorTypeAndName(new ExtendObjectOperator(literal, name, meta), TYPE_INLINE, name, -1);
					}
				}
			}
		}
		else if (exp.isCountStar())
		{
			if (name != null)
			{
				return new OperatorTypeAndName(new CountOperator("." + name, meta), TYPE_GROUPBY, "." + name, -1);
			}
			else
			{
				name = "." + "_E" + suffix++;
				return new OperatorTypeAndName(new CountOperator(name, meta), TYPE_GROUPBY, name, -1);
			}
		}
		else if (exp.isList())
		{
			throw new ParseException("A list is not supported in the context where it is used");
		}
		else if (exp.isSelect())
		{
			throw new ParseException("A SELECT statement is not supported in the context where it is used");
		}
		else if (exp.isFunction())
		{
			Function f = exp.getFunction();
			String method = f.getName();
			if (method.equals("MAX"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("MAX() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (arg.isColumn())
				{
					Column input = arg.getColumn();
					String inputColumn = "";
					if (input.getTable() != null)
					{
						inputColumn += (input.getTable() + ".");
					}
					else
					{
						inputColumn += ".";
					}	
					inputColumn += input.getColumn();
					if (name != null)
					{
						return new OperatorTypeAndName(new MaxOperator(inputColumn, "." + name, meta, true), TYPE_GROUPBY, "." + name, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new MaxOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to MAX() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to MAX() cannot be of type DAYS");
					}
					
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(null);
					row.add(false);
					complex.add(row);
					
					if (name != null)
					{
						return new OperatorTypeAndName(new MaxOperator(retval.getName(), "." + name, meta, true), TYPE_GROUPBY, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new MaxOperator(retval.getName(), name, meta, true), TYPE_GROUPBY, name, (Integer)row.get(3));
					}
				}
				else
				{
					throw new ParseException("MAX() requires 1 argument that is a column or an expression");
				}
			}
			else if (method.equals("MIN"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("MIN() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (arg.isColumn())
				{
					Column input = arg.getColumn();
					String inputColumn = "";
					if (input.getTable() != null)
					{
						inputColumn += (input.getTable() + ".");
					}
					else
					{
						inputColumn += ".";
					}
					inputColumn += input.getColumn();
					if (name != null)
					{
						return new OperatorTypeAndName(new MinOperator(inputColumn, "." + name, meta, true), TYPE_GROUPBY, "." + name, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new MinOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to MIN() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to MIN() cannot be of type DAYS");
					}
					
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(null);
					row.add(false);
					complex.add(row);
					
					if (name != null)
					{
						return new OperatorTypeAndName(new MinOperator(retval.getName(), "." + name, meta, true), TYPE_GROUPBY, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new MinOperator(retval.getName(), name, meta, true), TYPE_GROUPBY, name, (Integer)row.get(3));
					}
				}
				else
				{
					throw new ParseException("MIN() requires an argument that is a column or an expression");
				}
			}
			else if (method.equals("AVG"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("AVG() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (arg.isColumn())
				{
					Column input = arg.getColumn();
					String inputColumn = "";
					if (input.getTable() != null)
					{
						inputColumn += (input.getTable() + ".");
					}
					else
					{
						inputColumn += ".";
					}	
					inputColumn += input.getColumn();
					if (name != null)
					{
						return new OperatorTypeAndName(new AvgOperator(inputColumn, "." + name, meta), TYPE_GROUPBY, "." + name, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new AvgOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to AVG() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to AVG() cannot be of type DAYS");
					}
					
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(null);
					row.add(false);
					complex.add(row);
					
					if (name != null)
					{
						return new OperatorTypeAndName(new AvgOperator(retval.getName(), "." + name, meta), TYPE_GROUPBY, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new AvgOperator(retval.getName(), name, meta), TYPE_GROUPBY, name, (Integer)row.get(3));
					}
				}
				else
				{
					throw new ParseException("AVG() requires 1 argument that is a column or an expression");
				}
			}
			else if (method.equals("SUM"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("SUM() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (arg.isColumn())
				{
					Column input = arg.getColumn();
					String inputColumn = "";
					if (input.getTable() != null)
					{
						inputColumn += (input.getTable() + ".");
					}
					else
					{
						inputColumn += ".";
					}	
					inputColumn += input.getColumn();
					if (name != null)
					{
						return new OperatorTypeAndName(new SumOperator(inputColumn, "." + name, meta, true), TYPE_GROUPBY, "." + name, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new SumOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to SUM() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to SUM() cannot be of type DAYS");
					}
					
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(null);
					row.add(false);
					complex.add(row);
					
					if (name != null)
					{
						return new OperatorTypeAndName(new SumOperator(retval.getName(), "." + name, meta, true), TYPE_GROUPBY, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new SumOperator(retval.getName(), name, meta, true), TYPE_GROUPBY, name, (Integer)row.get(3));
					}
				}
				else
				{
					throw new ParseException("SUM() requires 1 argument that is a column or an expression");
				}
			}
			else if (method.equals("COUNT"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("COUNT() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (arg.isColumn())
				{
					Column input = arg.getColumn();
					String inputColumn = "";
					if (input.getTable() != null)
					{
						inputColumn += (input.getTable() + ".");
					}
					else
					{
						inputColumn += ".";
					}	
					inputColumn += input.getColumn();
					if (name != null)
					{
						if (f.getDistinct())
						{
							return new OperatorTypeAndName(new CountDistinctOperator(inputColumn, "." + name, meta), TYPE_GROUPBY, "." + name, -1);
						}
						else
						{
							return new OperatorTypeAndName(new CountOperator(inputColumn, "." + name, meta), TYPE_GROUPBY, "." + name, -1);
						}
					}
					else
					{
						name = "._E" + suffix++;
						if (f.getDistinct())
						{
							return new OperatorTypeAndName(new CountDistinctOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
						}
						else
						{
							return new OperatorTypeAndName(new CountOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
						}
					}
				}
				else
				{
					throw new ParseException("COUNT() requires 1 argument that is a column");
				}
			}
			else if (method.equals("DATE"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("DATE() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (!arg.isLiteral())
				{
					throw new ParseException("DATE() requires a literal string argument");
				}
				
				Object literal = arg.getLiteral().getValue();
				if (!(literal instanceof String))
				{
					throw new ParseException("DATE() requires a literal string argument");
				}
				String dateString = (String)literal;
				GregorianCalendar cal = new GregorianCalendar();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				try
				{
					Date date = sdf.parse(dateString);
					cal.setTime(date);
					return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
				}
				catch(Exception e)
				{
					throw new ParseException("Date is not in the proper format");
				}
			}
			else if (method.equals("DAYS"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("DAYS() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (!arg.isLiteral())
				{
					throw new ParseException("DAYS() requires a literal numeric argument");
				}
				
				Object literal = arg.getLiteral().getValue();
				if (literal instanceof Integer)
				{
					return new OperatorTypeAndName(literal, TYPE_DAYS, "", -1);
				}
				else if (literal instanceof Long)
				{
					return new OperatorTypeAndName(new Integer(((Long)literal).intValue()), TYPE_DAYS, "", -1);
				}
				else
				{
					throw new ParseException("DAYS() requires a literal numeric argument");
				}
			}
			else if (method.equals("YEAR"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("YEAR() requires only 1 argument");
				}
				Expression arg = args.get(0);
				
				if (arg.isColumn())
				{
					String colName = "";
					Column col = arg.getColumn();
					if (col.getTable() != null)
					{
						colName += col.getTable();
					}
					
					colName += ("." + col.getColumn());
					
					if (name != null)
					{
						return new OperatorTypeAndName(new YearOperator(colName, "." + name, meta), TYPE_INLINE, "." + name, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new YearOperator(colName, name, meta), TYPE_INLINE, name, -1);
					}
				}
				else if (arg.isFunction() || arg.isExpression())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null);
					if (retval.getType() == TYPE_DATE)
					{
						GregorianCalendar cal = (GregorianCalendar)retval.getOp();
						int literal = cal.get(GregorianCalendar.YEAR);
						
						if (name != null)
						{
							return new OperatorTypeAndName(new ExtendOperator(Integer.toString(literal), "." + name, meta), TYPE_INLINE, "." + name, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new ExtendOperator(Integer.toString(literal), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("YEAR() cannot be called with an argument of type DAYS");
					}
					
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(null);
					row.add(false);
					complex.add(row);
					
					if (name != null)
					{
						return new OperatorTypeAndName(new YearOperator(retval.getName(), "." + name, meta), TYPE_INLINE, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new YearOperator(retval.getName(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else
				{
					throw new ParseException("YEAR() requires an argument that is a column, a function, or an expression");
				}
			}
			else if (method.equals("SUBSTRING"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 3 && args.size() != 2)
				{
					throw new ParseException("SUBSTRING() requires 2 or 3 arguments");
				}
				Expression arg = args.get(1);
				int start;
				int end = -1;
				if (!arg.isLiteral())
				{
					throw new ParseException("Argument 2 of SUBSTRING() must be a numeric literal");
				}
				
				Object val = arg.getLiteral().getValue();
				if (val instanceof Integer)
				{
					start = (Integer)val - 1;
					if (start < 0)
					{
						throw new ParseException("Argument 2 of SUBSTRING() cannot be less than 1");
					}
				}
				else if (val instanceof Long)
				{
					start = ((Long)val).intValue() - 1;
					if (start < 0)
					{
						throw new ParseException("Argument 2 of SUBSTRING() cannot be less than 1");
					}
				}
				else
				{
					throw new ParseException("Argument 2 of SUBSTRING() must be a numeric literal");
				}
				
				if (args.size() == 3)
				{
					int length;
					arg = args.get(2);
					if (!arg.isLiteral())
					{
						throw new ParseException("Argument 3 of SUBSTRING() must be a numeric literal");
					}
					
					val = arg.getLiteral().getValue();
					if (val instanceof Integer)
					{
						length = (Integer)val;
						if (length < 0)
						{
							throw new ParseException("Argument 3 of SUBSTRING() cannot be less than 0");
						}
						
						end = start + length;
					}
					else if (val instanceof Long)
					{
						length = ((Long)val).intValue();
						if (length < 0)
						{
							throw new ParseException("Argument 3 of SUBSTRING() cannot be less than 0");
						}
						
						end = start + length;
					}
					else
					{
						throw new ParseException("Argument 3 of SUBSTRING() must be a numeric literal");
					}
				}
				
				arg = args.get(0);
				if (arg.isLiteral() || arg.isFunction() || arg.isExpression())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null);
					if (retval.getType() == TYPE_DATE || retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("Argument 1 to SUBSTRING must be a string");
					}
					
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(null);
					row.add(false);
					complex.add(row);
					
					if (name != null)
					{
						return new OperatorTypeAndName(new SubstringOperator(retval.getName(), start, end, "." + name, meta), TYPE_INLINE, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new SubstringOperator(retval.getName(), start, end, name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else if (arg.isColumn())
				{
					String colName = "";
					Column col = arg.getColumn();
					if (col.getTable() != null)
					{
						colName += col.getTable();
					}
					
					colName += ("." + col.getColumn());
					
					if (name != null)
					{
						return new OperatorTypeAndName(new SubstringOperator(colName, start, end, "." + name, meta), TYPE_INLINE, "." + name, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new SubstringOperator(colName, start, end, name, meta), TYPE_INLINE, name, -1);
					}
				}
				else
				{
					throw new ParseException("The first argument to SUBSTRING() must be a literal, a column, a function, or an expression");
				}
			}
		}
		else if (exp.isExpression())
		{
			OperatorTypeAndName exp1 = buildOperatorTreeFromExpression(exp.getLHS(), null);
			OperatorTypeAndName exp2 = buildOperatorTreeFromExpression(exp.getRHS(), null);
			if (exp1.getType() == TYPE_DATE)
			{
				if (exp.getOp().equals("+"))
				{
					if (exp2.getType() == TYPE_DAYS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(GregorianCalendar.DATE, (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else
					{
						throw new ParseException("Only type DAYS can be added to a DATE");
					}
				}
				else if (exp.getOp().equals("-"))
				{
					if (exp2.getType() == TYPE_DAYS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(GregorianCalendar.DATE, -1 * (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else
					{
						throw new ParseException("Only type DAYS can be substracted from a DATE");
					}
				}
				else
				{
					throw new ParseException("Only the + and - operators are valid with type DATE");
				}
			}
			else if (exp2.getType() == TYPE_DATE)
			{
				if (exp.getOp().equals("+"))
				{
					if (exp1.getType() == TYPE_DAYS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
						cal.add(GregorianCalendar.DATE, (Integer)exp1.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else
					{
						throw new ParseException("Only type DAYS can be added to a DATE");
					}
				}
				else
				{
					throw new ParseException("Only a DATE can be added to type DAYS");
				}
			}
			else if (exp1.getType() == TYPE_DAYS)
			{
				//already handled DAYS + DATE
				//so need to handle DAYS + col(DATE)
				//and DAYS +/- DAYS
				if (exp2.getType() == TYPE_DAYS)
				{
					if (exp.getOp().equals("+"))
					{
						return new OperatorTypeAndName((Integer)exp1.getOp() + (Integer)exp2.getOp(), TYPE_DAYS, "", -1);
					}
					else if (exp.getOp().equals("-"))
					{
						return new OperatorTypeAndName((Integer)exp1.getOp() - (Integer)exp2.getOp(), TYPE_DAYS, "", -1);
					}
				}
				
				//externalize exp2
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(exp2.getName());
				row.add(exp2.getOp());
				row.add(exp2.getType());
				row.add(complexID++);
				row.add(exp.getRHS());
				row.add(exp2.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				
				if (name != null)
				{
					return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_DAYS, (Integer)exp1.getOp(), "." + name, meta), TYPE_INLINE, "." + name, (Integer)row.get(3));
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_DAYS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
				}
			}
			else if (exp2.getType() == TYPE_DAYS)
			{
				//already handled DATE +/- DAYS and DAYS +/- DAYS
				//so need to handle col(DATE) +/- DAYS
				//externalize exp1
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(exp1.getName());
				row.add(exp1.getOp());
				row.add(exp1.getType());
				row.add(complexID++);
				row.add(exp.getLHS());
				row.add(exp1.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				
				if (exp.getOp().equals("+"))
				{
					if (name != null)
					{
						return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, (Integer)exp2.getOp(), "." + name, meta), TYPE_INLINE, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else if (exp.getOp().equals("-"))
				{
					if (name != null)
					{
						return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, -1 * (Integer)exp2.getOp(), "." + name, meta), TYPE_INLINE, "." + name, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else
				{
					throw new ParseException("Only the + and - operators are allowed with type DAYS");
				}
			}
			
			//neither exp1 or exp2 are of type DATE or DAYS
			if (exp.getOp().equals("||"))
			{
				//string concatenation
				//externalize both exp1 and exp2
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(exp1.getName());
				row.add(exp1.getOp());
				row.add(exp1.getType());
				row.add(complexID++);
				row.add(exp.getLHS());
				row.add(exp1.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				int prereq1 = (Integer)row.get(3);
				row = new ArrayList<Object>();
				row.add(exp2.getName());
				row.add(exp2.getOp());
				row.add(exp2.getType());
				row.add(complexID++);
				row.add(exp.getRHS());
				row.add(exp2.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				int prereq2 = (Integer)row.get(3);
				ArrayList<Object> bottom = getBottomRow(row);
				bottom.remove(5);
				bottom.add(5, prereq1);
				
				if (name != null)
				{
					return new OperatorTypeAndName(new ConcatOperator(exp1.getName(), exp2.getName(), "." + name, meta), TYPE_INLINE, "." + name, prereq2);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(new ConcatOperator(exp1.getName(), exp2.getName(), name, meta), TYPE_INLINE, name, prereq2);
				}
			}
			
			// +,-,*, or /
			if (!(exp1.getOp() instanceof ExtendOperator) && !(exp2.getOp() instanceof ExtendOperator))
			{
				//externalize both exp1 and exp2
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(exp1.getName());
				row.add(exp1.getOp());
				row.add(exp1.getType());
				row.add(complexID++);
				row.add(exp.getLHS());
				row.add(exp1.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				int prereq1 = (Integer)row.get(3);
				row = new ArrayList<Object>();
				row.add(exp2.getName());
				row.add(exp2.getOp());
				row.add(exp2.getType());
				row.add(complexID++);
				row.add(exp.getRHS());
				row.add(exp2.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				int prereq2 = (Integer)row.get(3);
				ArrayList<Object> bottom = getBottomRow(row);
				bottom.remove(5);
				bottom.add(5, prereq1);
				
				if (name != null)
				{
					return new OperatorTypeAndName(new ExtendOperator(exp.getOp() + "," + exp1.getName() + "," + exp2.getName(), "." + name, meta), TYPE_INLINE, "." + name, prereq2);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(new ExtendOperator(exp.getOp() + "," + exp1.getName() + "," + exp2.getName(), name, meta), TYPE_INLINE, name, prereq2);
				}
			}
			else if (exp1.getOp() instanceof ExtendOperator && exp2.getOp() instanceof ExtendOperator)
			{
				ExtendOperator combined = null;
				if (name != null)
				{
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), "." + name, meta);
				}
				else
				{
					name = "._E" + suffix++;
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), name, meta);
				}
				
				int myPrereq = -1;
				if (exp1.getPrereq() != -1)
				{
					myPrereq = exp1.getPrereq();
				}
				
				if (exp2.getPrereq() != -1 && myPrereq == -1)
				{
					myPrereq = exp2.getPrereq();
				}
				else if (exp2.getPrereq() != -1 && myPrereq != -1)
				{
					ArrayList<Object> bottom = getBottomRow(getRow(myPrereq));
					bottom.remove(5);
					bottom.add(5, exp2.getPrereq());
				}
				
				if (name != null)
				{
					return new OperatorTypeAndName(combined, TYPE_INLINE, "." + name, myPrereq);
				}
				else
				{
					return new OperatorTypeAndName(combined, TYPE_INLINE, name, myPrereq);
				}
			}
			else if (exp1.getOp() instanceof ExtendOperator)
			{
				ExtendOperator combined = null;
				if (name != null)
				{
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + exp2.getName(), "." + name, meta);
				}
				else
				{
					name = "._E" + suffix++;
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + exp2.getName(), name, meta);
				}
				
				//externalize exp2
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(exp2.getName());
				row.add(exp2.getOp());
				row.add(exp2.getType());
				row.add(complexID++);
				row.add(exp.getRHS());
				row.add(exp2.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				ArrayList<Object> bottom = getBottomRow(row);
				bottom.remove(5);
				bottom.add(5, exp1.getPrereq());
				
				if (name != null)
				{
					return new OperatorTypeAndName(combined, TYPE_INLINE, "." + name, (Integer)row.get(3));
				}
				else
				{
					return new OperatorTypeAndName(combined, TYPE_INLINE, name, (Integer)row.get(3));
				}
			}
			else
			{
				//exp2 instanceof ExtendOperator
				ExtendOperator combined = null;
				if (name != null)
				{
					combined = new ExtendOperator(exp.getOp() + "," + exp1.getName() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), "." + name, meta);
				}
				else
				{
					name = "._E" + suffix++;
					combined = new ExtendOperator(exp.getOp() + "," + exp1.getName() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), name, meta);
				}
				
				//externalize exp1
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(exp1.getName());
				row.add(exp1.getOp());
				row.add(exp1.getType());
				row.add(complexID++);
				row.add(exp.getLHS());
				row.add(exp1.getPrereq());
				row.add(null);
				row.add(false);
				complex.add(row);
				ArrayList<Object> bottom = getBottomRow(row);
				bottom.remove(5);
				bottom.add(5, exp2.getPrereq());
				
				if (name != null)
				{
					return new OperatorTypeAndName(combined, TYPE_INLINE, "." + name, (Integer)row.get(3));
				}
				else
				{
					return new OperatorTypeAndName(combined, TYPE_INLINE, name, (Integer)row.get(3));
				}
			}
		}
		
		return null;
	}
	
	private ArrayList<Object> getBottomRow(ArrayList<Object> row)
	{
		while ((Integer)row.get(5) != -1)
		{
			row = getRow((Integer)row.get(5));
		}
		
		return row;
	}
	
	private ArrayList<Object> getRow(int num)
	{
		for (ArrayList<Object> row : complex)
		{
			if (((Integer)row.get(3)) == num)
			{
				return row;
			}
		}
		
		return null;
	}
	
	private Operator buildNGBExtends(Operator op, SubSelect sub) throws ParseException
	{
		for (ArrayList<Object> row : complex)
		{
			if ((Boolean)row.get(7) == false && (Integer)row.get(2) != TYPE_GROUPBY && ((SubSelect)row.get(6)).equals(sub))
			{
				Object o = buildNGBExtend(op, row);
				if (!(o instanceof Boolean))
				{
					op = (Operator)o;
				}
			}
		}
		
		return op;
	}
	
	private Object buildNGBExtend(Operator op, ArrayList<Object> row) throws ParseException
	{
		//colName, op, type, id, exp, prereq, done
		if (!((Integer)row.get(5)).equals(-1))
		{
			//get the row
			for (ArrayList<Object> r : complex)
			{
				if (r.get(3).equals(row.get(5)))
				{
					Object o = addComplexColumn(r, op);
					if (o instanceof Boolean)
					{
						return o;
					}
					break;
				}
			}
		}
				
		if ((Boolean)row.get(7) == true)
		{
			return op;
		}
				
		if ((Integer)row.get(2) == TYPE_INLINE)
		{
			Operator o = (Operator)row.get(1);
			row.remove(7);
			row.add(true);
			if (o instanceof YearOperator)
			{
				String name = o.getReferences().get(0);
				String type = op.getCols2Types().get(name);
				
				if (type == null)
				{
					throw new ParseException("A reference to column " + name + " was unable to be resolved.");
				}
				
				if (!type.equals("DATE"))
				{
					throw new ParseException("The YEAR() function cannot be used on a non-DATE column");
				}
			}
			else if (o instanceof SubstringOperator)
			{
				String name = o.getReferences().get(0);
				String type = op.getCols2Types().get(name);
				
				if (type == null)
				{
					throw new ParseException("A reference to column " + name + " was unable to be resolved.");
				}
				
				if (!type.equals("CHAR"))
				{
					throw new ParseException("The SUBSTRING() function cannot be used on a non-character data");
				}
			}
			else if (o instanceof DateMathOperator)
			{
				String name = o.getReferences().get(0);
				String type = op.getCols2Types().get(name);
				
				if (type == null)
				{
					throw new ParseException("A reference to column " + name + " was unable to be resolved.");
				}
				
				if (!type.equals("DATE"))
				{
					throw new ParseException("A DATE was expected but a different data type was found");
				}
			}
			else if (o instanceof ConcatOperator)
			{
				String name1 = o.getReferences().get(0);
				String type1 = op.getCols2Types().get(name1);
				String name2 = o.getReferences().get(1);
				String type2 = op.getCols2Types().get(name2);
				
				if (type1 == null)
				{
					throw new ParseException("A reference to column " + name1 + " was unable to be resolved.");
				}
				
				if (type2 == null)
				{
					throw new ParseException("A reference to column " + name2 + " was unable to be resolved.");
				}
				
				if (!type1.equals("CHAR") || !type2.equals("CHAR"))
				{
					throw new ParseException("Concatenation is not supported for non-character data");
				}
			}
			else if (o instanceof ExtendOperator)
			{
				for (String name : o.getReferences())
				{
					String type = op.getCols2Types().get(name);
					
					if (type == null)
					{
						throw new ParseException("A reference to column " + name + " was unable to be resolved.");
					}
					
					if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
					{
					}
					else
					{
						throw new ParseException("A numeric data type was expected but a different data type was found");
					}
				}
			}
			
			try
			{
				o.add(op);
				return o;
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			//type group by
			return false;
		}
	}
	
	private Operator buildOperatorTreeFromSubSelect(SubSelect select) throws ParseException
	{
		Operator op = buildOperatorTreeFromFrom(select.getFrom());
		getComplexColumns(select.getSelect(), select);
		op = buildNGBExtends(op, select);
		if (select.getWhere() != null)
		{
			op = buildOperatorTreeFromWhere(select.getWhere(), op);
		}
		
		//handle groupBy
		if (select.getGroupBy() != null)
		{
			op = buildOperatorTreeFromGroupBy(select.getGroupBy(), op);
		}
		
		//handle extends that haven't been done
		op = buildNGBExtends(op, select);
		//handle having
		if (select.getHaving() != null)
		{
			op = buildOperatorTreeFromHaving(select.getHaving(), op);
		}
		//handle orderBy
		if (select.getOrderBy() != null)
		{
			op = buildOperatorTreeFromOrderBy(select.getOrderBy(), op);
		}
		
		op = buildOperatorTreeFromSelectClause(select.getSelect(), op);
				
		//handle fetchFirst
		if (select.getFetchFirst() != null)
		{
			op = buildOperatorTreeFromFetchFirst(select.getFetchFirst(), op);
		}
		return op;
	}
	
	private Operator buildOperatorTreeFromGroupBy(GroupBy groupBy, Operator op) throws ParseException
	{
		ArrayList<Column> cols = groupBy.getCols();
		ArrayList<String> vStr = new ArrayList<String>(cols.size());
		for (Column col : cols)
		{
			String colString = "";
			if (col.getTable() != null)
			{
				colString += col.getTable();
			}
			
			colString += ("." + col.getColumn());
			vStr.add(colString);
		}
		
		ArrayList<AggregateOperator> ops = new ArrayList<AggregateOperator>();
		for (ArrayList<Object> row : complex)
		{
			//colName, op, type, id, exp, prereq, done
			if ((Integer)row.get(2) == TYPE_GROUPBY)
			{
				AggregateOperator agop = (AggregateOperator)row.get(1);
				if (agop instanceof AvgOperator)
				{
					String col = agop.getInputColumn();
					String type = op.getCols2Types().get(col);
					
					if (type == null)
					{
						throw new ParseException("Column " + col + " was referenced but not found");
					}
					
					if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
					{
					}
					else
					{
						throw new ParseException("The argument to AVG() must be numeric");
					}
				}
				else if (agop instanceof CountOperator)
				{
					if (!agop.getInputColumn().equals(agop.outputColumn()))
					{
						String col = agop.getInputColumn();
						String type = op.getCols2Types().get(col);
						
						if (type == null)
						{
							throw new ParseException("Column " + col + " was referenced but not found");
						}
					}
				}
				else if (agop instanceof CountDistinctOperator)
				{
					if (!agop.getInputColumn().equals(agop.outputColumn()))
					{
						String col = agop.getInputColumn();
						String type = op.getCols2Types().get(col);
						
						if (type == null)
						{
							throw new ParseException("Column " + col + " was referenced but not found");
						}
					}
				}
				else if (agop instanceof MaxOperator)
				{
					String col = agop.getInputColumn();
					String type = op.getCols2Types().get(col);
					
					if (type == null)
					{
						throw new ParseException("Column " + col + " was referenced but not found");
					}
					
					if (type.equals("INT") || type.equals("LONG"))
					{
						((MaxOperator)agop).setIsInt(true);
					}
					else if (type.equals("FLOAT"))
					{
						((MaxOperator)agop).setIsInt(false);
					}
					else
					{
						throw new ParseException("The argument to MAX() must be numeric");
					}
				}
				else if (agop instanceof MinOperator)
				{
					String col = agop.getInputColumn();
					String type = op.getCols2Types().get(col);
					
					if (type == null)
					{
						throw new ParseException("Column " + col + " was referenced but not found");
					}
					
					if (type.equals("INT") || type.equals("LONG"))
					{
						((MinOperator)agop).setIsInt(true);
					}
					else if (type.equals("FLOAT"))
					{
						((MinOperator)agop).setIsInt(false);
					}
					else
					{
						throw new ParseException("The argument to MIN() must be numeric");
					}
				}
				else if (agop instanceof SumOperator)
				{
					String col = agop.getInputColumn();
					String type = op.getCols2Types().get(col);
					
					if (type == null)
					{
						throw new ParseException("Column " + col + " was referenced but not found");
					}
					
					if (type.equals("INT") || type.equals("LONG"))
					{
						((SumOperator)agop).setIsInt(true);
					}
					else if (type.equals("FLOAT"))
					{
						((SumOperator)agop).setIsInt(false);
					}
					else
					{
						throw new ParseException("The argument to SUM() must be numeric");
					}
				}
				
				ops.add(agop);
				row.remove(7);
				row.add(true);
			}
		}
		
		try
		{
			MultiOperator multi = new MultiOperator(ops, vStr, meta, false);
			multi.add(op);
			return multi;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private Operator buildOperatorTreeFromSelectClause(SelectClause select, Operator op) throws ParseException
	{
		if (!select.isSelectStar())
		{
			ArrayList<String> cols = new ArrayList<String>();
			ArrayList<SelectListEntry> selects = select.getSelectList();
			for (SelectListEntry entry : selects)
			{
				if (entry.isColumn())
				{
					Column col = entry.getColumn();
					String colName = "";
					if (col.getTable() != null)
					{
						colName += col.getTable();
					}
					
					colName += ("." + col.getColumn());
					cols.add(colName);
				}
				else
				{
					if (entry.getName() != null)
					{
						cols.add("." + entry.getName());
					}
					else
					{
						//unnamed complex column
						for (ArrayList<Object> row : complex)
						{
							if (row.get(4).equals(entry.getExpression()))
							{
								cols.add((String)row.get(0));
							}
						}
					}
				}
			}
			
			for (String col : cols)
			{
				Integer pos = op.getCols2Pos().get(col);
				if (pos == null)
				{
					throw new ParseException("Column " + col + " was not found.");
				}
			}
			
			ReorderOperator reorder = new ReorderOperator(cols, meta);
			try
			{
				reorder.add(op);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			
			op = reorder;
		}
		
		if (!select.isSelectAll())
		{
			UnionOperator distinct = new UnionOperator(true, meta);
			try
			{
				distinct.add(op);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			op = distinct;
		}
		
		return op;
	}
	
	private Operator buildOperatorTreeFromHaving(Having having, Operator op) throws ParseException
	{
		SearchCondition search = having.getSearch();
		return buildOperatorTreeFromSearchCondition(search, op);
	}
	
	private Operator buildOperatorTreeFromOrderBy(OrderBy orderBy, Operator op) throws ParseException
	{
		ArrayList<SortKey> keys = orderBy.getKeys();
		ArrayList<String> columns = new ArrayList<String>(keys.size());
		ArrayList<Boolean> orders = new ArrayList<Boolean>(keys.size());
		
		for (SortKey key : keys)
		{
			String colStr = null;
			if (key.isColumn())
			{
				Column col = key.getColumn();
				if (col.getTable() != null)
				{	
					colStr = col.getTable() + "." + col.getColumn();
					int matches = 0;
					for (String c : op.getPos2Col().values())
					{
						if (c.equals(colStr))
						{
							matches++;
						}
					}
					
					if (matches == 0)
					{
						throw new ParseException("Column " + colStr + " does not exist!");
					}
					else if (matches > 1)
					{
						throw new ParseException("Column " + colStr + " is ambiguous!");
					}
				}
				else
				{
					int matches = 0;
					String table = null;
					for (String c : op.getPos2Col().values())
					{
						String p1 = c.substring(0, c.indexOf('.'));
						String p2 = c.substring(c.indexOf('.') + 1);
						
						if (p2.equals(col.getColumn()))
						{
							matches++;
							table = p1;
						}
					}
					
					if (matches == 0)
					{
						throw new ParseException("Column " + col.getColumn() + " does not exist");
					}
					else if (matches > 1)
					{
						throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
					}
					
					colStr = table + "." + col.getColumn();
				}
				
				columns.add(colStr);
				orders.add(key.getDirection());
			}
			else
			{
				//handle numbered keys
				colStr = op.getPos2Col().get(new Integer(key.getNum()));
				columns.add(colStr);
				orders.add(key.getDirection());
			}
		}
		
		try
		{
			SortOperator sort = new SortOperator(columns, orders, meta);
			sort.add(op);
			return sort;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private Operator buildOperatorTreeFromWhere(Where where, Operator op) throws ParseException
	{
		SearchCondition search = where.getSearch();
		return buildOperatorTreeFromSearchCondition(search, op);
	}
	
	private Operator buildOperatorTreeFromSearchCondition(SearchCondition search, Operator op) throws ParseException
	{
		SearchClause clause = search.getClause();
		if (search.getConnected() != null && search.getConnected().size() > 0)
		{
			convertToCNF(search);
		}
		
		if (search.getConnected() != null && search.getConnected().size() > 0 && search.getConnected().get(0).isAnd())
		{
			if (search.getClause().getPredicate() == null)
			{
				op = buildOperatorTreeFromSearchCondition(search.getClause().getSearch(), op);
			}
			else
			{
				ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
				op = buildOperatorTreeFromSearchCondition(new SearchCondition(search.getClause(), cscs), op);
			}
			
			for (ConnectedSearchClause csc : search.getConnected())
			{
				if (csc.getSearch().getPredicate() == null)
				{
					op = buildOperatorTreeFromSearchCondition(csc.getSearch().getSearch(), op);
				}
				else
				{
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					op = buildOperatorTreeFromSearchCondition(new SearchCondition(csc.getSearch(), cscs), op);
				}
			}
			
			return op;
		}
		
		if (search.getConnected() == null || search.getConnected().size() == 0)
		{
			boolean negated = clause.getNegated();
			if (clause.getPredicate() != null)
			{
				Predicate pred = clause.getPredicate();
				if (pred instanceof ExistsPredicate)
				{
					if (isCorrelated(((ExistsPredicate)pred).getSelect()))
					{
						SearchCondition join = rewriteCorrelatedSubSelect(((ExistsPredicate)pred).getSelect(), op); //update select list and search conditions, and maybe group by
						if (negated)
						{
							return connectWithAntiJoin(op, buildOperatorTreeFromSubSelect(((ExistsPredicate)pred).getSelect()), join);
						}
						else
						{
							return connectWithSemiJoin(op, buildOperatorTreeFromSubSelect(((ExistsPredicate)pred).getSelect()), join);
						}
					}
					else
					{
						throw new ParseException("An EXISTS predicate must be correlated");
					}
				}
				Expression lhs = pred.getLHS();
				String o = pred.getOp();
				Expression rhs = pred.getRHS();
				
				if (o.equals("IN") || o.equals("NI"))
				{
					String lhsStr = null;
					String rhsStr = null;
					if (!rhs.isList())
					{
						if (lhs.isLiteral())
						{
							Literal literal = lhs.getLiteral();
							if (literal.isNull())
							{
								//TODO
							}
							Object value = literal.getValue();
							if (value instanceof String)
							{
								lhsStr = "'" + value.toString() + "'";
							}
							else
							{
								lhsStr = value.toString();
							}
						}
						else if (lhs.isColumn())
						{
							Column col = lhs.getColumn();
							//is column unambiguous?
							if (col.getTable() != null)
							{
								lhsStr = col.getTable() + "." + col.getColumn();
								int matches = 0;
								for (String c : op.getPos2Col().values())
								{
									if (c.equals(lhsStr))
									{
										matches++;
									}
								}
							
								if (matches == 0)
								{
									throw new ParseException("Column " + lhsStr + " does not exist!");
								}
								else if (matches > 1)
								{
									throw new ParseException("Column " + lhsStr + " is ambiguous!");
								}
							}
							else
							{
								int matches = 0;
								String table = null;
								for (String c : op.getPos2Col().values())
								{
									String p1 = c.substring(0, c.indexOf('.'));
									String p2 = c.substring(c.indexOf('.') + 1);
								
									if (p2.equals(col.getColumn()))
									{
										matches++;
										table = p1;
									}
								}
							
								if (matches == 0)
								{
									//could be a complex column
									for (ArrayList<Object> row : complex)
									{
										//colName, op, type, id, exp, prereq, done
										if (row.get(0).equals("." + col.getColumn()))
										{
											//its a match
											matches++;
										}
									}
								
									if (matches == 0)
									{
										throw new ParseException("Column " + col.getColumn() + " does not exist");
									}
									else if (matches > 1)
									{
										throw new ParseException("Column " + lhsStr + " is ambiguous!");
									}
								
									for (ArrayList<Object> row : complex)
									{
										//colName, op, type, id, exp, prereq, done
										if (row.get(0).equals("." + col.getColumn()))
										{
											if (((Boolean)row.get(7)).equals(true))
											{
												throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
											}
										
											op = addComplexColumn(row, op);
										}
									}
								}
								else if (matches > 1)
								{
									throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
								}
							
								lhsStr = table + "." + col.getColumn();
							}
						}
						else if (lhs.isCountStar())
						{
							throw new ParseException("Count(*) cannot be used in this context!");
						}
						else if (lhs.isList())
						{
							throw new ParseException("A list cannot be used in this context!");
						}
						else if (lhs.isSelect())
						{
							SubSelect sub = lhs.getSelect();
							if (isCorrelated(sub))
							{
								lhsStr = getOneCol(sub);
								SearchCondition join = rewriteCorrelatedSubSelect(sub, op); //update select list and search conditions
								ProductOperator product = new ProductOperator(meta);
								try
								{
									product.add(op);
									product.add(buildOperatorTreeFromSubSelect(sub));
								}
								catch(Exception e)
								{
									throw new ParseException(e.getMessage());
								}
								op = buildOperatorTreeFromSearchCondition(join, product);
							}
							else
							{
								ProductOperator extend = new ProductOperator(meta);
								if (!ensuresOnlyOneRow(sub))
								{
									throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
								}
								Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
								if (rhs2.getCols2Pos().size() != 1)
								{
									throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
								}
								try
								{
									extend.add(op);
									extend.add(rhs2);
								}
								catch(Exception e)
								{
									throw new ParseException(e.getMessage());
								}
								op = extend;
								lhsStr = rhs2.getPos2Col().get(0);
							}
						}
						else
						{
							//check to see if complex already contains this expression
							boolean found = false;
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, sub, done
								if (row.get(4).equals(rhs))
								{
									if ((Boolean)row.get(7) == true)
									{
										//found it
										found = true;
										lhsStr = (String)row.get(0);
										break;
									}
								}
							}
						
							if (!found)
							{
								//if not, build it
								OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null);
								if (otan.getType() == TYPE_DAYS)
								{
									throw new ParseException("A type of DAYS is not allowed for a predicate");
								}
							
								if (otan.getType() == TYPE_DATE)
								{
									SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
									String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
									String name = "._E" + suffix++;
									ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
									ArrayList<Object> row = new ArrayList<Object>();
									row.add(name);
									row.add(operator);
									row.add(TYPE_INLINE);
									row.add(complexID++);
									row.add(lhs);
									row.add(-1);
									row.add(null);
									row.add(false);
									complex.add(row);
									op = addComplexColumn(row, op);
									lhsStr = name;
								}
								else
								{
									//colName, op, type, id, exp, prereq, sub, done
									ArrayList<Object> row = new ArrayList<Object>();
									row.add(otan.getName());
									row.add(otan.getOp());
									row.add(otan.getType());
									row.add(complexID++);
									row.add(lhs);
									row.add(otan.getPrereq());
									row.add(null);
									row.add(false);
									complex.add(row);
									op = addComplexColumn(row, op);
									lhsStr = otan.getName();
								}
							}
						}
					}
					
					if (rhs.isLiteral())
					{
						throw new ParseException("A literal value cannot be used in this context");
					}
					else if (rhs.isColumn())
					{
						throw new ParseException("A column cannot be used in this context");
					}
					else if (rhs.isCountStar())
					{
						throw new ParseException("Count(*) cannot be used in this context!");
					}
					else if (rhs.isList())
					{
						if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "E", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							ArrayList<ConnectedSearchClause> ss = new ArrayList<ConnectedSearchClause>();
							int i = 1;
							while (i < list.size())
							{
								Expression exp = list.get(i);
								Predicate p = new Predicate(lhs, "E", exp);
								SearchClause s = new SearchClause(p, false);
								ConnectedSearchClause cs = new ConnectedSearchClause(s, false);
								ss.add(cs);
							}
							
							SearchCondition sc = new SearchCondition(s1, ss);
							return buildOperatorTreeFromSearchCondition(sc, op);
						}
						else
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "NE", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							ArrayList<ConnectedSearchClause> ss = new ArrayList<ConnectedSearchClause>();
							int i = 1;
							while (i < list.size())
							{
								Expression exp = list.get(i);
								Predicate p = new Predicate(lhs, "NE", exp);
								SearchClause s = new SearchClause(p, false);
								ConnectedSearchClause cs = new ConnectedSearchClause(s, true);
								ss.add(cs);
							}
							
							SearchCondition sc = new SearchCondition(s1, ss);
							return buildOperatorTreeFromSearchCondition(sc, op);
						}
					}
					else if (rhs.isSelect())
					{
						SubSelect sub = rhs.getSelect();
						if (isCorrelated(sub))
						{
							rhsStr = getOneCol(sub);
							SearchCondition join = rewriteCorrelatedSubSelect(sub, op); //update select list and search conditions
							if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
							{
								Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
								verifyTypes(lhsStr, op, rhsStr, rhs2);
								try
								{
									op = connectWithSemiJoin(op, rhs2, join, new Filter(lhsStr, "E", rhsStr));
								}
								catch(Exception e)
								{
									throw new ParseException(e.getMessage());
								}
							}
							else
							{
								Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
								verifyTypes(lhsStr, op, rhsStr, rhs2);
								try
								{
									op = connectWithAntiJoin(op, rhs2, join, new Filter(lhsStr, "E", rhsStr));
								}
								catch(Exception e)
								{
									throw new ParseException(e.getMessage());
								}
							}
							
							return op;
						}
						else
						{
							Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
							rhsStr = getOneCol(sub);
							verifyTypes(lhsStr, op, rhsStr, rhs2);
							if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
							{
								ArrayList<String> cols = new ArrayList<String>();
								cols.add(lhsStr);
								try
								{
									SemiJoinOperator join = new SemiJoinOperator(cols, meta);
									join.add(op);
									join.add(rhs2);
									return join;
								}
								catch(Exception e)
								{
									throw new ParseException(e.getMessage());
								}
							}
							else
							{
								ArrayList<String> cols = new ArrayList<String>();
								cols.add(lhsStr);
								try
								{
									AntiJoinOperator join = new AntiJoinOperator(cols, meta);
									join.add(op);
									join.add(rhs2);
									return join;
								}
								catch(Exception e)
								{
									throw new ParseException(e.getMessage());
								}
							}
						}
					}
					else
					{
						throw new ParseException("An expression cannot be used in this context");
					}
				}
				
				String lhsStr = null;
				String rhsStr = null;
				
				if (lhs.isLiteral())
				{
					Literal literal = lhs.getLiteral();
					if (literal.isNull())
					{
						//TODO
					}
					Object value = literal.getValue();
					if (value instanceof String)
					{
						lhsStr = "'" + value.toString() + "'";
					}
					else
					{
						lhsStr = value.toString();
					}
				}
				else if (lhs.isColumn())
				{
					Column col = lhs.getColumn();
					//is column unambiguous?
					if (col.getTable() != null)
					{
						lhsStr = col.getTable() + "." + col.getColumn();
						int matches = 0;
						for (String c : op.getPos2Col().values())
						{
							if (c.equals(lhsStr))
							{
								matches++;
							}
						}
						
						if (matches == 0)
						{
							throw new ParseException("Column " + lhsStr + " does not exist!");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + lhsStr + " is ambiguous!");
						}
					}
					else
					{
						int matches = 0;
						String table = null;
						for (String c : op.getPos2Col().values())
						{
							String p1 = c.substring(0, c.indexOf('.'));
							String p2 = c.substring(c.indexOf('.') + 1);
							
							if (p2.equals(col.getColumn()))
							{
								matches++;
								table = p1;
							}
						}
						
						if (matches == 0)
						{
							//could be a complex column
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									//its a match
									matches++;
								}
							}
							
							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous!");
							}
							
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}
									
									op = addComplexColumn(row, op);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
						}
						
						lhsStr = table + "." + col.getColumn();
					}
				}
				else if (lhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context!");
				}
				else if (lhs.isList())
				{
					throw new ParseException("A list cannot be used in this context!");
				}
				else if (lhs.isSelect())
				{
					SubSelect sub = lhs.getSelect();
					if (isCorrelated(sub))
					{
						lhsStr = getOneCol(sub);
						SearchCondition join = rewriteCorrelatedSubSelect(sub, op); //update select list and search conditions
						ProductOperator product = new ProductOperator(meta);
						try
						{
							product.add(op);
							product.add(buildOperatorTreeFromSubSelect(sub));
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = buildOperatorTreeFromSearchCondition(join, product);
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						lhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					//check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						//colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(rhs))
						{
							if ((Boolean)row.get(7) == true)
							{
								//found it
								found = true;
								lhsStr = (String)row.get(0);
								break;
							}
						}
					}
					
					if (!found)
					{
						//if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}
						
						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							String name = "._E" + suffix++;
							ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(name);
							row.add(operator);
							row.add(TYPE_INLINE);
							row.add(complexID++);
							row.add(lhs);
							row.add(-1);
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							lhsStr = name;
						}
						else
						{
							//colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(lhs);
							row.add(otan.getPrereq());
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							lhsStr = otan.getName();
						}
					}
				}
				
				//do the same for rhs
				if (rhs.isLiteral())
				{
					Literal literal = rhs.getLiteral();
					if (literal.isNull())
					{
						//TODO
					}
					Object value = literal.getValue();
					if (value instanceof String)
					{
						rhsStr = "'" + value.toString() + "'";
					}
					else
					{
						rhsStr = value.toString();
					}
				}
				else if (rhs.isColumn())
				{
					Column col = rhs.getColumn();
					//is column unambiguous?
					if (col.getTable() != null)
					{
						rhsStr = col.getTable() + "." + col.getColumn();
						int matches = 0;
						for (String c : op.getPos2Col().values())
						{
							if (c.equals(rhsStr))
							{
								matches++;
							}
						}
						
						if (matches == 0)
						{
							throw new ParseException("Column " + rhsStr + " does not exist!");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + rhsStr + " is ambiguous!");
						}
					}
					else
					{
						int matches = 0;
						String table = null;
						for (String c : op.getPos2Col().values())
						{
							String p1 = c.substring(0, c.indexOf('.'));
							String p2 = c.substring(c.indexOf('.') + 1);
							
							if (p2.equals(col.getColumn()))
							{
								matches++;
								table = p1;
							}
						}
						
						if (matches == 0)
						{
							//could be a complex column
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									//its a match
									matches++;
								}
							}
							
							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous!");
							}
							
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}
									
									op = addComplexColumn(row, op);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
						}
						
						rhsStr = table + "." + col.getColumn();
					}
				}
				else if (rhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context!");
				}
				else if (rhs.isList())
				{
					throw new ParseException("A list cannot be used in this context!");
				}
				else if (rhs.isSelect())
				{
					SubSelect sub = rhs.getSelect();
					if (isCorrelated(sub))
					{
						rhsStr = getOneCol(sub);
						SearchCondition join = rewriteCorrelatedSubSelect(sub, op); //update select list and search conditions
						ProductOperator product = new ProductOperator(meta);
						try
						{
							product.add(op);
							product.add(buildOperatorTreeFromSubSelect(sub));
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = buildOperatorTreeFromSearchCondition(join, product);
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						rhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					//check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						//colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(rhs))
						{
							if ((Boolean)row.get(7) == true)
							{
								//found it
								found = true;
								rhsStr = (String)row.get(0);
								break;
							}
						}
					}
					
					if (!found)
					{
						//if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(rhs, null);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}
						
						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							String name = "._E" + suffix++;
							ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(name);
							row.add(operator);
							row.add(TYPE_INLINE);
							row.add(complexID++);
							row.add(rhs);
							row.add(-1);
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							rhsStr = name;
						}
						else
						{
							//colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(rhs);
							row.add(otan.getPrereq());
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							rhsStr = otan.getName();
						}
					}
				}
				
				//add SelectOperator to top of tree
				verifyTypes(lhsStr, o, rhsStr, op);
				if (negated)
				{
					if (o.equals("E"))
					{
						o = "NE";
					}
					else if (o.equals("NE"))
					{
						o = "E";
					}
					else if (o.equals("G"))
					{
						o = "LE";
					}
					else if (o.equals("GE"))
					{
						o = "L";
					}
					else if (o.equals("L"))
					{
						o = "GE";
					}
					else if (o.equals("LE"))
					{
						o = "G";
					}
					else if (o.equals("LI"))
					{
						o = "NL";
					}
					else if (o.equals("NL"))
					{
						o = "LI";
					}
				}
				try
				{
					SelectOperator select = new SelectOperator(new Filter(lhsStr, o, rhsStr), meta);
					select.add(op);
					return select;
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
			else
			{
				   SearchCondition s = clause.getSearch();
				   if (!negated)
				   {
					   return buildOperatorTreeFromSearchCondition(s, op);
				   }
				   else
				   {
					   negateSearchCondition(s);
					   return buildOperatorTreeFromSearchCondition(s, op);
				   }
			}
		}
		else
		{
			//ored
			ArrayList<Filter> ors = new ArrayList<Filter>();
			ArrayList<SearchClause> preds = new ArrayList<SearchClause>();
			preds.add(search.getClause());
			for (ConnectedSearchClause csc : search.getConnected())
			{
				preds.add(csc.getSearch());
			}
			
			int i = 0;
			while (i < preds.size())
			{
				SearchClause sc = preds.get(i);
				boolean negated = sc.getNegated();
				Predicate pred = sc.getPredicate();
				if (pred instanceof ExistsPredicate)
				{
					throw new ParseException("Restriction: An EXISTS predicate cannot be used with a logical OR");
				}
				Expression lhs = pred.getLHS();
				String o = pred.getOp();
				Expression rhs = pred.getRHS();
				
				if (o.equals("IN") || o.equals("NI"))
				{
					String lhsStr = null;
					String rhsStr = null;
					if (!rhs.isList())
					{
						if (lhs.isLiteral())
						{
							Literal literal = lhs.getLiteral();
							if (literal.isNull())
							{
								//TODO
							}
							Object value = literal.getValue();
							if (value instanceof String)
							{
								lhsStr = "'" + value.toString() + "'";
							}
							else
							{
								lhsStr = value.toString();
							}
						}
						else if (lhs.isColumn())
						{
							Column col = lhs.getColumn();
							//is column unambiguous?
							if (col.getTable() != null)
							{
								lhsStr = col.getTable() + "." + col.getColumn();
								int matches = 0;
								for (String c : op.getPos2Col().values())
								{
									if (c.equals(lhsStr))
									{
										matches++;
									}
								}
							
								if (matches == 0)
								{
									throw new ParseException("Column " + lhsStr + " does not exist!");
								}
								else if (matches > 1)
								{
									throw new ParseException("Column " + lhsStr + " is ambiguous!");
								}
							}
							else
							{
								int matches = 0;
								String table = null;
								for (String c : op.getPos2Col().values())
								{
									String p1 = c.substring(0, c.indexOf('.'));
									String p2 = c.substring(c.indexOf('.') + 1);
								
									if (p2.equals(col.getColumn()))
									{
										matches++;
										table = p1;
									}
								}
							
								if (matches == 0)
								{
									//could be a complex column
									for (ArrayList<Object> row : complex)
									{
										//colName, op, type, id, exp, prereq, done
										if (row.get(0).equals("." + col.getColumn()))
										{
											//its a match
											matches++;
										}
									}
								
									if (matches == 0)
									{
										throw new ParseException("Column " + col.getColumn() + " does not exist");
									}
									else if (matches > 1)
									{
										throw new ParseException("Column " + lhsStr + " is ambiguous!");
									}
								
									for (ArrayList<Object> row : complex)
									{
										//colName, op, type, id, exp, prereq, done
										if (row.get(0).equals("." + col.getColumn()))
										{
											if (((Boolean)row.get(7)).equals(true))
											{
												throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
											}
										
											op = addComplexColumn(row, op);
										}
									}
								}
								else if (matches > 1)
								{
									throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
								}
							
								lhsStr = table + "." + col.getColumn();
							}
						}
						else if (lhs.isCountStar())
						{
							throw new ParseException("Count(*) cannot be used in this context!");
						}
						else if (lhs.isList())
						{
							throw new ParseException("A list cannot be used in this context!");
						}
						else if (lhs.isSelect())
						{
							throw new ParseException("Restriction: An IN predicate cannot contain a subselect if it is connected with a logical OR");
						}
						else
						{
							//check to see if complex already contains this expression
							boolean found = false;
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, sub, done
								if (row.get(4).equals(rhs))
								{
									if ((Boolean)row.get(7) == true)
									{
										//found it
										found = true;
										lhsStr = (String)row.get(0);
										break;
									}
								}
							}
						
							if (!found)
							{
								//if not, build it
								OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null);
								if (otan.getType() == TYPE_DAYS)
								{
									throw new ParseException("A type of DAYS is not allowed for a predicate");
								}
							
								if (otan.getType() == TYPE_DATE)
								{
									SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
									String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
									String name = "._E" + suffix++;
									ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
									ArrayList<Object> row = new ArrayList<Object>();
									row.add(name);
									row.add(operator);
									row.add(TYPE_INLINE);
									row.add(complexID++);
									row.add(lhs);
									row.add(-1);
									row.add(null);
									row.add(false);
									complex.add(row);
									op = addComplexColumn(row, op);
									lhsStr = name;
								}
								else
								{
									//colName, op, type, id, exp, prereq, sub, done
									ArrayList<Object> row = new ArrayList<Object>();
									row.add(otan.getName());
									row.add(otan.getOp());
									row.add(otan.getType());
									row.add(complexID++);
									row.add(lhs);
									row.add(otan.getPrereq());
									row.add(null);
									row.add(false);
									complex.add(row);
									op = addComplexColumn(row, op);
									lhsStr = otan.getName();
								}
							}
						}
					}
					
					if (rhs.isLiteral())
					{
						throw new ParseException("A literal value cannot be used in this context");
					}
					else if (rhs.isColumn())
					{
						throw new ParseException("A column cannot be used in this context");
					}
					else if (rhs.isCountStar())
					{
						throw new ParseException("Count(*) cannot be used in this context!");
					}
					else if (rhs.isList())
					{
						if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "E", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							preds.add(s1);
							ArrayList<ConnectedSearchClause> ss = new ArrayList<ConnectedSearchClause>();
							int j = 1;
							while (j < list.size())
							{
								Expression exp = list.get(j);
								Predicate p = new Predicate(lhs, "E", exp);
								SearchClause s = new SearchClause(p, false);
								preds.add(s);
								j++;
							}
							
							continue;
						}
						else
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "NE", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							preds.add(s1);
							int j = 1;
							while (j < list.size())
							{
								Expression exp = list.get(j);
								Predicate p = new Predicate(lhs, "NE", exp);
								SearchClause s = new SearchClause(p, false);
								preds.add(s);
								j++;
							}
							
							continue;
						}
					}
					else if (rhs.isSelect())
					{
						throw new ParseException("Restriction: An IN predicate cannot contain a subselect if it participates in a logical OR");
					}
					else
					{
						throw new ParseException("An expression cannot be used in this context");
					}
				}
				
				String lhsStr = null;
				String rhsStr = null;
				
				if (lhs.isLiteral())
				{
					Literal literal = lhs.getLiteral();
					if (literal.isNull())
					{
						//TODO
					}
					Object value = literal.getValue();
					if (value instanceof String)
					{
						lhsStr = "'" + value.toString() + "'";
					}
					else
					{
						lhsStr = value.toString();
					}
				}
				else if (lhs.isColumn())
				{
					Column col = lhs.getColumn();
					//is column unambiguous?
					if (col.getTable() != null)
					{
						lhsStr = col.getTable() + "." + col.getColumn();
						int matches = 0;
						for (String c : op.getPos2Col().values())
						{
							if (c.equals(lhsStr))
							{
								matches++;
							}
						}
						
						if (matches == 0)
						{
							throw new ParseException("Column " + lhsStr + " does not exist!");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + lhsStr + " is ambiguous!");
						}
					}
					else
					{
						int matches = 0;
						String table = null;
						for (String c : op.getPos2Col().values())
						{
							String p1 = c.substring(0, c.indexOf('.'));
							String p2 = c.substring(c.indexOf('.') + 1);
							
							if (p2.equals(col.getColumn()))
							{
								matches++;
								table = p1;
							}
						}
						
						if (matches == 0)
						{
							//could be a complex column
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									//its a match
									matches++;
								}
							}
							
							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous!");
							}
							
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}
									
									op = addComplexColumn(row, op);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
						}
						
						lhsStr = table + "." + col.getColumn();
					}
				}
				else if (lhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context!");
				}
				else if (lhs.isList())
				{
					throw new ParseException("A list cannot be used in this context!");
				}
				else if (lhs.isSelect())
				{
					SubSelect sub = lhs.getSelect();
					if (isCorrelated(sub))
					{
						throw new ParseException("Restriction: A correlated subquery cannot be used in a predicate that is part of a logical OR");
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						lhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					//check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						//colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(rhs))
						{
							if ((Boolean)row.get(7) == true)
							{
								//found it
								found = true;
								lhsStr = (String)row.get(0);
								break;
							}
						}
					}
					
					if (!found)
					{
						//if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}
						
						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							String name = "._E" + suffix++;
							ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(name);
							row.add(operator);
							row.add(TYPE_INLINE);
							row.add(complexID++);
							row.add(lhs);
							row.add(-1);
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							lhsStr = name;
						}
						else
						{
							//colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(lhs);
							row.add(otan.getPrereq());
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							lhsStr = otan.getName();
						}
					}
				}
				
				//do the same for rhs
				if (rhs.isLiteral())
				{
					Literal literal = rhs.getLiteral();
					if (literal.isNull())
					{
						//TODO
					}
					Object value = literal.getValue();
					if (value instanceof String)
					{
						rhsStr = "'" + value.toString() + "'";
					}
					else
					{
						rhsStr = value.toString();
					}
				}
				else if (rhs.isColumn())
				{
					Column col = rhs.getColumn();
					//is column unambiguous?
					if (col.getTable() != null)
					{
						rhsStr = col.getTable() + "." + col.getColumn();
						int matches = 0;
						for (String c : op.getPos2Col().values())
						{
							if (c.equals(rhsStr))
							{
								matches++;
							}
						}
						
						if (matches == 0)
						{
							throw new ParseException("Column " + rhsStr + " does not exist!");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + rhsStr + " is ambiguous!");
						}
					}
					else
					{
						int matches = 0;
						String table = null;
						for (String c : op.getPos2Col().values())
						{
							String p1 = c.substring(0, c.indexOf('.'));
							String p2 = c.substring(c.indexOf('.') + 1);
							
							if (p2.equals(col.getColumn()))
							{
								matches++;
								table = p1;
							}
						}
						
						if (matches == 0)
						{
							//could be a complex column
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									//its a match
									matches++;
								}
							}
							
							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous!");
							}
							
							for (ArrayList<Object> row : complex)
							{
								//colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}
									
									op = addComplexColumn(row, op);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous!");
						}
						
						rhsStr = table + "." + col.getColumn();
					}
				}
				else if (rhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context!");
				}
				else if (rhs.isList())
				{
					throw new ParseException("A list cannot be used in this context!");
				}
				else if (rhs.isSelect())
				{
					SubSelect sub = rhs.getSelect();
					if (isCorrelated(sub))
					{
						throw new ParseException("Restriction: A correlated subquery cannot be used in a predicate that is part of a logical OR");
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						rhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					//check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						//colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(rhs))
						{
							if ((Boolean)row.get(7) == true)
							{
								//found it
								found = true;
								rhsStr = (String)row.get(0);
								break;
							}
						}
					}
					
					if (!found)
					{
						//if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(rhs, null);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}
						
						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							String name = "._E" + suffix++;
							ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), name, meta);
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(name);
							row.add(operator);
							row.add(TYPE_INLINE);
							row.add(complexID++);
							row.add(rhs);
							row.add(-1);
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							rhsStr = name;
						}
						else
						{
							//colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(rhs);
							row.add(otan.getPrereq());
							row.add(null);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op);
							rhsStr = otan.getName();
						}
					}
				}
				
				//add SelectOperator to top of tree
				verifyTypes(lhsStr, o, rhsStr, op);
				if (negated)
				{
					if (o.equals("E"))
					{
						o = "NE";
					}
					else if (o.equals("NE"))
					{
						o = "E";
					}
					else if (o.equals("G"))
					{
						o = "LE";
					}
					else if (o.equals("GE"))
					{
						o = "L";
					}
					else if (o.equals("L"))
					{
						o = "GE";
					}
					else if (o.equals("LE"))
					{
						o = "G";
					}
					else if (o.equals("LI"))
					{
						o = "NL";
					}
					else if (o.equals("NL"))
					{
						o = "LI";
					}
					
					i++;
				}
				try
				{
					ors.add(new Filter(lhsStr, o, rhsStr));
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
			
			try
			{
				SelectOperator select = new SelectOperator(ors, meta);
				select.add(op);
				return select;
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
	}
	
	private void negateSearchCondition(SearchCondition s)
	{
		if (s.getConnected() != null && s.getConnected().size() > 0)
		{
			s.getClause().setNegated(!s.getClause().getNegated());
			for (ConnectedSearchClause clause : s.getConnected())
			{
				clause.setAnd(!clause.isAnd());
				clause.getSearch().setNegated(clause.getSearch().getNegated());
			}
			
			return;
		}
		else
		{
			s.getClause().setNegated(!s.getClause().getNegated());
		}
	}
	
	private Operator addComplexColumn(ArrayList<Object> row, Operator op) throws ParseException
	{
		//colName, op, type, id, exp, prereq, done
		if (!((Integer)row.get(5)).equals(-1))
		{
			//get the row
			for (ArrayList<Object> r : complex)
			{
				if (r.get(3).equals(row.get(5)))
				{
					op = addComplexColumn(r, op);
					break;
				}
			}
		}
		
		if ((Boolean)row.get(7) == true)
		{
			return op;
		}
		
		if ((Integer)row.get(2) == TYPE_INLINE)
		{
			Operator o = (Operator)row.get(1);
			row.remove(7);
			row.add(true);
			if (o instanceof YearOperator)
			{
				String name = o.getReferences().get(0);
				String type = op.getCols2Types().get(name);
				
				if (type == null)
				{
					throw new ParseException("A reference to column " + name + " was unable to be resolved.");
				}
				
				if (!type.equals("DATE"))
				{
					throw new ParseException("The YEAR() function cannot be used on a non-DATE column");
				}
			}
			else if (o instanceof SubstringOperator)
			{
				String name = o.getReferences().get(0);
				String type = op.getCols2Types().get(name);
				
				if (type == null)
				{
					throw new ParseException("A reference to column " + name + " was unable to be resolved.");
				}
				
				if (!type.equals("CHAR"))
				{
					throw new ParseException("The SUBSTRING() function cannot be used on a non-character data");
				}
			}
			else if (o instanceof DateMathOperator)
			{
				String name = o.getReferences().get(0);
				String type = op.getCols2Types().get(name);
				
				if (type == null)
				{
					throw new ParseException("A reference to column " + name + " was unable to be resolved.");
				}
				
				if (!type.equals("DATE"))
				{
					throw new ParseException("A DATE was expected but a different data type was found");
				}
			}
			else if (o instanceof ConcatOperator)
			{
				String name1 = o.getReferences().get(0);
				String type1 = op.getCols2Types().get(name1);
				String name2 = o.getReferences().get(1);
				String type2 = op.getCols2Types().get(name2);
				
				if (type1 == null)
				{
					throw new ParseException("A reference to column " + name1 + " was unable to be resolved.");
				}
				
				if (type2 == null)
				{
					throw new ParseException("A reference to column " + name2 + " was unable to be resolved.");
				}
				
				if (!type1.equals("CHAR") || !type2.equals("CHAR"))
				{
					throw new ParseException("Concatenation is not supported for non-character data");
				}
			}
			else if (o instanceof ExtendOperator)
			{
				for (String name : o.getReferences())
				{
					String type = op.getCols2Types().get(name);
					
					if (type == null)
					{
						throw new ParseException("A reference to column " + name + " was unable to be resolved.");
					}
					
					if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
					{
					}
					else
					{
						throw new ParseException("A numeric data type was expected but a different data type was found");
					}
				}
			}
			
			try
			{
				o.add(op);
				return o;
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			//type group by
			throw new ParseException("A WHERE clause cannot refer to an aggregate column. This must be done in the HAVING clause.");
		}
	}
	
	private void verifyTypes(String lhs, Operator lOp, String rhs, Operator rOp) throws ParseException
	{
		String lhsType = null;
		String rhsType = null;
		if (Character.isDigit(lhs.charAt(0)))
		{
			lhsType = "NUMBER";
		}
		else if (lhs.charAt(0) == '\'')
		{
			lhsType = "STRING";
		}
		else 
		{
			String type = lOp.getCols2Types().get(lhs);
			if (type == null)
			{
				throw new ParseException("The column " + lhs + " was not found");
			}
			else if (type.equals("CHAR"))
			{
				lhsType = "STRING";
			}
			else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
			{
				lhsType = "NUMBER";
			}
			else
			{
				lhsType = type;
			}
		}
		
		if (Character.isDigit(rhs.charAt(0)))
		{
			rhsType = "NUMBER";
		}
		else if (rhs.charAt(0) == '\'')
		{
			rhsType = "STRING";
		}
		else 
		{
			String type = rOp.getCols2Types().get(rhs);
			if (type == null)
			{
				throw new ParseException("The column " + rhs + " was not found");
			}
			else if (type.equals("CHAR"))
			{
				rhsType = "STRING";
			}
			else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
			{
				rhsType = "NUMBER";
			}
			else
			{
				rhsType = type;
			}
		}
		
		if (lhsType.equals("NUMBER") && rhsType.equals("NUMBER"))
		{
			return;
		}
		else if (lhsType.equals("STRING") && rhsType.equals("STRING"))
		{
			return;
		}
		else
		{
			throw new ParseException("Invalid comparison between " + lhsType + " and " + rhsType);
		}
	}
	
	private void verifyTypes(String lhs, String op, String rhs, Operator o) throws ParseException
	{
		String lhsType = null;
		String rhsType = null;
		if (Character.isDigit(lhs.charAt(0)))
		{
			lhsType = "NUMBER";
		}
		else if (lhs.charAt(0) == '\'')
		{
			lhsType = "STRING";
		}
		else 
		{
			String type = o.getCols2Types().get(lhs);
			if (type == null)
			{
				throw new ParseException("The column " + lhs + " was not found");
			}
			else if (type.equals("CHAR"))
			{
				lhsType = "STRING";
			}
			else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
			{
				lhsType = "NUMBER";
			}
			else
			{
				lhsType = type;
			}
		}
		
		if (Character.isDigit(rhs.charAt(0)))
		{
			rhsType = "NUMBER";
		}
		else if (rhs.charAt(0) == '\'')
		{
			rhsType = "STRING";
		}
		else 
		{
			String type = o.getCols2Types().get(rhs);
			if (type == null)
			{
				throw new ParseException("The column " + rhs + " was not found");
			}
			else if (type.equals("CHAR"))
			{
				rhsType = "STRING";
			}
			else if (type.equals("INT") || type.equals("LONG") || type.equals("FLOAT"))
			{
				rhsType = "NUMBER";
			}
			else
			{
				rhsType = type;
			}
		}
		
		if (lhsType.equals("NUMBER") && rhsType.equals("NUMBER"))
		{
			if (op.equals("E") || op.equals("NE") || op.equals("G") || op.equals("GE") || op.equals("L") || op.equals("LE"))
			{
				return;
			}
			
			throw new ParseException("Invalid operator for comparing 2 numbers: " + op);
		}
		else if (lhsType.equals("STRING") && rhsType.equals("STRING"))
		{
			if (op.equals("LI") || op.equals("NL"))
			{
				return;
			}
			
			throw new ParseException("Invalid operator for comparing 2 string: " + op);
		}
		else
		{
			throw new ParseException("Invalid comparison between " + lhsType + " and " + rhsType);
		}
	}
	
	private Operator buildOperatorTreeFromFrom(FromClause from) throws ParseException
	{
		ArrayList<TableReference> tables = from.getTables();
		ArrayList<Operator> ops = new ArrayList<Operator>(tables.size());
		for (TableReference table : tables)
		{
			ops.add(buildOperatorTreeFromTableReference(table));
		}
		
		if (ops.size() == 1)
		{
			return ops.get(0);
		}
		
		Operator top = ops.get(0);
		ops.remove(0);
		while (ops.size() > 0)
		{
			Operator product = new ProductOperator(meta);
			try
			{
				product.add(top);
				product.add(ops.get(0));
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			ops.remove(0);
			top = product;
		}
		
		return top;
	}
	
	private void checkSizeOfNewCols(ArrayList<Column> newCols, Operator op) throws ParseException
	{
		if (newCols.size() != op.getPos2Col().size())
		{
			throw new ParseException("The common table expression has the wrong number of columns!");
		}
	}
	
	private Operator buildOperatorTreeFromTableReference(TableReference table) throws ParseException
	{
		if (table.isSingleTable())
		{
			return buildOperatorTreeFromSingleTable(table.getSingleTable());
		}
		
		if (table.isSelect())
		{
			Operator op = buildOperatorTreeFromFullSelect(table.getSelect());
			if (table.getSelect().getCols() != null && table.getSelect().getCols().size() > 0)
			{
				//rename cols
				checkSizeOfNewCols(table.getSelect().getCols(), op);
				ArrayList<String> original = new ArrayList<String>();
				ArrayList<String> newCols = new ArrayList<String>();
				if (table.getAlias() == null)
				{
					int i = 0;
					for (String col : op.getPos2Col().values())
					{
						original.add(col);
						String newCol = col.substring(0, col.indexOf('.'));
						newCol += ("." + table.getSelect().getCols().get(i));
						newCols.add(newCol);
						i++;
					}
					
					try
					{
						RenameOperator rename = new RenameOperator(original, newCols, meta);
						rename.add(op);
						return rename;
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					//rename cols and table
					int i = 0;
					for (String col : op.getPos2Col().values())
					{
						original.add(col);
						String newCol = table.getAlias();
						newCol += ("." + table.getSelect().getCols().get(i));
						newCols.add(newCol);
						i++;
					}
					
					try
					{
						RenameOperator rename = new RenameOperator(original, newCols, meta);
						rename.add(op);
						return rename;
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
			}
			else
			{
				//check for need to rename table
				if (table.getAlias() != null)
				{
					//rename table
					ArrayList<String> original = new ArrayList<String>();
					ArrayList<String> newCols = new ArrayList<String>();
					int i = 0;
					for (String col : op.getPos2Col().values())
					{
						original.add(col);
						String newCol = col.substring(col.indexOf('.') + 1);
						newCols.add(table.getAlias() + "." + newCol);
						i++;
					}
					
					try
					{
						RenameOperator rename = new RenameOperator(original, newCols, meta);
						rename.add(op);
						return rename;
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					return op;
				}
			}
		}
		
		Operator op1 = buildOperatorTreeFromTableReference(table.getLHS());
		Operator op2 = buildOperatorTreeFromTableReference(table.getRHS());
		String op = table.getOp();
		
		if (op.equals("CP"))
		{
			try
			{
				ProductOperator product = new ProductOperator(meta);
				product.add(op1);
				product.add(op2);
				if (table.getAlias() != null)
				{
					return handleAlias(table.getAlias(), product);
				}
				return product;
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		
		if (op.equals("I"))
		{
			try
			{
				ProductOperator product = new ProductOperator(meta);
				product.add(op1);
				product.add(op2);
				Operator o = buildOperatorTreeFromSearchCondition(table.getSearch(), product);
				if (table.getAlias() != null)
				{
					return handleAlias(table.getAlias(), o);
				}
			
				return o;
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		
		if (op.equals("L"))
		{
			//TODO
		}
		
		if (op.equals("R"))
		{
			//TODO
		}
		
		if (op.equals("F"))
		{
			//TODO
		}
		
		return null;
	}
	
	private Operator handleAlias(String alias, Operator op) throws ParseException
	{
		ArrayList<String> original = new ArrayList<String>();
		ArrayList<String> newCols = new ArrayList<String>();
		
		for (String col : op.getPos2Col().values())
		{
			original.add(col);
			String newCol = alias + "." + col.substring(col.indexOf('.') + 1);
			newCols.add(newCol);
		}
		
		try
		{
			RenameOperator rename = new RenameOperator(original, newCols, meta);
			rename.add(op);
			return rename;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private Operator buildOperatorTreeFromSingleTable(SingleTable table) throws ParseException
	{
		TableName name = table.getName();
		String schema, tblName;
		if (name.getSchema() == null)
		{
			schema = new MetaData(connection).getCurrentSchema();
			doesNotUseCurrentSchema = false;
		}
		else
		{
			schema = name.getSchema();
		}
		
		tblName = name.getName();
		
		if (!meta.verifyTableExistence(schema, tblName))
		{
			if (!meta.verifyViewExistence(schema, tblName))
			{
				throw new ParseException("Table or view " + schema + "." + tblName + " does not exist!");
			}
			
			SQLParser viewParser = new SQLParser(meta.getViewSQL(schema, tblName), connection);
			Operator op = null;
			try
			{
				op = viewParser.parse();
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			if (!viewParser.doesNotUseCurrentSchema())
			{
				doesNotUseCurrentSchema = false;
			}
			
			Operator retval = op.children().get(0);
			op.removeChild(retval);
			return retval;
		}
		
		TableScanOperator op = null;
		try
		{
			op = new TableScanOperator(schema, tblName, meta);
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
		
		if (table.getAlias() != null)
		{
			op.setAlias(table.getAlias());
		}
		
		return op;
	}
	
	private boolean isCorrelated(SubSelect select) throws ParseException
	{
		try
		{
			buildOperatorTreeFromSubSelect(select);
		}
		catch(ParseException e)
		{
			String msg = e.getMessage();
			if (msg.startsWith("Column ") && msg.endsWith(" does not exist!"))
			{
				//delete any entries in complex with sub=select
				int i = 0;
				while (i < complex.size())
				{
					ArrayList<Object> row = complex.get(i);
					if (row.get(6).equals(select))
					{
						complex.remove(i);
						continue;
					}
					
					i++;
				}
				return true;
			}
			
			throw new ParseException(e.getMessage());
		}
		
		//delete any entries in complex with sub=select
		int i = 0;
		while (i < complex.size())
		{
			ArrayList<Object> row = complex.get(i);
			if (row.get(6).equals(select))
			{
				complex.remove(i);
				continue;
			}
			
			i++;
		}
		return false;
	}
	
	private String getOneCol(SubSelect sub) throws ParseException
	{
		SelectClause select = sub.getSelect();
		if (select.isSelectStar())
		{
			throw new ParseException("SELECT * is not allowed in a subselect that must return 1 column");
		}
		
		ArrayList<SelectListEntry> list = select.getSelectList();
		if (list.size() != 1)
		{
			throw new ParseException("A subselect was used in a context where it must return 1 column, but instead returns a different number of columns");
		}
		
		SelectListEntry entry = list.get(0);
		if (entry.isColumn())
		{
			String retval = "";
			Column col = entry.getColumn();
			if (col.getTable() != null)
			{
				retval += col.getTable();
			}
			
			retval += ("." + col.getColumn());
			return retval;
		}
		
		return entry.getName();
	}
	
	private boolean ensuresOnlyOneRow(SubSelect sub)
	{
		if (sub.getFetchFirst() != null && sub.getFetchFirst().getNumber() == 1)
		{
			return true;
		}
		
		if (sub.getGroupBy() != null)
		{
			return false;
		}
		
		//if we find a column in the select list that uses a aggregation function, return true
		//else return false
		SelectClause select = sub.getSelect();
		ArrayList<SelectListEntry> list = select.getSelectList();
		for (SelectListEntry entry : list)
		{
			if (entry.isColumn())
			{
				return false;
			}
			
			Expression exp = entry.getExpression();
			if (exp.isCountStar())
			{
				return true;
			}
			
			ArrayList<Expression> args = null;
			
			if (exp.isFunction())
			{
				Function f = exp.getFunction();
				String method = f.getName();
				if (method.equals("AVG") || method.equals("COUNT") || method.equals("MAX") || method.equals("MIN") || method.equals("SUM"))
				{
					return true;
				}
				
				args = f.getArgs();
			}
			else if (exp.isExpression())
			{
				args = new ArrayList<Expression>(2);
				args.add(exp.getLHS());
				args.add(exp.getRHS());
			}
			
			if (doesAggregation(args))
			{
				return true;
			}
		}
		
		return false;
	}
	
	private boolean doesAggregation(ArrayList<Expression> args)
	{
		for (Expression exp : args)
		{
			if (doesAggregation(exp))
			{
				return true;
			}
		}
		
		return false;
	}
	
	private boolean doesAggregation(Expression exp)
	{
		if (exp.isCountStar())
		{
			return true;
		}
		
		ArrayList<Expression> args = null;
		
		if (exp.isFunction())
		{
			Function f = exp.getFunction();
			String method = f.getName();
			if (method.equals("AVG") || method.equals("COUNT") || method.equals("MAX") || method.equals("MIN") || method.equals("SUM"))
			{
				return true;
			}
			
			args = f.getArgs();
		}
		else if (exp.isExpression())
		{
			args = new ArrayList<Expression>(2);
			args.add(exp.getLHS());
			args.add(exp.getRHS());
		}
		
		return doesAggregation(args);
	}
	
	private void convertToCNF(SearchCondition s)
	{
		SearchClause sc = s.getClause();
		if (sc.getPredicate() == null)
		{
			pushInwardAnyNegation(sc);
		}
		
		if (s.getConnected() != null && s.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (csc.getSearch().getPredicate() == null)
				{
					pushInwardAnyNegation(csc.getSearch());
				}
			}
		}
		
		if (isCNF(s))
		{
			return;
		}
		
		if ((s.getConnected() == null || s.getConnected().size() == 0) && sc.getPredicate() == null)
		{
			SearchCondition s2 = sc.getSearch();
			s.setClause(s2.getClause());
			s.setConnected(s2.getConnected());
			convertToCNF(s);
			return;
		}
		
		if (allPredicates(s))
		{
			//we must have mixed ands and ors
			ArrayList<SearchClause> preds = new ArrayList<SearchClause>();
			SearchClause next = null;
			ArrayList<ConnectedSearchClause> remainder = new ArrayList<ConnectedSearchClause>();
			preds.add(sc);
			boolean found = false;
			
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (found)
				{
					remainder.add(csc);
					continue;
				}
				
				if (csc.isAnd())
				{
					preds.add(csc.getSearch());
				}
				else
				{
					found = true;
					next = csc.getSearch();
				}
			}
			
			ArrayList<SearchCondition> searches = new ArrayList<SearchCondition>();
			for (SearchClause a : preds)
			{
				ArrayList<ConnectedSearchClause> b = new ArrayList<ConnectedSearchClause>(1);
				ConnectedSearchClause b1 = new ConnectedSearchClause(next, false);
				b.add(b1);
				SearchCondition search = new SearchCondition(a, b);
				searches.add(search);
			}
			
			SearchCondition first = searches.remove(0);
			ArrayList<ConnectedSearchClause> b = new ArrayList<ConnectedSearchClause>();
			for (SearchCondition s2 : searches)
			{
				SearchClause sc2 = new SearchClause(s2, false);
				b.add(new ConnectedSearchClause(sc2, true));
			}
			
			SearchCondition s3 = new SearchCondition(new SearchClause(first, false), b);
			s.setClause(new SearchClause(s3, false));
			s.setConnected(remainder);
			convertToCNF(s);
			return;
		}
		
		if (s.getConnected().get(0).isAnd())
		{
			//starts with and
			if (sc.getPredicate() == null)
			{
				convertToCNF(sc.getSearch());
			}
			
			if (s.getConnected().get(0).getSearch().getPredicate() == null)
			{
				convertToCNF(s.getConnected().get(0).getSearch().getSearch());
			}
			
			if (sc.getPredicate() != null || (allPredicates(sc.getSearch()) && allAnd(sc.getSearch())))
			{
				//A is all anded predicates
				ArrayList<SearchClause> A = new ArrayList<SearchClause>();
				if (sc.getPredicate() != null)
				{
					A.add(sc);
				}
				else
				{
					SearchCondition s2 = sc.getSearch();
					A.add(s2.getClause());
					for (ConnectedSearchClause csc : s2.getConnected())
					{
						A.add(csc.getSearch());
					}
				}
				
				//figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					//B is all anded predicates
					if (sc2.getPredicate() != null)
					{
						A.add(sc2);
					}
					else
					{
						SearchCondition s2 = sc2.getSearch();
						A.add(s2.getClause());
						for (ConnectedSearchClause csc : s2.getConnected())
						{
							A.add(csc.getSearch());
						}
					}
					
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					SearchClause first = A.remove(0);
					for (SearchClause search : A)
					{
						cscs.add(new ConnectedSearchClause(search, true));
					}
					
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
				{
					//B all ors
					s.getConnected().remove(0);
					SearchClause first = A.remove(0);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchClause search : A)
					{
						cscs.add(new ConnectedSearchClause(search, true));
					}
					
					cscs.add(new ConnectedSearchClause(sc2, true));
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else
				{
					//A AND B CNF
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc2.getSearch().getConnected();
					if (cscs == null)
					{
						cscs = new ArrayList<ConnectedSearchClause>();
					}
					for (SearchClause search : A)
					{
						cscs.add(new ConnectedSearchClause(search, true));
					}
					sc2.getSearch().setConnected(cscs);
					s.setClause(sc2);
					convertToCNF(s);
					return;
				}
			}
			else if (allPredicates(sc.getSearch()) && allOrs(sc.getSearch()))
			{
				//A all ored preds
				//figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					//B is all anded predicates
					SearchClause first = sc;
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					if (sc2.getPredicate() != null)
					{
						s.getConnected().remove(0);
						cscs.add(new ConnectedSearchClause(sc2, true));
						s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
						convertToCNF(s);
						return;
					}
					
					cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), true));
					if (sc2.getSearch().getConnected() != null)
					{
						cscs.addAll(sc2.getSearch().getConnected());
					}
					s.getConnected().remove(0);
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
				{
					//B all ors
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					SearchClause first = sc;
					cscs.add(new ConnectedSearchClause(sc2, true));
					s.getConnected().remove(0);
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else
				{
					//A ors B CNF
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc2.getSearch().getConnected();
					if (cscs == null)
					{
						cscs = new ArrayList<ConnectedSearchClause>();
					}
					cscs.add(new ConnectedSearchClause(sc, true));
					sc2.getSearch().setConnected(cscs);
					s.setClause(sc2);
					convertToCNF(s);
					return;
				}
			}
			else
			{
				//A is CNF
				//figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					//A CNF B AND
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
					if (cscs == null)
					{
						cscs = new ArrayList<ConnectedSearchClause>();
					}
					
					
					if (sc2.getPredicate() != null)
					{
						cscs.add(new ConnectedSearchClause(sc2, true));
						sc.getSearch().setConnected(cscs);
						convertToCNF(s);
						return;
					}
					
					cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), true));
					if (sc2.getSearch().getConnected() != null)
					{
						cscs.addAll(sc2.getSearch().getConnected());
					}
					
					sc.getSearch().setConnected(cscs);
					convertToCNF(s);
					return;
				}
				else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
				{
					//A CNF B OR
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
					if (cscs == null)
					{
						cscs = new ArrayList<ConnectedSearchClause>();
					}
					
					sc.getSearch().setConnected(cscs);
					cscs.add(new ConnectedSearchClause(sc2, true));
					convertToCNF(s);
					return;
				}
				else
				{
					//A CNF B CNF
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
					if (cscs == null)
					{
						cscs = new ArrayList<ConnectedSearchClause>();
						sc.getSearch().setConnected(cscs);
					}
					
					cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), true));
					if (sc2.getSearch().getConnected() != null)
					{
						cscs.addAll(sc2.getSearch().getConnected());
					}
					
					convertToCNF(s);
					return;
				}
			}
		}
		else
		{
			//starts with or
			if (sc.getPredicate() == null)
			{
				convertToCNF(sc.getSearch());
			}
			
			if (s.getConnected().get(0).getSearch().getPredicate() == null)
			{
				convertToCNF(s.getConnected().get(0).getSearch().getSearch());
			}
			
			if (sc.getPredicate() != null || (allPredicates(sc.getSearch()) && allAnd(sc.getSearch())))
			{
				//A is all anded predicates
				ArrayList<SearchClause> A = new ArrayList<SearchClause>();
				if (sc.getPredicate() != null)
				{
					A.add(sc);
				}
				else
				{
					SearchCondition s2 = sc.getSearch();
					A.add(s2.getClause());
					for (ConnectedSearchClause csc : s2.getConnected())
					{
						A.add(csc.getSearch());
					}
				}
				
				//figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					//B is all anded predicates
					ArrayList<SearchClause> B = new ArrayList<SearchClause>();
					if (sc2.getPredicate() != null)
					{
						B.add(sc2);
					}
					else
					{
						SearchCondition s2 = sc2.getSearch();
						B.add(s2.getClause());
						for (ConnectedSearchClause csc : s2.getConnected())
						{
							B.add(csc.getSearch());
						}
					}
					
					ArrayList<SearchClause> searches = new ArrayList<SearchClause>();
					for (SearchClause p1 : A)
					{
						for (SearchClause p2 : B)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							cscs.add(new ConnectedSearchClause(p2, false));
							searches.add(new SearchClause(new SearchCondition(p1, cscs), false));
						}
					}
					
					SearchClause first = searches.remove(0);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchClause search : searches)
					{
						cscs.add(new ConnectedSearchClause(search, true));
					}
					s.getConnected().remove(0);
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
				{
					//B all ors
					ArrayList<SearchClause> searches = new ArrayList<SearchClause>();
					for (SearchClause a : A)
					{
						SearchClause search = sc2.clone();
						search.getSearch().getConnected().add(new ConnectedSearchClause(a, false));
						searches.add(search);
					}
					
					SearchClause first = searches.remove(0);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchClause search : searches)
					{
						cscs.add(new ConnectedSearchClause(search, true));
					}
					s.getConnected().remove(0);
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else
				{
					//A AND B CNF
					s.getConnected().remove(0);
					//build list of ored clauses in B
					SearchCondition scond = sc2.getSearch();
					ArrayList<SearchCondition> searches = new ArrayList<SearchCondition>();
					if (scond.getClause().getPredicate() != null)
					{
						ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
						searches.add(new SearchCondition(scond.getClause(), cscs));
					}
					else
					{
						searches.add(scond.getClause().getSearch());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						if (csc.getSearch().getPredicate() != null)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							searches.add(new SearchCondition(csc.getSearch(), cscs));
						}
						else
						{
							searches.add(csc.getSearch().getSearch());
						}
					}
					
					ArrayList<SearchCondition> AB = new ArrayList<SearchCondition>();
					for (SearchClause a : A)
					{
						for (SearchCondition b : searches)
						{
							SearchCondition ab = b.clone();
							ab.getConnected().add(new ConnectedSearchClause(a, false));
							AB.add(ab);
						}
					}
					
					SearchClause first = new SearchClause(AB.remove(0), false);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchCondition ab : AB)
					{
						cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
					}
					
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
			}
			else if (allPredicates(sc.getSearch()) && allOrs(sc.getSearch()))
			{
				//A all ored preds
				//figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					//B is all anded predicates
					ArrayList<SearchClause> B = new ArrayList<SearchClause>();
					if (sc2.getPredicate() != null)
					{
						B.add(sc2);
					}
					else
					{
						SearchCondition s2 = sc2.getSearch();
						B.add(s2.getClause());
						for (ConnectedSearchClause csc : s2.getConnected())
						{
							B.add(csc.getSearch());
						}
					}
					
					ArrayList<SearchClause> searches = new ArrayList<SearchClause>();
					for (SearchClause b : B)
					{
						SearchClause search = sc.clone();
						search.getSearch().getConnected().add(new ConnectedSearchClause(b, false));
						searches.add(search);
					}
					
					SearchClause first = searches.remove(0);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchClause search : searches)
					{
						cscs.add(new ConnectedSearchClause(search, true));
					}
					s.getConnected().remove(0);
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
				{
					//B all ors
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
					cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), false));
					cscs.addAll(sc2.getSearch().getConnected());
					convertToCNF(s);
					return;
				}
				else
				{
					//A ors B CNF
					s.getConnected().remove(0);
					//build list of ored clauses in B
					SearchCondition scond = sc2.getSearch();
					ArrayList<SearchCondition> searches = new ArrayList<SearchCondition>();
					if (scond.getClause().getPredicate() != null)
					{
						ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
						searches.add(new SearchCondition(scond.getClause(), cscs));
					}
					else
					{
						searches.add(scond.getClause().getSearch());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						if (csc.getSearch().getPredicate() != null)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							searches.add(new SearchCondition(csc.getSearch(), cscs));
						}
						else
						{
							searches.add(csc.getSearch().getSearch());
						}
					}
					
					for (SearchCondition search : searches)
					{
						search.getConnected().add(new ConnectedSearchClause(sc.getSearch().getClause(), false));
						for (ConnectedSearchClause csc : sc.getSearch().getConnected())
						{
							search.getConnected().add(csc);
						}
					}
					
					SearchClause first = new SearchClause(searches.remove(0), false);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchCondition ab : searches)
					{
						cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
					}
					
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
			}
			else
			{
				//A is CNF
				//figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					//A CNF B AND
					ArrayList<SearchClause> B = new ArrayList<SearchClause>();
					if (sc2.getPredicate() != null)
					{
						B.add(sc2);
					}
					else
					{
						SearchCondition s2 = sc2.getSearch();
						B.add(s2.getClause());
						for (ConnectedSearchClause csc : s2.getConnected())
						{
							B.add(csc.getSearch());
						}
					}
					
					s.getConnected().remove(0);
					//build list of ored clauses in A
					SearchCondition scond = sc.getSearch();
					ArrayList<SearchCondition> searches = new ArrayList<SearchCondition>();
					if (scond.getClause().getPredicate() != null)
					{
						ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
						searches.add(new SearchCondition(scond.getClause(), cscs));
					}
					else
					{
						searches.add(scond.getClause().getSearch());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						if (csc.getSearch().getPredicate() != null)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							searches.add(new SearchCondition(csc.getSearch(), cscs));
						}
						else
						{
							searches.add(csc.getSearch().getSearch());
						}
					}
					
					ArrayList<SearchCondition> AB = new ArrayList<SearchCondition>();
					for (SearchClause b : B)
					{
						for (SearchCondition a : searches)
						{
							SearchCondition ab = a.clone();
							ab.getConnected().add(new ConnectedSearchClause(b, false));
							AB.add(ab);
						}
					}
					
					SearchClause first = new SearchClause(AB.remove(0), false);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchCondition ab : AB)
					{
						cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
					}
					
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else if (allPredicates(sc2.getSearch()) && allOrs(sc2.getSearch()))
				{
					//A CNF B OR
					s.getConnected().remove(0);
					//build list of ored clauses in A
					SearchCondition scond = sc.getSearch();
					ArrayList<SearchCondition> searches = new ArrayList<SearchCondition>();
					if (scond.getClause().getPredicate() != null)
					{
						ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
						searches.add(new SearchCondition(scond.getClause(), cscs));
					}
					else
					{
						searches.add(scond.getClause().getSearch());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						if (csc.getSearch().getPredicate() != null)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							searches.add(new SearchCondition(csc.getSearch(), cscs));
						}
						else
						{
							searches.add(csc.getSearch().getSearch());
						}
					}
					
					for (SearchCondition search : searches)
					{
						search.getConnected().add(new ConnectedSearchClause(sc2.getSearch().getClause(), false));
						for (ConnectedSearchClause csc : sc2.getSearch().getConnected())
						{
							search.getConnected().add(csc);
						}
					}
					
					SearchClause first = new SearchClause(searches.remove(0), false);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchCondition ab : searches)
					{
						cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
					}
					
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
				else
				{
					//A CNF B CNF
					SearchCondition scond = sc.getSearch();
					ArrayList<SearchCondition> A = new ArrayList<SearchCondition>();
					if (scond.getClause().getPredicate() != null)
					{
						ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
						A.add(new SearchCondition(scond.getClause(), cscs));
					}
					else
					{
						A.add(scond.getClause().getSearch());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						if (csc.getSearch().getPredicate() != null)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							A.add(new SearchCondition(csc.getSearch(), cscs));
						}
						else
						{
							A.add(csc.getSearch().getSearch());
						}
					}
					
					scond = sc2.getSearch();
					ArrayList<SearchCondition> B = new ArrayList<SearchCondition>();
					if (scond.getClause().getPredicate() != null)
					{
						ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
						B.add(new SearchCondition(scond.getClause(), cscs));
					}
					else
					{
						B.add(scond.getClause().getSearch());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						if (csc.getSearch().getPredicate() != null)
						{
							ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
							B.add(new SearchCondition(csc.getSearch(), cscs));
						}
						else
						{
							B.add(csc.getSearch().getSearch());
						}
					}
					
					ArrayList<SearchCondition> AB = new ArrayList<SearchCondition>();
					for (SearchCondition a : A)
					{
						for (SearchCondition b : B)
						{
							SearchCondition ab = a.clone();
							ab.getConnected().add(new ConnectedSearchClause(b.getClause(), false));
							ab.getConnected().addAll(b.getConnected());
							AB.add(ab);
						}
					}
					
					SearchClause first = new SearchClause(AB.remove(0), false);
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					for (SearchCondition ab : AB)
					{
						cscs.add(new ConnectedSearchClause(new SearchClause(ab, false), true));
					}
					
					s.setClause(new SearchClause(new SearchCondition(first, cscs), false));
					convertToCNF(s);
					return;
				}
			}
		}
	}
	
	private boolean allAnd(SearchCondition s)
	{
		if (s.getConnected() != null && s.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (!csc.isAnd())
				{
					return false;
				}
			}
		}
		
		return true;
	}
	
	private boolean allOrs(SearchCondition s)
	{
		if (s.getConnected() != null && s.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (csc.isAnd())
				{
					return false;
				}
			}
		}
		
		return true;
	}
	
	private boolean allPredicates(SearchCondition s)
	{
		if (s.getClause().getPredicate() == null)
		{
			return false;
		}
		
		if (s.getConnected() != null && s.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (csc.getSearch().getPredicate() == null)
				{
					return false;
				}
			}
		}
		
		return true;
	}
	
	private boolean isCNF(SearchCondition s)
	{
		SearchClause sc = s.getClause();
		if (sc.getPredicate() != null && (s.getConnected() == null || s.getConnected().size() == 0))
		{
			//single predicate
			return true;
		}
		
		if (allOredPreds(s))
		{
			return true;
		}
		
		if (sc.getPredicate() == null)
		{
			if (sc.getSearch().getClause().getPredicate() == null)
			{
				return false;
			}
			
			if (sc.getSearch().getConnected() != null && sc.getSearch().getConnected().size() > 0)
			{
				for (ConnectedSearchClause csc : sc.getSearch().getConnected())
				{
					if (csc.isAnd())
					{
						return false;
					}
					
					if (csc.getSearch().getPredicate() == null)
					{
						return false;
					}
				}
			}
		}
		
		if (s.getConnected() != null & s.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (!csc.isAnd())
				{
					return false;
				}
				
				if (csc.getSearch().getPredicate() == null)
				{
					if (csc.getSearch().getSearch().getClause().getPredicate() == null)
					{
						return false;
					}
					
					if (csc.getSearch().getSearch().getConnected() != null && csc.getSearch().getSearch().getConnected().size() > 0)
					{
						for (ConnectedSearchClause csc2 : csc.getSearch().getSearch().getConnected())
						{
							if (csc2.isAnd())
							{
								return false;
							}
							
							if (csc2.getSearch().getPredicate() == null)
							{
								return false;
							}
						}
					}
				}
			}
		}
		
		return true;
	}
	
	private boolean allOredPreds(SearchCondition s)
	{
		SearchClause sc = s.getClause();
		boolean allOredPreds = true;
		if (sc.getPredicate() != null)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (csc.isAnd())
				{
					allOredPreds = false;
					break;
				}
				
				if (csc.getSearch().getPredicate() == null)
				{
					allOredPreds = false;
					break;
				}
			}
			
			if (allOredPreds)
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		
		return false;
	}
	
	private void pushInwardAnyNegation(SearchClause sc)
	{
		if (sc.getNegated())
		{
			negateSearchCondition(sc.getSearch());
			sc.setNegated(false);
		}
		
		SearchCondition s = sc.getSearch();
		sc = s.getClause();
		if (sc.getPredicate() == null)
		{
			pushInwardAnyNegation(sc);
		}
		
		if (s.getConnected() != null && s.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : s.getConnected())
			{
				if (csc.getSearch().getPredicate() == null)
				{
					pushInwardAnyNegation(csc.getSearch());
				}
			}
		}
	}
	
	private Operator connectWithSemiJoin(Operator left, Operator right, SearchCondition join) throws ParseException
	{
		//assume join is already cnf
		//and contains only columns
		HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
		HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
		if (join.getClause().getPredicate() != null)
		{
			Column l = join.getClause().getPredicate().getLHS().getColumn();
			Column r = join.getClause().getPredicate().getRHS().getColumn();
			String o = join.getClause().getPredicate().getOp();
			
			if (join.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			SearchCondition scond = join.getClause().getSearch();
			Column l = scond.getClause().getPredicate().getLHS().getColumn();
			Column r = scond.getClause().getPredicate().getRHS().getColumn();
			String o = scond.getClause().getPredicate().getOp();
			
			if (scond.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			
			for (ConnectedSearchClause csc : scond.getConnected())
			{
				l = csc.getSearch().getPredicate().getLHS().getColumn();
				r = csc.getSearch().getPredicate().getRHS().getColumn();
				o = csc.getSearch().getPredicate().getOp();
				
				if (csc.getSearch().getNegated())
				{
					if (o.equals("E"))
					{
						o = "NE";
					}
					else if (o.equals("NE"))
					{
						o = "E";
					}
					else if (o.equals("G"))
					{
						o = "LE";
					}
					else if (o.equals("GE"))
					{
						o = "L";
					}
					else if (o.equals("L"))
					{
						o = "GE";
					}
					else if (o.equals("LE"))
					{
						o = "G";
					}
					else if (o.equals("LI"))
					{
						o = "NL";
					}
					else if (o.equals("NL"))
					{
						o = "LI";
					}	
				}
				
				lhs = "";
				if (l.getTable() != null)
				{
					lhs += l.getTable();
				}
				
				lhs += ("." + l.getColumn());
				
				rhs = "";
				if (r.getTable() != null)
				{
					rhs += r.getTable();
				}
				
				rhs += ("." + r.getColumn());
				
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
		}
		
		hshm.add(hm);
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause sc : join.getConnected())
			{
				hm = new HashMap<Filter, Filter>();
				if (sc.getSearch().getPredicate() != null)
				{
					Column l = sc.getSearch().getPredicate().getLHS().getColumn();
					Column r = sc.getSearch().getPredicate().getRHS().getColumn();
					String o = sc.getSearch().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					SearchCondition scond = sc.getSearch().getSearch();
					Column l = scond.getClause().getPredicate().getLHS().getColumn();
					Column r = scond.getClause().getPredicate().getRHS().getColumn();
					String o = scond.getClause().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						l = csc.getSearch().getPredicate().getLHS().getColumn();
						r = csc.getSearch().getPredicate().getRHS().getColumn();
						o = csc.getSearch().getPredicate().getOp();
						
						if (csc.getSearch().getNegated())
						{
							if (o.equals("E"))
							{
								o = "NE";
							}
							else if (o.equals("NE"))
							{
								o = "E";
							}
							else if (o.equals("G"))
							{
								o = "LE";
							}
							else if (o.equals("GE"))
							{
								o = "L";
							}
							else if (o.equals("L"))
							{
								o = "GE";
							}
							else if (o.equals("LE"))
							{
								o = "G";
							}
							else if (o.equals("LI"))
							{
								o = "NL";
							}
							else if (o.equals("NL"))
							{
								o = "LI";
							}	
						}
						
						lhs = "";
						if (l.getTable() != null)
						{
							lhs += l.getTable();
						}
						
						lhs += ("." + l.getColumn());
						
						rhs = "";
						if (r.getTable() != null)
						{
							rhs += r.getTable();
						}
						
						rhs += ("." + r.getColumn());
						
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
					}
				}
				
				hshm.add(hm);
			}
		}
		
		try
		{
			SemiJoinOperator semi = new SemiJoinOperator(hshm, meta);
			semi.add(left);
			semi.add(right);
			return semi;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private Operator connectWithAntiJoin(Operator left, Operator right, SearchCondition join) throws ParseException
	{
		//assume join is already cnf
		//and contains only columns
		HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
		HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
		if (join.getClause().getPredicate() != null)
		{
			Column l = join.getClause().getPredicate().getLHS().getColumn();
			Column r = join.getClause().getPredicate().getRHS().getColumn();
			String o = join.getClause().getPredicate().getOp();
			
			if (join.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			SearchCondition scond = join.getClause().getSearch();
			Column l = scond.getClause().getPredicate().getLHS().getColumn();
			Column r = scond.getClause().getPredicate().getRHS().getColumn();
			String o = scond.getClause().getPredicate().getOp();
			
			if (scond.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			
			for (ConnectedSearchClause csc : scond.getConnected())
			{
				l = csc.getSearch().getPredicate().getLHS().getColumn();
				r = csc.getSearch().getPredicate().getRHS().getColumn();
				o = csc.getSearch().getPredicate().getOp();
				
				if (csc.getSearch().getNegated())
				{
					if (o.equals("E"))
					{
						o = "NE";
					}
					else if (o.equals("NE"))
					{
						o = "E";
					}
					else if (o.equals("G"))
					{
						o = "LE";
					}
					else if (o.equals("GE"))
					{
						o = "L";
					}
					else if (o.equals("L"))
					{
						o = "GE";
					}
					else if (o.equals("LE"))
					{
						o = "G";
					}
					else if (o.equals("LI"))
					{
						o = "NL";
					}
					else if (o.equals("NL"))
					{
						o = "LI";
					}	
				}
				
				lhs = "";
				if (l.getTable() != null)
				{
					lhs += l.getTable();
				}
				
				lhs += ("." + l.getColumn());
				
				rhs = "";
				if (r.getTable() != null)
				{
					rhs += r.getTable();
				}
				
				rhs += ("." + r.getColumn());
				
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
		}
		
		hshm.add(hm);
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause sc : join.getConnected())
			{
				hm = new HashMap<Filter, Filter>();
				if (sc.getSearch().getPredicate() != null)
				{
					Column l = sc.getSearch().getPredicate().getLHS().getColumn();
					Column r = sc.getSearch().getPredicate().getRHS().getColumn();
					String o = sc.getSearch().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					SearchCondition scond = sc.getSearch().getSearch();
					Column l = scond.getClause().getPredicate().getLHS().getColumn();
					Column r = scond.getClause().getPredicate().getRHS().getColumn();
					String o = scond.getClause().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						l = csc.getSearch().getPredicate().getLHS().getColumn();
						r = csc.getSearch().getPredicate().getRHS().getColumn();
						o = csc.getSearch().getPredicate().getOp();
						
						if (csc.getSearch().getNegated())
						{
							if (o.equals("E"))
							{
								o = "NE";
							}
							else if (o.equals("NE"))
							{
								o = "E";
							}
							else if (o.equals("G"))
							{
								o = "LE";
							}
							else if (o.equals("GE"))
							{
								o = "L";
							}
							else if (o.equals("L"))
							{
								o = "GE";
							}
							else if (o.equals("LE"))
							{
								o = "G";
							}
							else if (o.equals("LI"))
							{
								o = "NL";
							}
							else if (o.equals("NL"))
							{
								o = "LI";
							}	
						}
						
						lhs = "";
						if (l.getTable() != null)
						{
							lhs += l.getTable();
						}
						
						lhs += ("." + l.getColumn());
						
						rhs = "";
						if (r.getTable() != null)
						{
							rhs += r.getTable();
						}
						
						rhs += ("." + r.getColumn());
						
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
					}
				}
				
				hshm.add(hm);
			}
		}
		
		try
		{
			AntiJoinOperator anti = new AntiJoinOperator(hshm, meta);
			anti.add(left);
			anti.add(right);
			return anti;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private Operator connectWithSemiJoin(Operator left, Operator right, SearchCondition join, Filter filter) throws ParseException
	{
		//assume join is already cnf
		//and contains only columns
		HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
		HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
		if (join.getClause().getPredicate() != null)
		{
			Column l = join.getClause().getPredicate().getLHS().getColumn();
			Column r = join.getClause().getPredicate().getRHS().getColumn();
			String o = join.getClause().getPredicate().getOp();
			
			if (join.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			SearchCondition scond = join.getClause().getSearch();
			Column l = scond.getClause().getPredicate().getLHS().getColumn();
			Column r = scond.getClause().getPredicate().getRHS().getColumn();
			String o = scond.getClause().getPredicate().getOp();
			
			if (scond.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			
			for (ConnectedSearchClause csc : scond.getConnected())
			{
				l = csc.getSearch().getPredicate().getLHS().getColumn();
				r = csc.getSearch().getPredicate().getRHS().getColumn();
				o = csc.getSearch().getPredicate().getOp();
				
				if (csc.getSearch().getNegated())
				{
					if (o.equals("E"))
					{
						o = "NE";
					}
					else if (o.equals("NE"))
					{
						o = "E";
					}
					else if (o.equals("G"))
					{
						o = "LE";
					}
					else if (o.equals("GE"))
					{
						o = "L";
					}
					else if (o.equals("L"))
					{
						o = "GE";
					}
					else if (o.equals("LE"))
					{
						o = "G";
					}
					else if (o.equals("LI"))
					{
						o = "NL";
					}
					else if (o.equals("NL"))
					{
						o = "LI";
					}	
				}
				
				lhs = "";
				if (l.getTable() != null)
				{
					lhs += l.getTable();
				}
				
				lhs += ("." + l.getColumn());
				
				rhs = "";
				if (r.getTable() != null)
				{
					rhs += r.getTable();
				}
				
				rhs += ("." + r.getColumn());
				
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
		}
		
		hshm.add(hm);
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause sc : join.getConnected())
			{
				hm = new HashMap<Filter, Filter>();
				if (sc.getSearch().getPredicate() != null)
				{
					Column l = sc.getSearch().getPredicate().getLHS().getColumn();
					Column r = sc.getSearch().getPredicate().getRHS().getColumn();
					String o = sc.getSearch().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					SearchCondition scond = sc.getSearch().getSearch();
					Column l = scond.getClause().getPredicate().getLHS().getColumn();
					Column r = scond.getClause().getPredicate().getRHS().getColumn();
					String o = scond.getClause().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						l = csc.getSearch().getPredicate().getLHS().getColumn();
						r = csc.getSearch().getPredicate().getRHS().getColumn();
						o = csc.getSearch().getPredicate().getOp();
						
						if (csc.getSearch().getNegated())
						{
							if (o.equals("E"))
							{
								o = "NE";
							}
							else if (o.equals("NE"))
							{
								o = "E";
							}
							else if (o.equals("G"))
							{
								o = "LE";
							}
							else if (o.equals("GE"))
							{
								o = "L";
							}
							else if (o.equals("L"))
							{
								o = "GE";
							}
							else if (o.equals("LE"))
							{
								o = "G";
							}
							else if (o.equals("LI"))
							{
								o = "NL";
							}
							else if (o.equals("NL"))
							{
								o = "LI";
							}	
						}
						
						lhs = "";
						if (l.getTable() != null)
						{
							lhs += l.getTable();
						}
						
						lhs += ("." + l.getColumn());
						
						rhs = "";
						if (r.getTable() != null)
						{
							rhs += r.getTable();
						}
						
						rhs += ("." + r.getColumn());
						
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
					}
				}
				
				hshm.add(hm);
			}
		}
		
		hm = new HashMap<Filter, Filter>();
		hm.put(filter, filter);
		hshm.add(hm);
		
		try
		{
			SemiJoinOperator semi = new SemiJoinOperator(hshm, meta);
			semi.add(left);
			semi.add(right);
			return semi;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private Operator connectWithAntiJoin(Operator left, Operator right, SearchCondition join, Filter filter) throws ParseException
	{
		//assume join is already cnf
		//and contains only columns
		HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
		HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
		if (join.getClause().getPredicate() != null)
		{
			Column l = join.getClause().getPredicate().getLHS().getColumn();
			Column r = join.getClause().getPredicate().getRHS().getColumn();
			String o = join.getClause().getPredicate().getOp();
			
			if (join.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			SearchCondition scond = join.getClause().getSearch();
			Column l = scond.getClause().getPredicate().getLHS().getColumn();
			Column r = scond.getClause().getPredicate().getRHS().getColumn();
			String o = scond.getClause().getPredicate().getOp();
			
			if (scond.getClause().getNegated())
			{
				if (o.equals("E"))
				{
					o = "NE";
				}
				else if (o.equals("NE"))
				{
					o = "E";
				}
				else if (o.equals("G"))
				{
					o = "LE";
				}
				else if (o.equals("GE"))
				{
					o = "L";
				}
				else if (o.equals("L"))
				{
					o = "GE";
				}
				else if (o.equals("LE"))
				{
					o = "G";
				}
				else if (o.equals("LI"))
				{
					o = "NL";
				}
				else if (o.equals("NL"))
				{
					o = "LI";
				}	
			}
			
			String lhs = "";
			if (l.getTable() != null)
			{
				lhs += l.getTable();
			}
			
			lhs += ("." + l.getColumn());
			
			String rhs = "";
			if (r.getTable() != null)
			{
				rhs += r.getTable();
			}
			
			rhs += ("." + r.getColumn());
			
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch(Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			
			for (ConnectedSearchClause csc : scond.getConnected())
			{
				l = csc.getSearch().getPredicate().getLHS().getColumn();
				r = csc.getSearch().getPredicate().getRHS().getColumn();
				o = csc.getSearch().getPredicate().getOp();
				
				if (csc.getSearch().getNegated())
				{
					if (o.equals("E"))
					{
						o = "NE";
					}
					else if (o.equals("NE"))
					{
						o = "E";
					}
					else if (o.equals("G"))
					{
						o = "LE";
					}
					else if (o.equals("GE"))
					{
						o = "L";
					}
					else if (o.equals("L"))
					{
						o = "GE";
					}
					else if (o.equals("LE"))
					{
						o = "G";
					}
					else if (o.equals("LI"))
					{
						o = "NL";
					}
					else if (o.equals("NL"))
					{
						o = "LI";
					}	
				}
				
				lhs = "";
				if (l.getTable() != null)
				{
					lhs += l.getTable();
				}
				
				lhs += ("." + l.getColumn());
				
				rhs = "";
				if (r.getTable() != null)
				{
					rhs += r.getTable();
				}
				
				rhs += ("." + r.getColumn());
				
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch(Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
		}
		
		hshm.add(hm);
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause sc : join.getConnected())
			{
				hm = new HashMap<Filter, Filter>();
				if (sc.getSearch().getPredicate() != null)
				{
					Column l = sc.getSearch().getPredicate().getLHS().getColumn();
					Column r = sc.getSearch().getPredicate().getRHS().getColumn();
					String o = sc.getSearch().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					SearchCondition scond = sc.getSearch().getSearch();
					Column l = scond.getClause().getPredicate().getLHS().getColumn();
					Column r = scond.getClause().getPredicate().getRHS().getColumn();
					String o = scond.getClause().getPredicate().getOp();
					
					if (sc.getSearch().getNegated())
					{
						if (o.equals("E"))
						{
							o = "NE";
						}
						else if (o.equals("NE"))
						{
							o = "E";
						}
						else if (o.equals("G"))
						{
							o = "LE";
						}
						else if (o.equals("GE"))
						{
							o = "L";
						}
						else if (o.equals("L"))
						{
							o = "GE";
						}
						else if (o.equals("LE"))
						{
							o = "G";
						}
						else if (o.equals("LI"))
						{
							o = "NL";
						}
						else if (o.equals("NL"))
						{
							o = "LI";
						}	
					}
					
					String lhs = "";
					if (l.getTable() != null)
					{
						lhs += l.getTable();
					}
					
					lhs += ("." + l.getColumn());
					
					String rhs = "";
					if (r.getTable() != null)
					{
						rhs += r.getTable();
					}
					
					rhs += ("." + r.getColumn());
					
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch(Exception e)
					{
						throw new ParseException(e.getMessage());
					}
					
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						l = csc.getSearch().getPredicate().getLHS().getColumn();
						r = csc.getSearch().getPredicate().getRHS().getColumn();
						o = csc.getSearch().getPredicate().getOp();
						
						if (csc.getSearch().getNegated())
						{
							if (o.equals("E"))
							{
								o = "NE";
							}
							else if (o.equals("NE"))
							{
								o = "E";
							}
							else if (o.equals("G"))
							{
								o = "LE";
							}
							else if (o.equals("GE"))
							{
								o = "L";
							}
							else if (o.equals("L"))
							{
								o = "GE";
							}
							else if (o.equals("LE"))
							{
								o = "G";
							}
							else if (o.equals("LI"))
							{
								o = "NL";
							}
							else if (o.equals("NL"))
							{
								o = "LI";
							}	
						}
						
						lhs = "";
						if (l.getTable() != null)
						{
							lhs += l.getTable();
						}
						
						lhs += ("." + l.getColumn());
						
						rhs = "";
						if (r.getTable() != null)
						{
							rhs += r.getTable();
						}
						
						rhs += ("." + r.getColumn());
						
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch(Exception e)
						{
							throw new ParseException(e.getMessage());
						}
					}
				}
				
				hshm.add(hm);
			}
		}
		
		hm = new HashMap<Filter, Filter>();
		hm.put(filter, filter);
		hshm.add(hm);
		
		try
		{
			AntiJoinOperator anti = new AntiJoinOperator(hshm, meta);
			anti.add(left);
			anti.add(right);
			return anti;
		}
		catch(Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}
	
	private SearchCondition rewriteCorrelatedSubSelect(SubSelect select, Operator op) throws ParseException
	{
		//update select list and search conditions and group by
		SearchCondition retval = removeCorrelatedSearchCondition(select, op.getCols2Pos());
		updateSelectList(retval, select);
		updateGroupBy(retval, select);
		return retval;
	}
	
	private SearchCondition removeCorrelatedSearchCondition(SubSelect select, HashMap<String, Integer> cols2Pos) throws ParseException
	{
		SearchCondition search = select.getWhere().getSearch();
		convertToCNF(search);
		//for the clause and any connected
		ArrayList<ConnectedSearchClause> searches = new ArrayList<ConnectedSearchClause>();
		ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
		searches.add(new ConnectedSearchClause(search.getClause(), true));
		if (search.getConnected() != null && search.getConnected().size() > 0)
		{
			searches.addAll(search.getConnected());
		}
		
		for (ConnectedSearchClause csc : searches)
		{
			//if it's a predicate and contains a correlated column, add to retval
			if (csc.getSearch().getPredicate() != null)
			{
				Predicate p = csc.getSearch().getPredicate();
				if (containsCorrelatedCol(p.getLHS(), cols2Pos))
				{
					if (!p.getRHS().isColumn())
					{
						throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
					}
					
					cscs.add(csc);
				}
				else if (containsCorrelatedCol(p.getRHS(), cols2Pos))
				{
					if (!p.getLHS().isColumn())
					{
						throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
					}
					
					String op = p.getOp();
					if (op.equals("E"))
					{
					}
					else if (op.equals("NE"))
					{
					}
					else if (op.equals("G"))
					{
						op = "L";
					}
					else if (op.equals("GE"))
					{
						op = "LE";
					}
					else if (op.equals("L"))
					{
						op = "G";
					}
					else if (op.equals("LE"))
					{
						op = "GE";
					}
					else
					{
						throw new ParseException("Restriction: A correlated join condition in a correlated subquery cannot use the LIKE operator");
					}
					
					Predicate p2 = new Predicate(p.getRHS(), op, p.getLHS());
					csc.setSearch(new SearchClause(p2, csc.getSearch().getNegated()));
					cscs.add(csc);
				}
			}
			else
			{
				//if it's a search condition, every predicate in the ored group must contain a correlated col
				//add whole group to retval
				boolean correlated = false;
				SearchCondition s = csc.getSearch().getSearch();
				Predicate p = s.getClause().getPredicate();
				if (containsCorrelatedCol(p.getLHS(), cols2Pos))
				{
					correlated = true;
				}
				else if (containsCorrelatedCol(p.getRHS(), cols2Pos))
				{
					correlated = true;
				}
				
				if (!correlated)
				{
					for (ConnectedSearchClause csc2 : s.getConnected())
					{
						p = csc2.getSearch().getPredicate();
						if (containsCorrelatedCol(p.getLHS(), cols2Pos))
						{
							correlated = true;
							break;
						}
						else if (containsCorrelatedCol(p.getRHS(), cols2Pos))
						{
							correlated = true;
							break;
						}
					}
				}
				
				if (correlated)
				{
					//Verify that every predicate is correlated
					p = s.getClause().getPredicate();
					if (!containsCorrelatedCol(p.getLHS(), cols2Pos) && !containsCorrelatedCol(p.getRHS(), cols2Pos))
					{
						throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
					}
					
					for (ConnectedSearchClause csc2 : s.getConnected())
					{
						p = csc2.getSearch().getPredicate();
						if (!containsCorrelatedCol(p.getLHS(), cols2Pos) && !containsCorrelatedCol(p.getRHS(), cols2Pos))
						{
							throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
						}
					}
					
					ArrayList<ConnectedSearchClause> cscs2 = new ArrayList<ConnectedSearchClause>();
					p = s.getClause().getPredicate();
					if (containsCorrelatedCol(p.getLHS(), cols2Pos))
					{
						if (!p.getRHS().isColumn())
						{
							throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
						}
						
						cscs2.add(new ConnectedSearchClause(s.getClause(), false));
					}
					else 
					{
						if (!p.getLHS().isColumn())
						{
							throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
						}
						
						String op = p.getOp();
						if (op.equals("E"))
						{
						}
						else if (op.equals("NE"))
						{
						}
						else if (op.equals("G"))
						{
							op = "L";
						}
						else if (op.equals("GE"))
						{
							op = "LE";
						}
						else if (op.equals("L"))
						{
							op = "G";
						}
						else if (op.equals("LE"))
						{
							op = "GE";
						}
						else
						{
							throw new ParseException("Restriction: A correlated join condition in a correlated subquery cannot use the LIKE operator");
						}
						
						Predicate p2 = new Predicate(p.getRHS(), op, p.getLHS());
						cscs2.add(new ConnectedSearchClause(new SearchClause(p2, csc.getSearch().getNegated()), false));
					}
					
					for (ConnectedSearchClause csc2 : s.getConnected())
					{
						p = csc2.getSearch().getPredicate();
						if (containsCorrelatedCol(p.getLHS(), cols2Pos))
						{
							if (!p.getRHS().isColumn())
							{
								throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
							}
							
							cscs2.add(csc2);
						}
						else 
						{
							if (!p.getLHS().isColumn())
							{
								throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
							}
							
							String op = p.getOp();
							if (op.equals("E"))
							{
							}
							else if (op.equals("NE"))
							{
							}
							else if (op.equals("G"))
							{
								op = "L";
							}
							else if (op.equals("GE"))
							{
								op = "LE";
							}
							else if (op.equals("L"))
							{
								op = "G";
							}
							else if (op.equals("LE"))
							{
								op = "GE";
							}
							else
							{
								throw new ParseException("Restriction: A correlated join condition in a correlated subquery cannot use the LIKE operator");
							}
							
							Predicate p2 = new Predicate(p.getRHS(), op, p.getLHS());
							cscs2.add(new ConnectedSearchClause(new SearchClause(p2, csc2.getSearch().getNegated()), false));
						}
					}
					
					SearchClause first = cscs2.remove(0).getSearch();
					s.setClause(first);
					s.setConnected(cscs2);
					cscs.add(csc);
				}
			}
		}
		
		//update search and build retval
		for (ConnectedSearchClause csc : cscs)
		{
			searches.remove(csc);
		}
		
		SearchClause first = searches.remove(0).getSearch();
		search.setClause(first);
		search.setConnected(searches);
		
		first = cscs.remove(0).getSearch();
		return new SearchCondition(first, cscs);
	}
	
	private boolean containsCorrelatedCol(Expression exp, HashMap<String, Integer> cols2Pos) throws ParseException
	{
		if (exp.isColumn())
		{
			Column col = exp.getColumn();
			String colString = "";
			if (col.getTable() != null)
			{
				colString += col.getTable();
			}
			colString += ("." + col.getColumn());
			if (cols2Pos.containsKey(colString))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		
		//if it contains a correlated column, it is a ParseException, otherwise return false
		if (exp.isCountStar())
		{
			return false;
		}
		else if (exp.isExpression())
		{
			if (containsCorrelatedCol(exp.getLHS(), cols2Pos) || containsCorrelatedCol(exp.getRHS(), cols2Pos))
			{
				throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
			}
			else
			{
				return false;
			}
		}
		else if (exp.isFunction())
		{
			for (Expression e : exp.getFunction().getArgs())
			{
				if (containsCorrelatedCol(e, cols2Pos))
				{
					throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
				}
			}
			
			return false;
		}
		else if (exp.isList())
		{
			for (Expression e : exp.getList())
			{
				if (containsCorrelatedCol(e, cols2Pos))
				{
					throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
				}
			}
			
			return false;
		}
		else if (exp.isLiteral())
		{
			return false;
		}
		else if (exp.isSelect())
		{
			return false;
		}
		else
		{
			return false;
		}
	}
	
	private boolean updateGroupByNeeded(SubSelect select)
	{
		if (select.getGroupBy() != null || select.getHaving() != null)
		{
			return true;
		}
		
		SelectClause s = select.getSelect();
		ArrayList<SelectListEntry> list = s.getSelectList();
		for (SelectListEntry entry : list)
		{
			if (entry.isColumn())
			{
				continue;
			}
			
			Expression exp = entry.getExpression();
			if (containsAggregation(exp))
			{
				return true;
			}
		}
		
		Where where = select.getWhere();
		if (where != null)
		{
			SearchCondition search = where.getSearch();
			if (containsAggregation(search.getClause()))
			{
				return true;
			}
			
			if (search.getConnected() != null && search.getConnected().size() > 0)
			{
				for (ConnectedSearchClause csc : search.getConnected())
				{
					if (containsAggregation(csc.getSearch()))
					{
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	private boolean containsAggregation(SearchClause clause)
	{
		if (clause.getPredicate() != null)
		{
			Predicate p = clause.getPredicate();
			if (containsAggregation(p.getLHS()))
			{
				return true;
			}
			
			if (containsAggregation(p.getRHS()))
			{
				return true;
			}
			
			return false;
		}
		
		SearchCondition search = clause.getSearch();
		if (containsAggregation(search.getClause()))
		{
			return true;
		}
		
		if (search.getConnected() != null && search.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : search.getConnected())
			{
				if (containsAggregation(csc.getSearch()))
				{
					return true;
				}
			}
		}
		
		return false;
	}
	
	private boolean containsAggregation(Expression exp)
	{
		if (exp.isColumn())
		{
			return false;
		}
		else if (exp.isCountStar())
		{
			return false;
		}
		else if (exp.isExpression())
		{
			if (containsAggregation(exp.getLHS()))
			{
				return true;
			}
			
			if (containsAggregation(exp.getRHS()))
			{
				return true;
			}
			
			return false;
		}
		else if (exp.isFunction())
		{
			Function f = exp.getFunction();
			if (f.getName().equals("AVG") || f.getName().equals("COUNT") || f.getName().equals("MAX") || f.getName().equals("MIN") || f.getName().equals("SUM"))
			{
				return true;
			}
			
			
			for (Expression e : f.getArgs())
			{
				if (containsAggregation(e))
				{
					return true;
				}
			}
			
			return false;
		}
		else if (exp.isList())
		{
			for (Expression e : exp.getList())
			{
				if (containsAggregation(e))
				{
					return true;
				}
			}
			
			return false;
		}
		else if (exp.isLiteral())
		{
			return false;
		}
		else if (exp.isSelect())
		{
			return false;
		}
		else
		{
			return false;
		}
	}
	
	private void updateGroupBy(SearchCondition join, SubSelect select) throws ParseException
	{
		if (updateGroupByNeeded(select))
		{
			if (!allPredicates(join) || !allAnd(join))
			{
				throw new ParseException("Restriction: A correlated subquery is not allowed if it involves aggregation and also contains correlated predicates that are not all anded together");
			}
			
			//verify all predicates are also equijoins
			if (!isAllEquals(join))
			{
				throw new ParseException("Restriction: Correlated subqueries with aggregation can only have equality operators on the correlated predicates");
			}
			
			//if group by already exists, add to it
			if (select.getGroupBy() != null)
			{
				GroupBy groupBy = select.getGroupBy();
				groupBy.getCols().addAll(getRightColumns(join));
			}
			else
			{
				//otherwise create it
				select.setGroupBy(new GroupBy(getRightColumns(join)));
			}
		}
	}
	
	private boolean isAllEquals(SearchCondition join)
	{
		if (!join.getClause().getPredicate().getOp().equals("E"))
		{
			return false;
		}
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : join.getConnected())
			{
				if (!csc.getSearch().getPredicate().getOp().equals("E"))
				{
					return false;
				}
			}
		}
		
		return true;
	}
	
	private ArrayList<Column> getRightColumns(SearchCondition join)
	{
		ArrayList<Column> retval = new ArrayList<Column>();
		retval.add(join.getClause().getPredicate().getRHS().getColumn());
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : join.getConnected())
			{
				retval.add(csc.getSearch().getPredicate().getRHS().getColumn());
			}
		}
		
		return retval;
	}
	
	private void updateSelectList(SearchCondition join, SubSelect select)
	{
		ArrayList<Column> needed = new ArrayList<Column>();
		if (join.getClause().getPredicate() != null)
		{
			needed.add(join.getClause().getPredicate().getRHS().getColumn());
		}
		else
		{
			SearchCondition scond = join.getClause().getSearch();
			needed.add(scond.getClause().getPredicate().getRHS().getColumn());
			for (ConnectedSearchClause csc : scond.getConnected())
			{
				needed.add(csc.getSearch().getPredicate().getRHS().getColumn());
			}
		}
		
		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause sc : join.getConnected())
			{
				if (sc.getSearch().getPredicate() != null)
				{
					needed.add(sc.getSearch().getPredicate().getRHS().getColumn());
				}
				else
				{
					SearchCondition scond = sc.getSearch().getSearch();
					needed.add(scond.getClause().getPredicate().getRHS().getColumn());
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						needed.add(csc.getSearch().getPredicate().getRHS().getColumn());
					}
				}
			}
		}
		
		SelectClause sclause = select.getSelect();
		if (sclause.isSelectStar())
		{
			return;
		}
		
		ArrayList<SelectListEntry> list = sclause.getSelectList();
		for (Column col : needed)
		{
			String colName = "";
			if (col.getTable() != null)
			{
				colName += col.getTable();
			}
			colName += ("." + col.getColumn());
			boolean found = false;
			for (SelectListEntry entry : list)
			{
				if (entry.getName() != null && ("." + entry.getName()).equals(colName))
				{
					found = true;
					break;
				}
				else if (entry.getName() == null && entry.isColumn())
				{
					String colName2 = "";
					if (entry.getColumn().getTable() != null)
					{
						colName2 += entry.getColumn().getTable();
					}
					colName2 += ("." + entry.getColumn().getColumn());
					
					if (colName.equals(colName2))
					{
						found = true;
						break;
					}
				}
			}
			
			if (!found)
			{
				list.add(new SelectListEntry(col, null));
			}
		}
	}
}
