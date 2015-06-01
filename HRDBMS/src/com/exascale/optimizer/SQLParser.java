package com.exascale.optimizer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import com.exascale.exceptions.ParseException;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DateParser;
import com.exascale.tables.SQL;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

public class SQLParser
{
	public static final int TYPE_INLINE = 0;
	public static final int TYPE_GROUPBY = 1;
	public static final int TYPE_DATE = 2;
	public static final int TYPE_DAYS = 3;
	public static final int TYPE_MONTHS = 4;
	public static final int TYPE_YEARS = 5;
	private SQL sql;
	private final MetaData meta = new MetaData();
	private int suffix = 0;
	private int complexID = 0;

	private final ArrayList<ArrayList<Object>> complex = new ArrayList<ArrayList<Object>>();
	private ConnectionWorker connection;
	private boolean doesNotUseCurrentSchema = true;
	private boolean authorized = false;
	private Transaction tx;
	private int rewriteCounter = 0;

	public SQLParser()
	{
	}

	public SQLParser(String sql, ConnectionWorker connection, Transaction tx)
	{
		this.sql = new SQL(sql);
		this.connection = connection;
		this.tx = tx;
	}

	public static void printTree(Operator op, int indent)
	{
		String line = "";
		int i = 0;
		while (i < indent)
		{
			line += " ";
			i++;
		}

		line += op;
		HRDBMSWorker.logger.debug(line);

		if (op.children().size() > 0)
		{
			line = "";
			i = 0;
			while (i < indent)
			{
				line += " ";
				i++;
			}

			line += "(";
			HRDBMSWorker.logger.debug(line);

			for (Operator child : op.children())
			{
				printTree(child, indent + 3);
			}

			line = "";
			i = 0;
			while (i < indent)
			{
				line += " ";
				i++;
			}

			line += ")";
			HRDBMSWorker.logger.debug(line);
		}
	}

	public void authorize()
	{
		authorized = true;
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
		parser.setErrorHandler(new BailErrorStrategy());
		ParseTree tree = parser.select();
		SelectVisitorImpl visitor = new SelectVisitorImpl();
		SQLStatement stmt = (SQLStatement)visitor.visit(tree);

		if (stmt instanceof Select)
		{
			Operator op = buildOperatorTreeFromSelect((Select)stmt);
			RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
			retval.add(op);
			// printTree(op, 0); // DEBUG
			return retval;
		}

		if (stmt instanceof Insert)
		{
			Insert insert = (Insert)stmt;
			TableName table = insert.getTable();
			if (!authorized && table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("Catalog updates are not allowed");
			}
			Operator op = buildOperatorTreeFromInsert((Insert)stmt);
			return op;
		}

		if (stmt instanceof Update)
		{
			Update update = (Update)stmt;
			TableName table = update.getTable();
			if (!authorized && table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("Catalog updates are not allowed");
			}
			Operator op = buildOperatorTreeFromUpdate((Update)stmt);
			return op;
		}

		if (stmt instanceof Delete)
		{
			Delete delete = (Delete)stmt;
			TableName table = delete.getTable();
			if (!authorized && table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("Catalog updates are not allowed");
			}
			Operator op = buildOperatorTreeFromDelete((Delete)stmt);
			return op;
		}

		if (stmt instanceof Runstats)
		{
			Operator op = buildOperatorTreeFromRunstats((Runstats)stmt);
			return op;
		}

		if (stmt instanceof CreateTable)
		{
			CreateTable createTable = (CreateTable)stmt;
			TableName table = createTable.getTable();
			if (table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("You cannot create new tables in the SYS schema");
			}
			Operator op = buildOperatorTreeFromCreateTable((CreateTable)stmt);
			return op;
		}

		if (stmt instanceof DropTable)
		{
			DropTable dropTable = (DropTable)stmt;
			TableName table = dropTable.getTable();
			if (table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("You cannot drop tables in the SYS schema");
			}
			Operator op = buildOperatorTreeFromDropTable((DropTable)stmt);
			return op;
		}

		if (stmt instanceof CreateIndex)
		{
			Operator op = buildOperatorTreeFromCreateIndex((CreateIndex)stmt);
			return op;
		}

		if (stmt instanceof DropIndex)
		{
			Operator op = buildOperatorTreeFromDropIndex((DropIndex)stmt);
			return op;
		}

		if (stmt instanceof CreateView)
		{
			Operator op = buildOperatorTreeFromCreateView((CreateView)stmt);
			return op;
		}

		if (stmt instanceof DropView)
		{
			Operator op = buildOperatorTreeFromDropView((DropView)stmt);
			return op;
		}

		if (stmt instanceof Load)
		{
			Operator op = buildOperatorTreeFromLoad((Load)stmt);
			return op;
		}

		return null;
	}

	private Operator addComplexColumn(ArrayList<Object> row, Operator op, SubSelect sub) throws Exception
	{
		// colName, op, type, id, exp, prereq, done
		if (!((Integer)row.get(5)).equals(-1))
		{
			// get the row
			for (ArrayList<Object> r : complex)
			{
				if (r.get(3).equals(row.get(5)))
				{
					op = addComplexColumn(r, op, sub);
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
			if (o instanceof CaseOperator)
			{
				Expression exp = (Expression)row.get(4);
				ArrayList<HashSet<HashMap<Filter, Filter>>> alhshm = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
				// HRDBMSWorker.logger.debug("Build SC has " +
				// exp.getCases().size() + " cases to examine");
				for (Case c : exp.getCases())
				{
					Operator top = op;
					op = this.buildOperatorTreeFromSearchCondition(c.getCondition(), top, sub);
					if (op == top)
					{
						HRDBMSWorker.logger.debug("Build SC when building a CaseOperator did not do anything");
					}
					else if (!(op instanceof SelectOperator))
					{
						HRDBMSWorker.logger.debug("Build SC when building a CaseOperator did something, but it wasn't a SelectOperator");
					}
					// add filters to alhshm and remove from tree
					HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
					while (op instanceof SelectOperator)
					{
						HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
						for (Filter f : ((SelectOperator)op).getFilter())
						{
							hm.put(f, f);
						}

						hshm.add(hm);
						op = op.children().get(0);
						op.parent().removeChild(op);
					}

					Operator newTop = op;
					while (op != top)
					{
						if (op.children().size() > 1)
						{
							throw new ParseException("Invalid search condition in a case expression");
						}

						if (op instanceof SelectOperator)
						{
							throw new ParseException("Internal error: stranded select operator when processing a case expression");
						}

						op = op.children().get(0);
					}

					op = newTop;
					alhshm.add(hshm);
				}

				((CaseOperator)o).setFilters(alhshm);
				String type = ((CaseOperator)o).getType();
				ArrayList<Object> results = ((CaseOperator)o).getResults();
				for (Object o2 : results)
				{
					if (o2 instanceof String)
					{
						if (((String)o2).startsWith("\u0000"))
						{
							String newType = getType(((String)o2).substring(1), op.getCols2Types());
							if (type == null)
							{
								type = newType;
							}
							else if (type.equals("INT") && !newType.equals("INT"))
							{
								if (newType.equals("LONG") || newType.equals("FLOAT"))
								{
									type = newType;
								}
								else
								{
									throw new ParseException("All possible results in a case expression must have the same type");
								}
							}
							else if (type.equals("LONG") && !newType.equals("LONG"))
							{
								if (newType.equals("INT"))
								{
								}
								else if (newType.equals("FLOAT"))
								{
									type = newType;
								}
								else
								{
									throw new ParseException("All possible results in a case expression must have the same type");
								}
							}
							else if (type.equals("FLOAT") && !newType.equals("FLOAT"))
							{
								if (newType.equals("INT") || newType.equals("LONG"))
								{
								}
								else
								{
									throw new ParseException("All possible results in a case expression must have the same type");
								}
							}
							else if (!type.equals(newType))
							{
								throw new ParseException("All possible results in a case expression must have the same type");
							}
						}
					}
				}
				((CaseOperator)o).setType(type);
				try
				{
					o.add(op);
				}
				catch (Exception e)
				{
					throw new ParseException(e.getMessage());
				}
				row.remove(7);
				row.add(true);
				return o;
			}
			try
			{
				o.add(op);
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
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
						HRDBMSWorker.logger.debug("Looking for " + name + " in " + op.getCols2Types());
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

			return o;
		}
		else
		{
			// type group by
			throw new ParseException("A WHERE clause cannot refer to an aggregate column. This must be done in the HAVING clause.");
		}
	}

	private Operator addRename(Operator op, SearchCondition join) throws Exception
	{
		ArrayList<String> olds = new ArrayList<String>();
		ArrayList<String> news = new ArrayList<String>();

		if (join.getClause().getPredicate() != null)
		{
			Column col = join.getClause().getPredicate().getRHS().getColumn();
			String c = "";
			if (col.getTable() != null)
			{
				c += col.getTable();
			}

			c += ("." + col.getColumn());
			olds.add(getMatchingCol(op, c));
			String c2 = c.substring(0, c.indexOf('.') + 1);
			c2 += ("_" + rewriteCounter);
			c2 += c.substring(c.indexOf('.') + 1);
			col.setColumn("_" + rewriteCounter++ + col.getColumn());
			news.add(c2);
		}
		else
		{
			SearchCondition scond = join.getClause().getSearch();
			Column col = scond.getClause().getPredicate().getRHS().getColumn();
			String c = "";
			if (col.getTable() != null)
			{
				c += col.getTable();
			}

			c += ("." + col.getColumn());
			olds.add(getMatchingCol(op, c));
			String c2 = c.substring(0, c.indexOf('.') + 1);
			c2 += ("_" + rewriteCounter);
			c2 += c.substring(c.indexOf('.') + 1);
			col.setColumn("_" + rewriteCounter++ + col.getColumn());
			news.add(c2);
			for (ConnectedSearchClause csc : scond.getConnected())
			{
				col = csc.getSearch().getPredicate().getRHS().getColumn();
				c = "";
				if (col.getTable() != null)
				{
					c += col.getTable();
				}

				c += ("." + col.getColumn());
				olds.add(getMatchingCol(op, c));
				c2 = c.substring(0, c.indexOf('.') + 1);
				c2 += ("_" + rewriteCounter);
				c2 += c.substring(c.indexOf('.') + 1);
				col.setColumn("_" + rewriteCounter++ + col.getColumn());
				news.add(c2);
			}
		}

		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause sc : join.getConnected())
			{
				if (sc.getSearch().getPredicate() != null)
				{
					Column col = sc.getSearch().getPredicate().getRHS().getColumn();
					String c = "";
					if (col.getTable() != null)
					{
						c += col.getTable();
					}

					c += ("." + col.getColumn());
					olds.add(getMatchingCol(op, c));
					String c2 = c.substring(0, c.indexOf('.') + 1);
					c2 += ("_" + rewriteCounter);
					c2 += c.substring(c.indexOf('.') + 1);
					col.setColumn("_" + rewriteCounter++ + col.getColumn());
					news.add(c2);
				}
				else
				{
					SearchCondition scond = sc.getSearch().getSearch();
					Column col = scond.getClause().getPredicate().getRHS().getColumn();
					String c = "";
					if (col.getTable() != null)
					{
						c += col.getTable();
					}

					c += ("." + col.getColumn());
					olds.add(getMatchingCol(op, c));
					String c2 = c.substring(0, c.indexOf('.') + 1);
					c2 += ("_" + rewriteCounter);
					c2 += c.substring(c.indexOf('.') + 1);
					col.setColumn("_" + rewriteCounter++ + col.getColumn());
					news.add(c2);
					for (ConnectedSearchClause csc : scond.getConnected())
					{
						col = csc.getSearch().getPredicate().getRHS().getColumn();
						c = "";
						if (col.getTable() != null)
						{
							c += col.getTable();
						}

						c += ("." + col.getColumn());
						olds.add(getMatchingCol(op, c));
						c2 = c.substring(0, c.indexOf('.') + 1);
						c2 += ("_" + rewriteCounter);
						c2 += c.substring(c.indexOf('.') + 1);
						col.setColumn("_" + rewriteCounter++ + col.getColumn());
						news.add(c2);
					}
				}
			}
		}

		Operator rename = new RenameOperator(olds, news, meta);
		rename.add(op);
		return rename;
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

	private boolean allReferencesSatisfied(ArrayList<String> ref, Operator op)
	{
		Set<String> set = op.getCols2Pos().keySet();
		for (String r : ref)
		{
			if (set.contains(r))
			{
				continue;
			}

			if (r.indexOf('.') > 0)
			{
				return false;
			}

			if (r.contains("."))
			{
				r = r.substring(1);
			}

			int matches = 0;
			for (String c : set)
			{
				if (c.contains("."))
				{
					c = c.substring(c.indexOf('.') + 1);
				}

				if (c.equals(r))
				{
					matches++;
				}
			}

			if (matches != 1)
			{
				return false;
			}
		}

		return true;
	}

	private Object buildNGBExtend(Operator op, ArrayList<Object> row, SubSelect sub) throws Exception
	{
		// colName, op, type, id, exp, prereq, done
		if (!((Integer)row.get(5)).equals(-1))
		{
			// get the row
			for (ArrayList<Object> r : complex)
			{
				if (r.get(3).equals(row.get(5)))
				{
					try
					{
						Object o = addComplexColumn(r, op, sub);
						op = (Operator)o;
						break;
					}
					catch (Exception e)
					{
						if (row.get(1) instanceof Operator && !(row.get(1) instanceof CaseOperator) && allReferencesSatisfied(((Operator)row.get(1)).getReferences(), op))
						{
							break;
						}
						else if (row.get(1) instanceof CaseOperator)
						{
							if (allReferencesSatisfied(((CaseOperator)row.get(1)).getReferences(), op))
							{
								ArrayList<String> references = new ArrayList<String>();
								Expression exp = (Expression)row.get(4);
								ArrayList<Case> cases = exp.getCases();
								for (Case c : cases)
								{
									references.addAll(getReferences(c.getCondition()));
								}

								if (allReferencesSatisfied(references, op))
								{
									break;
								}
							}
						}

						return null;
					}
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
			if (o instanceof CaseOperator)
			{
				Expression exp = (Expression)row.get(4);
				ArrayList<HashSet<HashMap<Filter, Filter>>> alhshm = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
				// HRDBMSWorker.logger.debug("Build SC has " +
				// exp.getCases().size() + " cases to examine");
				for (Case c : exp.getCases())
				{
					Operator top = op;
					op = this.buildOperatorTreeFromSearchCondition(c.getCondition(), top, sub);
					if (op == top)
					{
						HRDBMSWorker.logger.debug("Build SC when building a CaseOperator did not do anything");
					}
					else if (!(op instanceof SelectOperator))
					{
						HRDBMSWorker.logger.debug("Build SC when building a CaseOperator did something, but it wasn't a SelectOperator");
					}
					// add filters to alhshm and remove from tree
					HashSet<HashMap<Filter, Filter>> hshm = new HashSet<HashMap<Filter, Filter>>();
					while (op instanceof SelectOperator)
					{
						HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
						for (Filter f : ((SelectOperator)op).getFilter())
						{
							hm.put(f, f);
						}

						hshm.add(hm);
						op = op.children().get(0);
						op.parent().removeChild(op);
					}

					Operator newTop = op;
					while (op != top)
					{
						if (op.children().size() > 1)
						{
							throw new ParseException("Invalid search condition in a case expression");
						}

						if (op instanceof SelectOperator)
						{
							throw new ParseException("Internal error: stranded select operator when processing a case expression");
						}

						op = op.children().get(0);
					}

					op = newTop;
					alhshm.add(hshm);
				}

				((CaseOperator)o).setFilters(alhshm);
				String type = ((CaseOperator)o).getType();
				ArrayList<Object> results = ((CaseOperator)o).getResults();
				for (Object o2 : results)
				{
					if (o2 instanceof String)
					{
						if (((String)o2).startsWith("\u0000"))
						{
							String newType = getType(((String)o2).substring(1), op.getCols2Types());
							if (type == null)
							{
								type = newType;
							}
							else if (type.equals("INT") && !newType.equals("INT"))
							{
								if (newType.equals("LONG") || newType.equals("FLOAT"))
								{
									type = newType;
								}
								else
								{
									throw new ParseException("All possible results in a case expression must have the same type");
								}
							}
							else if (type.equals("LONG") && !newType.equals("LONG"))
							{
								if (newType.equals("INT"))
								{
								}
								else if (newType.equals("FLOAT"))
								{
									type = newType;
								}
								else
								{
									throw new ParseException("All possible results in a case expression must have the same type");
								}
							}
							else if (type.equals("FLOAT") && !newType.equals("FLOAT"))
							{
								if (newType.equals("INT") || newType.equals("LONG"))
								{
								}
								else
								{
									throw new ParseException("All possible results in a case expression must have the same type");
								}
							}
							else if (!type.equals(newType))
							{
								throw new ParseException("All possible results in a case expression must have the same type");
							}
						}
					}
				}
				((CaseOperator)o).setType(type);
				try
				{
					o.add(op);
				}
				catch (Exception e)
				{
					throw new ParseException(e.getMessage());
				}
				row.remove(7);
				row.add(true);
				return o;
			}
			try
			{
				o.add(op);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				throw new ParseException(e.getMessage());
			}
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
						HRDBMSWorker.logger.debug("Could not find " + name + " in " + op.getCols2Types());
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

			return o;
		}
		else
		{
			// type group by
			return false;
		}
	}

	private Operator buildNGBExtends(Operator op, SubSelect sub) throws Exception
	{
		for (ArrayList<Object> row : complex)
		{
			if ((Boolean)row.get(7) == false && (Integer)row.get(2) != TYPE_GROUPBY && (row.get(6) == null || ((SubSelect)row.get(6)).equals(sub)))
			{
				Object o = buildNGBExtend(op, row, sub);
				if (o != null && !(o instanceof Boolean))
				{
					op = (Operator)o;
				}
			}
		}

		return op;
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

		if (!meta.verifyTableExistence(schema, tbl, tx))
		{
			throw new ParseException("Table does not exist");
		}

		String index = createIndex.getIndex().getName();
		if (createIndex.getIndex().getSchema() != null)
		{
			throw new ParseException("Schemas cannot be specified for index names");
		}

		if (MetaData.verifyIndexExistence(schema, index, tx))
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
			if (!meta.verifyColExistence(schema, tbl, col, tx))
			{
				throw new ParseException("Column " + col + " does not exist");
			}
		}

		return new CreateIndexOperator(schema, tbl, index, indexDefs, unique, meta);
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

		if (meta.verifyTableExistence(schema, tbl, tx) || meta.verifyViewExistence(schema, tbl, tx))
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

		return new CreateTableOperator(schema, tbl, colDefs, new ArrayList<String>(pks), createTable.getNodeGroupExp(), createTable.getNodeExp(), createTable.getDeviceExp(), meta);
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

		if (meta.verifyTableExistence(schema, tbl, tx) || meta.verifyViewExistence(schema, tbl, tx))
		{
			throw new ParseException("Table or view already exists");
		}

		return new CreateViewOperator(schema, tbl, createView.getText(), meta);
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

		if (!meta.verifyTableExistence(schema, tbl, tx))
		{
			throw new ParseException("Table does not exist");
		}

		if (delete.getWhere() == null)
		{
			return new MassDeleteOperator(schema, tbl, meta);
		}

		TableScanOperator scan = new TableScanOperator(schema, tbl, meta, tx);
		Operator op = buildOperatorTreeFromWhere(delete.getWhere(), scan, null);
		scan.getRID();
		ArrayList<String> cols = new ArrayList<String>();
		cols.add("_RID1");
		cols.add("_RID2");
		cols.add("_RID3");
		cols.add("_RID4");
		cols.addAll(meta.getIndexColsForTable(schema, tbl, tx));
		ProjectOperator project = new ProjectOperator(cols, meta);
		project.add(op);
		op = project;
		RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
		retval.add(op);
		Phase1 p1 = new Phase1(retval, tx);
		p1.optimize();
		new Phase2(retval, tx).optimize();
		new Phase3(retval, tx).optimize();
		new Phase4(retval, tx).optimize();
		new Phase5(retval, tx, p1.likelihoodCache).optimize();
		DeleteOperator dOp = new DeleteOperator(schema, tbl, meta);
		Operator child = retval.children().get(0);
		retval.removeChild(child);
		dOp.add(child);
		return dOp;
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

		if (!MetaData.verifyIndexExistence(schema, tbl, tx))
		{
			throw new ParseException("Index does not exist");
		}

		return new DropIndexOperator(schema, tbl, meta);
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

		if (!meta.verifyTableExistence(schema, tbl, tx))
		{
			throw new ParseException("Table does not exist");
		}

		return new DropTableOperator(schema, tbl, meta);
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

		if (!meta.verifyViewExistence(schema, tbl, tx))
		{
			throw new ParseException("Table or view does not exist");
		}

		return new DropViewOperator(schema, tbl, meta);
	}

	private OperatorTypeAndName buildOperatorTreeFromExpression(Expression exp, String name, SubSelect sub) throws ParseException
	{
		if (exp.isLiteral())
		{
			Literal l = exp.getLiteral();
			if (l.isNull())
			{
				// TODO
			}
			else
			{
				Object literal = l.getValue();
				if (literal instanceof Double || literal instanceof Long)
				{
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new ExtendOperator(literal.toString(), sgetName, meta), TYPE_INLINE, sgetName, -1);
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
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new ExtendObjectOperator(literal, sgetName, meta), TYPE_INLINE, sgetName, -1);
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
				String sgetName = name;
				if (!sgetName.contains("."))
				{
					sgetName = "." + sgetName;
				}
				return new OperatorTypeAndName(new CountOperator(sgetName, meta), TYPE_GROUPBY, sgetName, -1);
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
		else if (exp.isCase())
		{
			ArrayList<HashSet<HashMap<Filter, Filter>>> alhshm = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
			ArrayList<String> results = new ArrayList<String>();
			int prereq = -1;
			String type = null;
			for (Case c : exp.getCases())
			{
				// handle search condition later
				if (c.getResult().isColumn())
				{
					Column input = c.getResult().getColumn();
					String inputColumn = "";
					if (input.getTable() != null)
					{
						inputColumn += (input.getTable() + ".");
					}
					else
					{
						inputColumn += ".";
					}
					String end = input.getColumn();
					if (end.contains("."))
					{
						end = end.substring(end.indexOf('.') + 1);
					}
					inputColumn += end;

					results.add(inputColumn);
					continue;
				}
				else if (c.getResult().isLiteral())
				{
					if (c.getResult().getLiteral().isNull())
					{
						// TODO
					}

					Object obj = c.getResult().getLiteral().getValue();
					if (obj instanceof String)
					{
						results.add("'" + obj + "'");
						if (type != null && !type.equals("CHAR"))
						{
							throw new ParseException("All posible results of a case expression must have the same type");
						}
						type = "CHAR";
					}
					else if (obj instanceof Double)
					{
						if (type == null)
						{
							type = "FLOAT";
						}
						else if (type.equals("LONG"))
						{
							// fix
							int i = 0;
							for (String result : (ArrayList<String>)results.clone())
							{
								results.remove(i);
								results.add(i, result + ".0");
								i++;
							}
						}
						else if (type.equals("FLOAT"))
						{
						}
						else
						{
							throw new ParseException("All possible results of a case expression must have the same type");
						}
						results.add(obj.toString());
					}
					else if (obj instanceof Long)
					{
						if (type == null)
						{
							type = "LONG";
							results.add(obj.toString());
						}
						else if (type.equals("LONG"))
						{
							results.add(obj.toString());
						}
						else if (type.equals("FLOAT"))
						{
							results.add(obj.toString() + ".0");
						}
						else
						{
							throw new ParseException("All possible results of a case expression must have the same type");
						}
					}

					continue;
				}

				OperatorTypeAndName otan = buildOperatorTreeFromExpression(c.getResult(), null, sub);
				if (otan.getType() == TYPE_DAYS)
				{
					throw new ParseException("A case expression cannot return a result of type DAYS");
				}
				else if (otan.getType() == TYPE_MONTHS)
				{
					throw new ParseException("A case expression cannot return a result of type MONTHS");
				}
				else if (otan.getType() == TYPE_YEARS)
				{
					throw new ParseException("A case expression cannot return a result of type YEARS");
				}
				else if (otan.getType() == TYPE_GROUPBY)
				{
					throw new ParseException("A case expression cannot perform an aggregation operation");
				}
				else if (otan.getType() == TYPE_DATE)
				{
					Date date = ((GregorianCalendar)otan.getOp()).getTime();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String dateString = sdf.format(date);
					results.add("DATE('" + dateString + "')");
					if (type == null)
					{
						type = "DATE";
					}
					else if (!type.equalsIgnoreCase("DATE"))
					{
						throw new ParseException("All possible results of a case expression must have the same type");
					}
				}
				else
				{
					results.add(otan.getName());

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(otan.getName());
					row.add(otan.getOp());
					row.add(otan.getType());
					row.add(complexID++);
					row.add(c.getResult());
					row.add(otan.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (prereq != -1)
					{
						// go all the way to the bottom and set the prereq to
						// prereq
						// then set prereq to the id of this one
						ArrayList<Object> bottom = getBottomRow(row);
						bottom.remove(5);
						bottom.add(5, prereq);
						prereq = (Integer)row.get(3);
					}
					else
					{
						prereq = (Integer)row.get(3);
					}
				}
			}

			if (exp.getDefault().isColumn())
			{
				Column input = exp.getDefault().getColumn();
				String inputColumn = "";
				if (input.getTable() != null)
				{
					inputColumn += (input.getTable() + ".");
				}
				else
				{
					inputColumn += ".";
				}
				String end = input.getColumn();
				if (end.contains("."))
				{
					end = end.substring(end.indexOf('.') + 1);
				}
				inputColumn += end;

				results.add(inputColumn);
			}
			else if (exp.getDefault().isLiteral())
			{
				if (exp.getDefault().getLiteral().isNull())
				{
					// TODO
				}

				Object obj = exp.getDefault().getLiteral().getValue();
				if (obj instanceof String)
				{
					results.add("'" + obj + "'");
					if (type != null && !type.equals("CHAR"))
					{
						throw new ParseException("All posible results of a case expression must have the same type");
					}
					type = "CHAR";
				}
				else if (obj instanceof Double)
				{
					if (type == null)
					{
						type = "FLOAT";
					}
					else if (type.equals("LONG"))
					{
						// fix
						int i = 0;
						for (String result : (ArrayList<String>)results.clone())
						{
							results.remove(i);
							results.add(i, result + ".0");
							i++;
						}
					}
					else if (type.equals("FLOAT"))
					{
					}
					else
					{
						throw new ParseException("All possible results of a case expression must have the same type");
					}
					results.add(obj.toString());
				}
				else if (obj instanceof Long)
				{
					if (type == null)
					{
						type = "LONG";
						results.add(obj.toString());
					}
					else if (type.equals("LONG"))
					{
						results.add(obj.toString());
					}
					else if (type.equals("FLOAT"))
					{
						results.add(obj.toString() + ".0");
					}
					else
					{
						throw new ParseException("All possible results of a case expression must have the same type");
					}
				}
			}
			else
			{
				OperatorTypeAndName otan = buildOperatorTreeFromExpression(exp.getDefault(), null, sub);
				if (otan.getType() == TYPE_DAYS)
				{
					throw new ParseException("A case expression cannot return a result of type DAYS");
				}
				else if (otan.getType() == TYPE_MONTHS)
				{
					throw new ParseException("A case expression cannot return a result of type MONTHS");
				}
				else if (otan.getType() == TYPE_YEARS)
				{
					throw new ParseException("A case expression cannot return a result of type YEARS");
				}
				else if (otan.getType() == TYPE_GROUPBY)
				{
					throw new ParseException("A case expression cannot perform an aggregation operation");
				}
				else if (otan.getType() == TYPE_DATE)
				{
					Date date = ((GregorianCalendar)otan.getOp()).getTime();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String dateString = sdf.format(date);
					results.add("DATE('" + dateString + "')");
					if (type == null)
					{
						type = "DATE";
					}
					else if (!type.equalsIgnoreCase("DATE"))
					{
						throw new ParseException("All possible results of a case expression must have the same type");
					}
				}
				else
				{
					results.add(otan.getName());

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(otan.getName());
					row.add(otan.getOp());
					row.add(otan.getType());
					row.add(complexID++);
					row.add(exp.getDefault());
					row.add(otan.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (prereq != -1)
					{
						// go all the way to the bottom and set the prereq to
						// prereq
						// then set prereq to the id of this one
						ArrayList<Object> bottom = getBottomRow(row);
						bottom.remove(5);
						bottom.add(5, prereq);
						prereq = (Integer)row.get(3);
					}
					else
					{
						prereq = (Integer)row.get(3);
					}
				}
			}

			if (name != null)
			{
				String sgetName = name;
				if (!sgetName.contains("."))
				{
					sgetName = "." + sgetName;
				}
				return new OperatorTypeAndName(new CaseOperator(alhshm, results, sgetName, type, meta), TYPE_INLINE, sgetName, prereq);
			}
			else
			{
				name = "._E" + suffix++;
				return new OperatorTypeAndName(new CaseOperator(alhshm, results, name, type, meta), TYPE_INLINE, name, prereq);
			}
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
					String end = input.getColumn();
					if (end.contains("."))
					{
						end = end.substring(end.indexOf('.') + 1);
					}
					inputColumn += end;
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new MaxOperator(inputColumn, sgetName, meta, true), TYPE_GROUPBY, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new MaxOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression() || arg.isCase() || arg.isFunction())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to MAX() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to MAX() cannot be of type DAYS");
					}
					else if (retval.getType() == TYPE_MONTHS)
					{
						throw new ParseException("The argument to MAX() cannot be of type MONTHS");
					}
					else if (retval.getType() == TYPE_YEARS)
					{
						throw new ParseException("The argument to MAX() cannot be of type YEARS");
					}
					else if (retval.getType() == TYPE_GROUPBY)
					{
						throw new ParseException("The argument to MAX() cannot be an aggregation");
					}

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new MaxOperator(retval.getName(), sgetName, meta, true), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
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

					String end = input.getColumn();
					if (end.contains("."))
					{
						end = end.substring(end.indexOf('.') + 1);
					}
					inputColumn += end;
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new MinOperator(inputColumn, sgetName, meta, true), TYPE_GROUPBY, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new MinOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression() || arg.isCase() || arg.isFunction())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to MIN() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to MIN() cannot be of type DAYS");
					}
					else if (retval.getType() == TYPE_MONTHS)
					{
						throw new ParseException("The argument to MIN() cannot be of type MONTHS");
					}
					else if (retval.getType() == TYPE_YEARS)
					{
						throw new ParseException("The argument to MIN() cannot be of type YEARS");
					}
					else if (retval.getType() == TYPE_GROUPBY)
					{
						throw new ParseException("The argument to MIN() cannot be an aggregation");
					}

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new MinOperator(retval.getName(), sgetName, meta, true), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
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
					String end = input.getColumn();
					if (end.contains("."))
					{
						end = end.substring(end.indexOf('.') + 1);
					}
					inputColumn += end;
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new AvgOperator(inputColumn, sgetName, meta), TYPE_GROUPBY, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new AvgOperator(inputColumn, name, meta), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression() || arg.isCase() || arg.isFunction())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to AVG() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to AVG() cannot be of type DAYS");
					}
					else if (retval.getType() == TYPE_MONTHS)
					{
						throw new ParseException("The argument to AVG() cannot be of type MONTHS");
					}
					else if (retval.getType() == TYPE_YEARS)
					{
						throw new ParseException("The argument to AVG() cannot be of type YEARS");
					}
					else if (retval.getType() == TYPE_GROUPBY)
					{
						throw new ParseException("The argument to AVG() cannot be an aggregation");
					}

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new AvgOperator(retval.getName(), sgetName, meta), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
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
					String end = input.getColumn();
					if (end.contains("."))
					{
						end = end.substring(end.indexOf('.') + 1);
					}
					inputColumn += end;
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new SumOperator(inputColumn, sgetName, meta, true), TYPE_GROUPBY, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new SumOperator(inputColumn, name, meta, true), TYPE_GROUPBY, name, -1);
					}
				}
				else if (arg.isExpression() || arg.isCase() || arg.isFunction())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
					if (retval.getType() == TYPE_DATE)
					{
						throw new ParseException("The argument to SUM() cannot be a literal DATE");
					}
					else if (retval.getType() == TYPE_DAYS)
					{
						throw new ParseException("The argument to SUM() cannot be of type DAYS");
					}
					else if (retval.getType() == TYPE_MONTHS)
					{
						throw new ParseException("The argument to SUM() cannot be of type MONTHS");
					}
					else if (retval.getType() == TYPE_YEARS)
					{
						throw new ParseException("The argument to SUM() cannot be of type YEARS");
					}
					else if (retval.getType() == TYPE_GROUPBY)
					{
						throw new ParseException("The argument to SUM() cannot be an aggregation");
					}

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new SumOperator(retval.getName(), sgetName, meta, true), TYPE_GROUPBY, sgetName, (Integer)row.get(3));
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
					String end = input.getColumn();
					if (end.contains("."))
					{
						end = end.substring(end.indexOf('.') + 1);
					}
					inputColumn += end;
					if (name != null)
					{
						if (f.getDistinct())
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new CountDistinctOperator(inputColumn, sgetName, meta), TYPE_GROUPBY, sgetName, -1);
						}
						else
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new CountOperator(inputColumn, sgetName, meta), TYPE_GROUPBY, sgetName, -1);
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
				catch (Exception e)
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
			else if (method.equals("MONTHS"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("MONTHS() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (!arg.isLiteral())
				{
					throw new ParseException("MONTHS() requires a literal numeric argument");
				}

				Object literal = arg.getLiteral().getValue();
				if (literal instanceof Integer)
				{
					return new OperatorTypeAndName(literal, TYPE_MONTHS, "", -1);
				}
				else if (literal instanceof Long)
				{
					return new OperatorTypeAndName(new Integer(((Long)literal).intValue()), TYPE_MONTHS, "", -1);
				}
				else
				{
					throw new ParseException("MONTHS() requires a literal numeric argument");
				}
			}
			else if (method.equals("YEARS"))
			{
				ArrayList<Expression> args = f.getArgs();
				if (args.size() != 1)
				{
					throw new ParseException("YEARS() requires only 1 argument");
				}
				Expression arg = args.get(0);
				if (!arg.isLiteral())
				{
					throw new ParseException("YEARS() requires a literal numeric argument");
				}

				Object literal = arg.getLiteral().getValue();
				if (literal instanceof Integer)
				{
					return new OperatorTypeAndName(literal, TYPE_YEARS, "", -1);
				}
				else if (literal instanceof Long)
				{
					return new OperatorTypeAndName(new Integer(((Long)literal).intValue()), TYPE_YEARS, "", -1);
				}
				else
				{
					throw new ParseException("YEARS() requires a literal numeric argument");
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
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new YearOperator(colName, sgetName, meta), TYPE_INLINE, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new YearOperator(colName, name, meta), TYPE_INLINE, name, -1);
					}
				}
				else if (arg.isFunction() || arg.isExpression() || arg.isCase())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
					if (retval.getType() == TYPE_DATE)
					{
						GregorianCalendar cal = (GregorianCalendar)retval.getOp();
						int literal = cal.get(Calendar.YEAR);

						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new ExtendOperator(Integer.toString(literal), sgetName, meta), TYPE_INLINE, sgetName, -1);
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
					else if (retval.getType() == TYPE_MONTHS)
					{
						throw new ParseException("YEAR() cannot be called with an argument of type MONTHS");
					}
					else if (retval.getType() == TYPE_YEARS)
					{
						throw new ParseException("YEAR() cannot be called with an argument of type YEARS");
					}
					else if (retval.getType() == TYPE_GROUPBY)
					{
						throw new ParseException("YEAR() cannot be called with an agrument that is an aggregation");
					}

					ArrayList<Object> row = new ArrayList<Object>();
					row.add(retval.getName());
					row.add(retval.getOp());
					row.add(retval.getType());
					row.add(complexID++);
					row.add(arg);
					row.add(retval.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new YearOperator(retval.getName(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
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
				if (arg.isLiteral() || arg.isFunction() || arg.isExpression() || arg.isCase())
				{
					OperatorTypeAndName retval = buildOperatorTreeFromExpression(arg, null, sub);
					if (retval.getType() == TYPE_DATE || retval.getType() == TYPE_DAYS || retval.getType() == TYPE_MONTHS || retval.getType() == TYPE_YEARS || retval.getType() == TYPE_GROUPBY)
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
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new SubstringOperator(retval.getName(), start, end, sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
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
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new SubstringOperator(colName, start, end, sgetName, meta), TYPE_INLINE, sgetName, -1);
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
			OperatorTypeAndName exp1 = buildOperatorTreeFromExpression(exp.getLHS(), null, sub);
			OperatorTypeAndName exp2 = buildOperatorTreeFromExpression(exp.getRHS(), null, sub);
			Column col1 = null;
			Column col2 = null;
			if (exp1 == null)
			{
				col1 = exp.getLHS().getColumn();
			}

			if (exp2 == null)
			{
				col2 = exp.getRHS().getColumn();
			}

			if (exp1 != null && exp1.getType() == TYPE_DATE)
			{
				if (exp.getOp().equals("+"))
				{
					if (exp2 != null && exp2.getType() == TYPE_DAYS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(Calendar.DATE, (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else if (exp2 != null && exp2.getType() == TYPE_MONTHS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(Calendar.MONTH, (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else if (exp2 != null && exp2.getType() == TYPE_YEARS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(Calendar.YEAR, (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else
					{
						throw new ParseException("Only types DAYS/MONTHS/YEARS can be added to a DATE");
					}
				}
				else if (exp.getOp().equals("-"))
				{
					if (exp2 != null && exp2.getType() == TYPE_DAYS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(Calendar.DATE, -1 * (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else if (exp2 != null && exp2.getType() == TYPE_MONTHS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(Calendar.MONTH, -1 * (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else if (exp2 != null && exp2.getType() == TYPE_YEARS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp1.getOp();
						cal.add(Calendar.YEAR, -1 * (Integer)exp2.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else
					{
						throw new ParseException("Only type DAYS/MONTHS/YEARS can be substracted from a DATE");
					}
				}
				else
				{
					throw new ParseException("Only the + and - operators are valid with type DATE");
				}
			}
			else if (exp2 != null && exp2.getType() == TYPE_DATE)
			{
				if (exp.getOp().equals("+"))
				{
					if (exp1 != null && exp1.getType() == TYPE_DAYS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
						cal.add(Calendar.DATE, (Integer)exp1.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else if (exp1 != null && exp1.getType() == TYPE_MONTHS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
						cal.add(Calendar.MONTH, (Integer)exp1.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else if (exp1 != null && exp1.getType() == TYPE_YEARS)
					{
						GregorianCalendar cal = (GregorianCalendar)exp2.getOp();
						cal.add(Calendar.YEAR, (Integer)exp1.getOp());
						return new OperatorTypeAndName(cal, TYPE_DATE, "", -1);
					}
					else
					{
						throw new ParseException("Only type DAYS/MONTHS/YEARS can be added to a DATE");
					}
				}
				else
				{
					throw new ParseException("Only a DATE can be added to type DAYS/MONTHS/YEARS");
				}
			}
			else if (exp1 != null && exp1.getType() == TYPE_DAYS)
			{
				// already handled DAYS + DATE
				// so need to handle DAYS + col(DATE)
				// and DAYS +/- DAYS
				if (exp2 != null && exp2.getType() == TYPE_DAYS)
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

				// externalize exp2 if not col
				if (exp2 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp2.getName());
					row.add(exp2.getOp());
					row.add(exp2.getType());
					row.add(complexID++);
					row.add(exp.getRHS());
					row.add(exp2.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_DAYS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_DAYS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else
				{
					String colString2 = "";
					if (col2.getTable() != null)
					{
						colString2 = col2.getTable();
					}
					colString2 += ("." + col2.getColumn());
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new DateMathOperator(colString2, TYPE_DAYS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(colString2, TYPE_DAYS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, -1);
					}
				}
			}
			else if (exp1 != null && exp1.getType() == TYPE_MONTHS)
			{
				if (exp2 != null && exp2.getType() == TYPE_MONTHS)
				{
					if (exp.getOp().equals("+"))
					{
						return new OperatorTypeAndName((Integer)exp1.getOp() + (Integer)exp2.getOp(), TYPE_MONTHS, "", -1);
					}
					else if (exp.getOp().equals("-"))
					{
						return new OperatorTypeAndName((Integer)exp1.getOp() - (Integer)exp2.getOp(), TYPE_MONTHS, "", -1);
					}
				}

				// externalize exp2 if not col
				if (exp2 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp2.getName());
					row.add(exp2.getOp());
					row.add(exp2.getType());
					row.add(complexID++);
					row.add(exp.getRHS());
					row.add(exp2.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_MONTHS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_MONTHS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else
				{
					String colString2 = "";
					if (col2.getTable() != null)
					{
						colString2 = col2.getTable();
					}
					colString2 += ("." + col2.getColumn());
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new DateMathOperator(colString2, TYPE_MONTHS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(colString2, TYPE_MONTHS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, -1);
					}
				}
			}
			else if (exp1 != null && exp1.getType() == TYPE_YEARS)
			{
				if (exp2 != null && exp2.getType() == TYPE_YEARS)
				{
					if (exp.getOp().equals("+"))
					{
						return new OperatorTypeAndName((Integer)exp1.getOp() + (Integer)exp2.getOp(), TYPE_YEARS, "", -1);
					}
					else if (exp.getOp().equals("-"))
					{
						return new OperatorTypeAndName((Integer)exp1.getOp() - (Integer)exp2.getOp(), TYPE_YEARS, "", -1);
					}
				}

				// externalize exp2 if not col
				if (exp2 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp2.getName());
					row.add(exp2.getOp());
					row.add(exp2.getType());
					row.add(complexID++);
					row.add(exp.getRHS());
					row.add(exp2.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_YEARS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(exp2.getName(), TYPE_YEARS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
					}
				}
				else
				{
					String colString2 = "";
					if (col2.getTable() != null)
					{
						colString2 = col2.getTable();
					}
					colString2 += ("." + col2.getColumn());
					if (name != null)
					{
						String sgetName = name;
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}
						return new OperatorTypeAndName(new DateMathOperator(colString2, TYPE_YEARS, (Integer)exp1.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
					}
					else
					{
						name = "._E" + suffix++;
						return new OperatorTypeAndName(new DateMathOperator(colString2, TYPE_YEARS, (Integer)exp1.getOp(), name, meta), TYPE_INLINE, name, -1);
					}
				}
			}
			else if (exp2 != null && exp2.getType() == TYPE_DAYS)
			{
				// already handled DATE +/- DAYS and DAYS +/- DAYS
				// so need to handle col(DATE) +/- DAYS
				// externalize exp1 if not col

				if (exp1 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp1.getName());
					row.add(exp1.getOp());
					row.add(exp1.getType());
					row.add(complexID++);
					row.add(exp.getLHS());
					row.add(exp1.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (exp.getOp().equals("+"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
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
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_DAYS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
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
				else
				{
					String colString1 = "";
					if (col1.getTable() != null)
					{
						colString1 = col1.getTable();
					}
					colString1 += ("." + col1.getColumn());
					if (exp.getOp().equals("+"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else if (exp.getOp().equals("-"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_DAYS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else
					{
						throw new ParseException("Only the + and - operators are allowed with type DAYS");
					}
				}
			}
			else if (exp2 != null && exp2.getType() == TYPE_MONTHS)
			{
				if (exp1 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp1.getName());
					row.add(exp1.getOp());
					row.add(exp1.getType());
					row.add(complexID++);
					row.add(exp.getLHS());
					row.add(exp1.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (exp.getOp().equals("+"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
						}
					}
					else if (exp.getOp().equals("-"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_MONTHS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
						}
					}
					else
					{
						throw new ParseException("Only the + and - operators are allowed with type MONTHS");
					}
				}
				else
				{
					String colString1 = "";
					if (col1.getTable() != null)
					{
						colString1 = col1.getTable();
					}
					colString1 += ("." + col1.getColumn());
					if (exp.getOp().equals("+"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else if (exp.getOp().equals("-"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_MONTHS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else
					{
						throw new ParseException("Only the + and - operators are allowed with type MONTHS");
					}
				}
			}
			else if (exp2 != null && exp2.getType() == TYPE_YEARS)
			{
				if (exp1 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp1.getName());
					row.add(exp1.getOp());
					row.add(exp1.getType());
					row.add(complexID++);
					row.add(exp.getLHS());
					row.add(exp1.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);

					if (exp.getOp().equals("+"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
						}
					}
					else if (exp.getOp().equals("-"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, (Integer)row.get(3));
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(exp1.getName(), TYPE_YEARS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, (Integer)row.get(3));
						}
					}
					else
					{
						throw new ParseException("Only the + and - operators are allowed with type YEARS");
					}
				}
				else
				{
					String colString1 = "";
					if (col1.getTable() != null)
					{
						colString1 = col1.getTable();
					}
					colString1 += ("." + col1.getColumn());
					if (exp.getOp().equals("+"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else if (exp.getOp().equals("-"))
					{
						if (name != null)
						{
							String sgetName = name;
							if (!sgetName.contains("."))
							{
								sgetName = "." + sgetName;
							}
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, -1 * (Integer)exp2.getOp(), sgetName, meta), TYPE_INLINE, sgetName, -1);
						}
						else
						{
							name = "._E" + suffix++;
							return new OperatorTypeAndName(new DateMathOperator(colString1, TYPE_YEARS, -1 * (Integer)exp2.getOp(), name, meta), TYPE_INLINE, name, -1);
						}
					}
					else
					{
						throw new ParseException("Only the + and - operators are allowed with type YEARS");
					}
				}
			}

			// neither exp1 or exp2 are of type DATE or DAYS or MONTHS or YEARS
			if (exp.getOp().equals("||"))
			{
				// string concatenation
				// externalize both exp1 and exp2 if they are not cols
				int prereq1 = -1;
				int prereq2 = -1;
				ArrayList<Object> row = null;
				if (exp1 != null)
				{
					row = new ArrayList<Object>();
					row.add(exp1.getName());
					row.add(exp1.getOp());
					row.add(exp1.getType());
					row.add(complexID++);
					row.add(exp.getLHS());
					row.add(exp1.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
					prereq1 = (Integer)row.get(3);
				}

				if (exp2 != null)
				{
					row = new ArrayList<Object>();
					row.add(exp2.getName());
					row.add(exp2.getOp());
					row.add(exp2.getType());
					row.add(complexID++);
					row.add(exp.getRHS());
					row.add(exp2.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
					prereq2 = (Integer)row.get(3);
				}

				if (prereq2 == -1)
				{
					prereq2 = prereq1;
				}
				else if (prereq1 != -1)
				{
					ArrayList<Object> bottom = getBottomRow(row);
					bottom.remove(5);
					bottom.add(5, prereq1);
				}

				String name1 = null;
				String name2 = null;
				if (exp1 != null)
				{
					name1 = exp1.getName();
				}
				else
				{
					name1 = "";
					if (col1.getTable() != null)
					{
						name1 = col1.getTable();
					}
					name1 += ("." + col1.getColumn());
				}

				if (exp2 != null)
				{
					name2 = exp2.getName();
				}
				else
				{
					name2 = "";
					if (col2.getTable() != null)
					{
						name2 = col2.getTable();
					}
					name2 += ("." + col2.getColumn());
				}
				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					return new OperatorTypeAndName(new ConcatOperator(name1, name2, sgetName, meta), TYPE_INLINE, sgetName, prereq2);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(new ConcatOperator(name1, name2, name, meta), TYPE_INLINE, name, prereq2);
				}
			}

			// +,-,*, or /
			if ((exp1 == null || !(exp1.getOp() instanceof ExtendOperator)) && (exp2 == null || !(exp2.getOp() instanceof ExtendOperator)))
			{
				// externalize both exp1 and exp2
				ArrayList<Object> row = new ArrayList<Object>();
				int prereq1 = -1;
				int prereq2 = -1;
				if (exp1 != null)
				{
					row.add(exp1.getName());
					row.add(exp1.getOp());
					row.add(exp1.getType());
					row.add(complexID++);
					row.add(exp.getLHS());
					row.add(exp1.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
					prereq1 = (Integer)row.get(3);
				}

				if (exp2 != null)
				{
					row = new ArrayList<Object>();
					row.add(exp2.getName());
					row.add(exp2.getOp());
					row.add(exp2.getType());
					row.add(complexID++);
					row.add(exp.getRHS());
					row.add(exp2.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
					prereq2 = (Integer)row.get(3);
				}

				if (prereq2 == -1)
				{
					prereq2 = prereq1;
				}
				else if (prereq1 != -1)
				{
					ArrayList<Object> bottom = getBottomRow(row);
					bottom.remove(5);
					bottom.add(5, prereq1);
				}

				String name1 = null;

				String name2 = null;
				if (exp1 != null)
				{
					name1 = exp1.getName();
				}
				else
				{
					name1 = "";
					if (col1.getTable() != null)
					{
						name1 = col1.getTable();
					}
					name1 += ("." + col1.getColumn());
				}

				if (exp2 != null)
				{
					name2 = exp2.getName();
				}
				else
				{
					name2 = "";
					if (col2.getTable() != null)
					{
						name2 = col2.getTable();
					}
					name2 += ("." + col2.getColumn());
				}
				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					return new OperatorTypeAndName(new ExtendOperator(exp.getOp() + "," + name1 + "," + name2, sgetName, meta), TYPE_INLINE, sgetName, prereq2);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(new ExtendOperator(exp.getOp() + "," + name1 + "," + name2, name, meta), TYPE_INLINE, name, prereq2);
				}
			}
			else if (exp1 != null && exp1.getOp() instanceof ExtendOperator && exp2 != null && exp2.getOp() instanceof ExtendOperator)
			{
				ExtendOperator combined = null;
				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), sgetName, meta);
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
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					return new OperatorTypeAndName(combined, TYPE_INLINE, sgetName, myPrereq);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(combined, TYPE_INLINE, name, myPrereq);
				}
			}
			else if (exp1 != null && exp1.getOp() instanceof ExtendOperator)
			{
				ExtendOperator combined = null;
				String name2 = null;
				if (exp2 != null)
				{
					name2 = exp2.getName();
				}
				else
				{
					name2 = "";
					if (col2.getTable() != null)
					{
						name2 = col2.getTable();
					}

					name2 += ("." + col2.getColumn());
				}
				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + name2, sgetName, meta);
				}
				else
				{
					name = "._E" + suffix++;
					combined = new ExtendOperator(exp.getOp() + "," + ((ExtendOperator)exp1.getOp()).getPrefix() + "," + name2, name, meta);
				}

				// externalize exp2
				int prereq = -1;
				if (exp2 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp2.getName());
					row.add(exp2.getOp());
					row.add(exp2.getType());
					prereq = complexID++;
					row.add(prereq);
					row.add(exp.getRHS());
					row.add(exp2.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
					ArrayList<Object> bottom = getBottomRow(row);
					bottom.remove(5);
					bottom.add(5, exp1.getPrereq());
				}

				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					return new OperatorTypeAndName(combined, TYPE_INLINE, sgetName, prereq);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(combined, TYPE_INLINE, name, prereq);
				}
			}
			else
			{
				// exp2 instanceof ExtendOperator
				ExtendOperator combined = null;
				String name1 = null;
				if (exp1 != null)
				{
					name1 = exp1.getName();
				}
				else
				{
					name1 = "";
					if (col1.getTable() != null)
					{
						name1 = col1.getTable();
					}

					name1 += ("." + col1.getColumn());
				}
				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					combined = new ExtendOperator(exp.getOp() + "," + name1 + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), sgetName, meta);
				}
				else
				{
					name = "._E" + suffix++;
					combined = new ExtendOperator(exp.getOp() + "," + name1 + "," + ((ExtendOperator)exp2.getOp()).getPrefix(), name, meta);
				}

				// externalize exp1
				int prereq = -1;
				if (exp1 != null)
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(exp1.getName());
					row.add(exp1.getOp());
					row.add(exp1.getType());
					prereq = complexID++;
					row.add(prereq);
					row.add(exp.getLHS());
					row.add(exp1.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
					ArrayList<Object> bottom = getBottomRow(row);
					bottom.remove(5);
					bottom.add(5, exp2.getPrereq());
				}

				if (name != null)
				{
					String sgetName = name;
					if (!sgetName.contains("."))
					{
						sgetName = "." + sgetName;
					}
					return new OperatorTypeAndName(combined, TYPE_INLINE, sgetName, prereq);
				}
				else
				{
					name = "._E" + suffix++;
					return new OperatorTypeAndName(combined, TYPE_INLINE, name, prereq);
				}
			}
		}

		return null;
	}

	private Operator buildOperatorTreeFromFetchFirst(FetchFirst fetchFirst, Operator op) throws ParseException
	{
		try
		{
			TopOperator top = new TopOperator(fetchFirst.getNumber(), meta);
			top.add(op);
			return top;
		}
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}

	private Operator buildOperatorTreeFromFrom(FromClause from, SubSelect sub) throws Exception
	{
		ArrayList<TableReference> tables = from.getTables();
		ArrayList<Operator> ops = new ArrayList<Operator>(tables.size());
		for (TableReference table : tables)
		{
			ops.add(buildOperatorTreeFromTableReference(table, sub));
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
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			ops.remove(0);
			top = product;
		}

		return top;
	}

	private Operator buildOperatorTreeFromFullSelect(FullSelect select) throws Exception
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

		// handle connectedSelects
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
				catch (Exception e)
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
				catch (Exception e)
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
				catch (Exception e)
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
				catch (Exception e)
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

		// handle orderBy
		if (select.getOrderBy() != null)
		{
			op = buildOperatorTreeFromOrderBy(select.getOrderBy(), op);
		}
		// handle fetchFirst
		if (select.getFetchFirst() != null)
		{
			op = buildOperatorTreeFromFetchFirst(select.getFetchFirst(), op);
		}

		return op;
	}

	private Operator buildOperatorTreeFromGroupBy(GroupBy groupBy, Operator op, SubSelect select) throws ParseException
	{
		ArrayList<Column> cols;
		ArrayList<String> vStr;

		if (groupBy != null)
		{
			cols = groupBy.getCols();
			vStr = new ArrayList<String>(cols.size());
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
		}
		else
		{
			// cols = new ArrayList<Column>();
			vStr = new ArrayList<String>();
		}

		ArrayList<AggregateOperator> ops = new ArrayList<AggregateOperator>();
		for (ArrayList<Object> row : complex)
		{
			// colName, op, type, id, exp, prereq, done
			if ((Boolean)row.get(7) == false && (Integer)row.get(2) == TYPE_GROUPBY && (row.get(6) == null || ((SubSelect)row.get(6)).equals(select)))
			{
				AggregateOperator agop = (AggregateOperator)row.get(1);
				if (agop instanceof AvgOperator)
				{
					String col = getMatchingCol(op, agop.getInputColumn());
					agop.setInput(col);
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
						String col = getMatchingCol(op, agop.getInputColumn());
						agop.setInput(col);
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
						String col = getMatchingCol(op, agop.getInputColumn());
						agop.setInput(col);
						String type = op.getCols2Types().get(col);

						if (type == null)
						{
							HRDBMSWorker.logger.debug("Could not find " + col + " in " + op.getCols2Types());
							throw new ParseException("Column " + col + " was referenced but not found");
						}
					}
				}
				else if (agop instanceof MaxOperator)
				{
					String col = getMatchingCol(op, agop.getInputColumn());
					agop.setInput(col);
					String type = op.getCols2Types().get(col);

					if (type == null)
					{
						throw new ParseException("Column " + col + " was referenced but not found");
					}

					if (type.equals("INT"))
					{
						((MaxOperator)agop).setIsInt(true);
						((MaxOperator)agop).setIsLong(false);
						((MaxOperator)agop).setIsFloat(false);
						((MaxOperator)agop).setIsChar(false);
						((MaxOperator)agop).setIsDate(false);
					}
					else if (type.equals("LONG"))
					{
						((MaxOperator)agop).setIsInt(false);
						((MaxOperator)agop).setIsLong(true);
						((MaxOperator)agop).setIsFloat(false);
						((MaxOperator)agop).setIsChar(false);
						((MaxOperator)agop).setIsDate(false);
					}
					else if (type.equals("FLOAT"))
					{
						((MaxOperator)agop).setIsInt(false);
						((MaxOperator)agop).setIsLong(false);
						((MaxOperator)agop).setIsFloat(true);
						((MaxOperator)agop).setIsChar(false);
						((MaxOperator)agop).setIsDate(false);
					}
					else if (type.equals("CHAR"))
					{
						((MaxOperator)agop).setIsInt(false);
						((MaxOperator)agop).setIsLong(false);
						((MaxOperator)agop).setIsFloat(false);
						((MaxOperator)agop).setIsChar(true);
						((MaxOperator)agop).setIsDate(false);
					}
					else if (type.equals("DATE"))
					{
						((MaxOperator)agop).setIsInt(false);
						((MaxOperator)agop).setIsLong(false);
						((MaxOperator)agop).setIsFloat(false);
						((MaxOperator)agop).setIsChar(false);
						((MaxOperator)agop).setIsDate(true);
					}
				}
				else if (agop instanceof MinOperator)
				{
					String col = getMatchingCol(op, agop.getInputColumn());
					agop.setInput(col);
					String type = op.getCols2Types().get(col);

					if (type == null)
					{
						throw new ParseException("Column " + col + " was referenced but not found");
					}

					if (type.equals("INT"))
					{
						((MinOperator)agop).setIsInt(true);
						((MinOperator)agop).setIsLong(false);
						((MinOperator)agop).setIsFloat(false);
						((MinOperator)agop).setIsChar(false);
						((MinOperator)agop).setIsDate(false);
					}
					else if (type.equals("LONG"))
					{
						((MinOperator)agop).setIsInt(false);
						((MinOperator)agop).setIsLong(true);
						((MinOperator)agop).setIsFloat(false);
						((MinOperator)agop).setIsChar(false);
						((MinOperator)agop).setIsDate(false);
					}
					else if (type.equals("FLOAT"))
					{
						((MinOperator)agop).setIsInt(false);
						((MinOperator)agop).setIsLong(false);
						((MinOperator)agop).setIsFloat(true);
						((MinOperator)agop).setIsChar(false);
						((MinOperator)agop).setIsDate(false);
					}
					else if (type.equals("CHAR"))
					{
						((MinOperator)agop).setIsInt(false);
						((MinOperator)agop).setIsLong(false);
						((MinOperator)agop).setIsFloat(false);
						((MinOperator)agop).setIsChar(true);
						((MinOperator)agop).setIsDate(false);
					}
					else if (type.equals("DATE"))
					{
						((MinOperator)agop).setIsInt(false);
						((MinOperator)agop).setIsLong(false);
						((MinOperator)agop).setIsFloat(false);
						((MinOperator)agop).setIsChar(false);
						((MinOperator)agop).setIsDate(true);
					}
				}
				else if (agop instanceof SumOperator)
				{
					String col = getMatchingCol(op, agop.getInputColumn());
					agop.setInput(col);
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

		if (ops.size() > 0)
		{
			try
			{
				MultiOperator multi = new MultiOperator(ops, vStr, meta, false);
				try
				{
					multi.add(op);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("Exception trying to add MultiOperator.  Tree is: ");
					printTree(op, 0);
					throw e;
				}
				return multi;
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
		else
		{
			return op;
		}
	}

	private Operator buildOperatorTreeFromHaving(Having having, Operator op, SubSelect sub) throws Exception
	{
		SearchCondition search = having.getSearch();
		return buildOperatorTreeFromSearchCondition(search, op, sub);
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

		if (!meta.verifyTableExistence(schema, tbl, tx))
		{
			throw new ParseException("Table does not exist");
		}

		if (insert.fromSelect())
		{
			Operator op = buildOperatorTreeFromFullSelect(insert.getSelect());
			RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
			retval.add(op);
			Phase1 p1 = new Phase1(retval, tx);
			p1.optimize();
			new Phase2(retval, tx).optimize();
			new Phase3(retval, tx).optimize();
			new Phase4(retval, tx).optimize();
			new Phase5(retval, tx, p1.likelihoodCache).optimize();

			if (!meta.verifyInsert(schema, tbl, op, tx))
			{
				throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
			}

			Operator iOp = new InsertOperator(schema, tbl, meta);
			Operator child = retval.children().get(0);
			retval.removeChild(child);
			iOp.add(child);
			return iOp;
		}
		else
		{
			Operator op = new DummyOperator(meta);
			for (Expression exp : insert.getExpressions())
			{
				OperatorTypeAndName otan = buildOperatorTreeFromExpression(exp, null, null);
				if (otan.getType() != SQLParser.TYPE_INLINE && otan.getType() != SQLParser.TYPE_DATE)
				{
					throw new ParseException("Invalid expression in insert statement");
				}

				if (otan.getType() == SQLParser.TYPE_DATE)
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

			if (!meta.verifyInsert(schema, tbl, op, tx))
			{
				throw new ParseException("The number of columns and/or data types from the select portion do not match the table being inserted into");
			}

			Operator iOp = new InsertOperator(schema, tbl, meta);
			iOp.add(op);
			return iOp;
		}
	}

	private Operator buildOperatorTreeFromLoad(Load load) throws Exception
	{
		TableName table = load.getTable();
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

		if (!meta.verifyTableExistence(schema, tbl, tx))
		{
			throw new ParseException("Table does not exist");
		}

		return new LoadOperator(schema, tbl, load.isReplace(), load.getDelimiter(), load.getGlob(), meta);
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
						throw new ParseException("Column " + colStr + " does not exist");
					}
					else if (matches > 1)
					{
						throw new ParseException("Column " + colStr + " is ambiguous");
					}
				}
				else
				{
					int matches = 0;
					String table = null;
					boolean withDot = true;
					for (String c : op.getPos2Col().values())
					{
						if (c.contains("."))
						{
							String p1 = c.substring(0, c.indexOf('.'));
							String p2 = c.substring(c.indexOf('.') + 1);

							if (p2.equals(col.getColumn()))
							{
								matches++;
								table = p1;
							}
						}
						else
						{
							if (c.equals(col.getColumn()))
							{
								matches++;
								withDot = false;
							}
						}
					}

					if (matches == 0)
					{
						printTree(op, 0);
						throw new ParseException("Column " + col.getColumn() + " does not exist");
					}
					else if (matches > 1)
					{
						throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
					}

					if (withDot)
					{
						colStr = table + "." + col.getColumn();
					}
					else
					{
						colStr = col.getColumn();
					}
				}

				columns.add(colStr);
				orders.add(key.getDirection());
			}
			else
			{
				// handle numbered keys
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
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw new ParseException(e.getMessage());
		}
	}

	private Operator buildOperatorTreeFromRunstats(Runstats runstats) throws Exception
	{
		TableName table = runstats.getTable();
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

		Transaction tx2 = new Transaction(Transaction.ISOLATION_RR);
		if (!meta.verifyTableExistence(schema, tbl, tx2))
		{
			tx2.commit();
			throw new ParseException("Table does not exist");
		}

		tx2.commit();
		return new RunstatsOperator(schema, tbl, meta);
	}

	private Operator buildOperatorTreeFromSearchCondition(SearchCondition search, Operator op, SubSelect sub) throws Exception
	{
		if (search.getConnected() != null && search.getConnected().size() > 0)
		{
			convertToCNF(search);
		}

		SearchClause clause = search.getClause();

		if (search.getConnected() != null && search.getConnected().size() > 0 && search.getConnected().get(0).isAnd())
		{
			if (search.getClause().getPredicate() == null)
			{
				op = buildOperatorTreeFromSearchCondition(search.getClause().getSearch(), op, sub);
			}
			else
			{
				ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
				op = buildOperatorTreeFromSearchCondition(new SearchCondition(search.getClause(), cscs), op, sub);
			}

			for (ConnectedSearchClause csc : search.getConnected())
			{
				if (csc.getSearch().getPredicate() == null)
				{
					op = buildOperatorTreeFromSearchCondition(csc.getSearch().getSearch(), op, sub);
				}
				else
				{
					ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
					op = buildOperatorTreeFromSearchCondition(new SearchCondition(csc.getSearch(), cscs), op, sub);
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
						SubSelect clone = ((ExistsPredicate)pred).getSelect().clone();
						SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
						// select
						// list
						// and
						// search
						// conditions,
						// and
						// maybe
						// group
						// by
						if (negated)
						{
							Operator op2 = buildOperatorTreeFromSubSelect(clone);
							op2 = addRename(op2, join);
							return connectWithAntiJoin(op, op2, join);
						}
						else
						{
							Operator op2 = buildOperatorTreeFromSubSelect(clone);
							op2 = addRename(op2, join);
							return connectWithSemiJoin(op, op2, join);
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
								// TODO
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
							// is column unambiguous?
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
									throw new ParseException("Column " + lhsStr + " does not exist");
								}
								else if (matches > 1)
								{
									throw new ParseException("Column " + lhsStr + " is ambiguous");
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
									// could be a complex column
									for (ArrayList<Object> row : complex)
									{
										// colName, op, type, id, exp, prereq,
										// done
										if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
										{
											// its a match
											matches++;
										}
									}

									if (matches == 0)
									{
										throw new ParseException("Column " + col.getColumn() + " does not exist");
									}
									else if (matches > 1)
									{
										throw new ParseException("Column " + lhsStr + " is ambiguous");
									}

									for (ArrayList<Object> row : complex)
									{
										// colName, op, type, id, exp, prereq,
										// done
										if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
										{
											if (((Boolean)row.get(7)).equals(true))
											{
												throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
											}

											op = addComplexColumn(row, op, sub);
										}
									}
								}
								else if (matches > 1)
								{
									throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
								}

								lhsStr = table + "." + col.getColumn();
							}
						}
						else if (lhs.isCountStar())
						{
							throw new ParseException("Count(*) cannot be used in this context");
						}
						else if (lhs.isList())
						{
							throw new ParseException("A list cannot be used in this context");
						}
						else if (lhs.isSelect())
						{
							SubSelect sub2 = lhs.getSelect();
							if (isCorrelated(sub))
							{
								// lhsStr = getOneCol(sub);
								SubSelect clone = sub2.clone();
								SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
								// select
								// list
								// and
								// search
								// conditions
								ProductOperator product = new ProductOperator(meta);
								try
								{
									product.add(op);
									Operator op2 = buildOperatorTreeFromSubSelect(clone);
									op2 = addRename(op2, join);
									lhsStr = op2.getPos2Col().get(0);
									product.add(op2);
								}
								catch (Exception e)
								{
									throw new ParseException(e.getMessage());
								}
								op = buildOperatorTreeFromSearchCondition(join, product, sub);
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
								catch (Exception e)
								{
									throw new ParseException(e.getMessage());
								}
								op = extend;
								lhsStr = rhs2.getPos2Col().get(0);
							}
						}
						else
						{
							// check to see if complex already contains this
							// expression
							boolean found = false;
							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, sub, done
								if (row.get(4).equals(lhs) && row.get(6).equals(sub))
								{
									if ((Boolean)row.get(7) == true)
									{
										// found it
										found = true;
										lhsStr = (String)row.get(0);
										break;
									}
								}
							}

							if (!found)
							{
								// if not, build it
								OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null, sub);
								if (otan.getType() == TYPE_DAYS)
								{
									throw new ParseException("A type of DAYS is not allowed for a predicate");
								}

								if (otan.getType() == TYPE_MONTHS)
								{
									throw new ParseException("A type of MONTHS is not allowed for a predicate");
								}

								if (otan.getType() == TYPE_YEARS)
								{
									throw new ParseException("A type of YEARS is not allowed for a predicate");
								}

								if (otan.getType() == TYPE_DATE)
								{
									SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
									String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
									// String name = "._E" + suffix++;
									// ExtendObjectOperator operator = new
									// ExtendObjectOperator(DateParser.parse(dateString),
									// name, meta);
									// ArrayList<Object> row = new
									// ArrayList<Object>();
									// row.add(name);
									// row.add(operator);
									// row.add(TYPE_INLINE);
									// row.add(complexID++);
									// row.add(lhs);
									// row.add(-1);
									// row.add(sub);
									// row.add(false);
									// complex.add(row);
									// op = addComplexColumn(row, op);
									lhsStr = "DATE('" + dateString + "')";
								}
								else
								{
									// colName, op, type, id, exp, prereq, sub,
									// done
									ArrayList<Object> row = new ArrayList<Object>();
									row.add(otan.getName());
									row.add(otan.getOp());
									row.add(otan.getType());
									row.add(complexID++);
									row.add(lhs);
									row.add(otan.getPrereq());
									row.add(sub);
									row.add(false);
									complex.add(row);
									op = addComplexColumn(row, op, sub);
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
						throw new ParseException("Count(*) cannot be used in this context");
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
							final int size = list.size();
							while (i < size)
							{
								Expression exp = list.get(i);
								Predicate p = new Predicate(lhs, "E", exp);
								SearchClause s = new SearchClause(p, false);
								ConnectedSearchClause cs = new ConnectedSearchClause(s, false);
								ss.add(cs);
								i++;
							}

							SearchCondition sc = new SearchCondition(s1, ss);
							return buildOperatorTreeFromSearchCondition(sc, op, sub);
						}
						else
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "NE", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							ArrayList<ConnectedSearchClause> ss = new ArrayList<ConnectedSearchClause>();
							int i = 1;
							final int size = list.size();
							while (i < size)
							{
								Expression exp = list.get(i);
								Predicate p = new Predicate(lhs, "NE", exp);
								SearchClause s = new SearchClause(p, false);
								ConnectedSearchClause cs = new ConnectedSearchClause(s, true);
								ss.add(cs);
								i++;
							}

							SearchCondition sc = new SearchCondition(s1, ss);
							return buildOperatorTreeFromSearchCondition(sc, op, sub);
						}
					}
					else if (rhs.isSelect())
					{
						SubSelect sub2 = rhs.getSelect();
						if (isCorrelated(sub2))
						{
							// rhsStr = getOneCol(sub);
							SubSelect clone = sub2.clone();
							SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
							// select
							// list
							// and
							// search
							// conditions
							if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
							{
								Operator rhs2 = buildOperatorTreeFromSubSelect(clone);
								rhs2 = addRename(rhs2, join);
								rhsStr = rhs2.getPos2Col().get(0);
								verifyTypes(lhsStr, op, rhsStr, rhs2);
								try
								{
									op = connectWithSemiJoin(op, rhs2, join, new Filter(lhsStr, "E", rhsStr));
								}
								catch (Exception e)
								{
									throw new ParseException(e.getMessage());
								}
							}
							else
							{
								Operator rhs2 = buildOperatorTreeFromSubSelect(clone);
								rhs2 = addRename(rhs2, join);
								rhsStr = rhs2.getPos2Col().get(0);
								verifyTypes(lhsStr, op, rhsStr, rhs2);
								try
								{
									op = connectWithAntiJoin(op, rhs2, join, new Filter(lhsStr, "E", rhsStr));
								}
								catch (Exception e)
								{
									throw new ParseException(e.getMessage());
								}
							}

							return op;
						}
						else
						{
							Operator rhs2 = buildOperatorTreeFromSubSelect(sub2);
							rhsStr = rhs2.getPos2Col().get(0);
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
								catch (Exception e)
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
								catch (Exception e)
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
						// TODO
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
					// is column unambiguous?
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
							throw new ParseException("Column " + lhsStr + " does not exist");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + lhsStr + " is ambiguous");
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
							// could be a complex column
							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									// its a match
									matches++;
								}
							}

							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous");
							}

							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}

									op = addComplexColumn(row, op, sub);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
						}

						lhsStr = table + "." + col.getColumn();
					}
				}
				else if (lhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context");
				}
				else if (lhs.isList())
				{
					throw new ParseException("A list cannot be used in this context");
				}
				else if (lhs.isSelect())
				{
					SubSelect sub2 = lhs.getSelect();
					if (isCorrelated(sub))
					{
						// lhsStr = getOneCol(sub);
						SubSelect clone = sub2.clone();
						SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
						// select
						// list
						// and
						// search
						// conditions
						ProductOperator product = new ProductOperator(meta);
						try
						{
							product.add(op);
							Operator op2 = buildOperatorTreeFromSubSelect(clone);
							op2 = addRename(op2, join);
							lhsStr = op2.getPos2Col().get(0);
							product.add(op2);
						}
						catch (Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = buildOperatorTreeFromSearchCondition(join, product, sub);
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub2);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch (Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						lhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					// check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						// colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(lhs) && row.get(6).equals(sub))
						{
							if ((Boolean)row.get(7) == true)
							{
								// found it
								found = true;
								lhsStr = (String)row.get(0);
								break;
							}
						}
					}

					if (!found)
					{
						// if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null, sub);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_MONTHS)
						{
							throw new ParseException("A type of MONTHS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_YEARS)
						{
							throw new ParseException("A type of YEARS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							// String name = "._E" + suffix++;
							// ExtendObjectOperator operator = new
							// ExtendObjectOperator(DateParser.parse(dateString),
							// name, meta);
							// ArrayList<Object> row = new ArrayList<Object>();
							// row.add(name);
							// row.add(operator);
							// row.add(TYPE_INLINE);
							// row.add(complexID++);
							// row.add(lhs);
							// row.add(-1);
							// row.add(sub);
							// row.add(false);
							// complex.add(row);
							// op = addComplexColumn(row, op);
							lhsStr = "DATE('" + dateString + "')";
						}
						else
						{
							// colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(lhs);
							row.add(otan.getPrereq());
							row.add(sub);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op, sub);
							lhsStr = otan.getName();
						}
					}
				}

				// do the same for rhs
				if (rhs.isLiteral())
				{
					Literal literal = rhs.getLiteral();
					if (literal.isNull())
					{
						// TODO
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
					// is column unambiguous?
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
							throw new ParseException("Column " + rhsStr + " does not exist");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + rhsStr + " is ambiguous");
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
							// could be a complex column
							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									// its a match
									matches++;
								}
							}

							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous");
							}

							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}

									op = addComplexColumn(row, op, sub);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
						}

						rhsStr = table + "." + col.getColumn();
					}
				}
				else if (rhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context");
				}
				else if (rhs.isList())
				{
					throw new ParseException("A list cannot be used in this context");
				}
				else if (rhs.isSelect())
				{
					SubSelect sub2 = rhs.getSelect();
					if (isCorrelated(sub2))
					{
						// rhsStr = getOneCol(sub);
						SubSelect clone = sub2.clone();
						SearchCondition join = rewriteCorrelatedSubSelect(clone); // update
						// select
						// list
						// and
						// search
						// conditions
						ProductOperator product = new ProductOperator(meta);
						try
						{
							product.add(op);
							Operator op2 = buildOperatorTreeFromSubSelect(clone);
							op2 = addRename(op2, join);
							rhsStr = op2.getPos2Col().get(0);
							product.add(op2);
						}
						catch (Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = buildOperatorTreeFromSearchCondition(join, product, sub);
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub2))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub2);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch (Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						rhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					// check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						// colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(rhs) && row.get(6).equals(sub))
						{
							if ((Boolean)row.get(7) == true)
							{
								// found it
								found = true;
								rhsStr = (String)row.get(0);
								break;
							}
						}
					}

					if (!found)
					{
						// if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(rhs, null, sub);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_MONTHS)
						{
							throw new ParseException("A type of MONTHS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_YEARS)
						{
							throw new ParseException("A type of YEARS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							// String name = "._E" + suffix++;
							// ExtendObjectOperator operator = new
							// ExtendObjectOperator(DateParser.parse(dateString),
							// name, meta);
							// ArrayList<Object> row = new ArrayList<Object>();
							// row.add(name);
							// row.add(operator);
							// row.add(TYPE_INLINE);
							// row.add(complexID++);
							// row.add(rhs);
							// row.add(-1);
							// row.add(sub);
							// row.add(false);
							// complex.add(row);
							// op = addComplexColumn(row, op);
							rhsStr = "DATE('" + dateString + "')";
						}
						else
						{
							// colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(rhs);
							row.add(otan.getPrereq());
							row.add(sub);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op, sub);
							rhsStr = otan.getName();
						}
					}
				}

				// add SelectOperator to top of tree
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
				catch (Exception e)
				{
					throw new ParseException(e.getMessage());
				}
			}
			else
			{
				SearchCondition s = clause.getSearch();
				if (!negated)
				{
					return buildOperatorTreeFromSearchCondition(s, op, sub);
				}
				else
				{
					negateSearchCondition(s);
					return buildOperatorTreeFromSearchCondition(s, op, sub);
				}
			}
		}
		else
		{
			// ored
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
					if (!rhs.isList())
					{
						if (lhs.isLiteral())
						{
							Literal literal = lhs.getLiteral();
							if (literal.isNull())
							{
								// TODO
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
							// is column unambiguous?
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
									throw new ParseException("Column " + lhsStr + " does not exist");
								}
								else if (matches > 1)
								{
									throw new ParseException("Column " + lhsStr + " is ambiguous");
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
									// could be a complex column
									for (ArrayList<Object> row : complex)
									{
										// colName, op, type, id, exp, prereq,
										// done
										if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
										{
											// its a match
											matches++;
										}
									}

									if (matches == 0)
									{
										throw new ParseException("Column " + col.getColumn() + " does not exist");
									}
									else if (matches > 1)
									{
										throw new ParseException("Column " + lhsStr + " is ambiguous");
									}

									for (ArrayList<Object> row : complex)
									{
										// colName, op, type, id, exp, prereq,
										// done
										if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
										{
											if (((Boolean)row.get(7)).equals(true))
											{
												throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
											}

											op = addComplexColumn(row, op, sub);
										}
									}
								}
								else if (matches > 1)
								{
									throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
								}

								lhsStr = table + "." + col.getColumn();
							}
						}
						else if (lhs.isCountStar())
						{
							throw new ParseException("Count(*) cannot be used in this context");
						}
						else if (lhs.isList())
						{
							throw new ParseException("A list cannot be used in this context");
						}
						else if (lhs.isSelect())
						{
							throw new ParseException("Restriction: An IN predicate cannot contain a subselect if it is connected with a logical OR");
						}
						else
						{
							// check to see if complex already contains this
							// expression
							boolean found = false;
							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, sub, done
								if (row.get(4).equals(lhs) && row.get(6).equals(sub))
								{
									if ((Boolean)row.get(7) == true)
									{
										// found it
										found = true;
										lhsStr = (String)row.get(0);
										break;
									}
								}
							}

							if (!found)
							{
								// if not, build it
								OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null, sub);
								if (otan.getType() == TYPE_DAYS)
								{
									throw new ParseException("A type of DAYS is not allowed for a predicate");
								}

								if (otan.getType() == TYPE_MONTHS)
								{
									throw new ParseException("A type of MONTHS is not allowed for a predicate");
								}

								if (otan.getType() == TYPE_YEARS)
								{
									throw new ParseException("A type of YEARS is not allowed for a predicate");
								}

								if (otan.getType() == TYPE_DATE)
								{
									SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
									String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
									// String name = "._E" + suffix++;
									// ExtendObjectOperator operator = new
									// ExtendObjectOperator(DateParser.parse(dateString),
									// name, meta);
									// ArrayList<Object> row = new
									// ArrayList<Object>();
									// row.add(name);
									// row.add(operator);
									// row.add(TYPE_INLINE);
									// row.add(complexID++);
									// row.add(lhs);
									// row.add(-1);
									// row.add(sub);
									// row.add(false);
									// complex.add(row);
									// op = addComplexColumn(row, op);
									lhsStr = "DATE('" + dateString + "')";
								}
								else
								{
									// colName, op, type, id, exp, prereq, sub,
									// done
									ArrayList<Object> row = new ArrayList<Object>();
									row.add(otan.getName());
									row.add(otan.getOp());
									row.add(otan.getType());
									row.add(complexID++);
									row.add(lhs);
									row.add(otan.getPrereq());
									row.add(sub);
									row.add(false);
									complex.add(row);
									op = addComplexColumn(row, op, sub);
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
						throw new ParseException("Count(*) cannot be used in this context");
					}
					else if (rhs.isList())
					{
						if ((o.equals("IN") && !negated) || (o.equals("NI") && negated))
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "E", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							preds.add(s1);
							new ArrayList<ConnectedSearchClause>();
							int j = 1;
							final int size = list.size();
							while (j < size)
							{
								Expression exp = list.get(j);
								Predicate p = new Predicate(lhs, "E", exp);
								SearchClause s = new SearchClause(p, false);
								preds.add(s);
								j++;
							}

							i++;
							continue;
						}
						else
						{
							ArrayList<Expression> list = rhs.getList();
							Predicate predicate = new Predicate(lhs, "NE", list.get(0));
							SearchClause s1 = new SearchClause(predicate, false);
							preds.add(s1);
							int j = 1;
							final int size = list.size();
							while (j < size)
							{
								Expression exp = list.get(j);
								Predicate p = new Predicate(lhs, "NE", exp);
								SearchClause s = new SearchClause(p, false);
								preds.add(s);
								j++;
							}

							i++;
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
						// TODO
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
					// is column unambiguous?
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
							throw new ParseException("Column " + lhsStr + " does not exist");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + lhsStr + " is ambiguous");
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
							// could be a complex column
							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									// its a match
									matches++;
								}
							}

							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous");
							}

							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}

									op = addComplexColumn(row, op, sub);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
						}

						lhsStr = table + "." + col.getColumn();
					}
				}
				else if (lhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context");
				}
				else if (lhs.isList())
				{
					throw new ParseException("A list cannot be used in this context");
				}
				else if (lhs.isSelect())
				{
					SubSelect sub2 = lhs.getSelect();
					if (isCorrelated(sub2))
					{
						throw new ParseException("Restriction: A correlated subquery cannot be used in a predicate that is part of a logical OR");
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub2))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub2);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch (Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						lhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					// check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						// colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(lhs) && row.get(6).equals(sub))
						{
							if ((Boolean)row.get(7) == true)
							{
								// found it
								found = true;
								lhsStr = (String)row.get(0);
								break;
							}
						}
					}

					if (!found)
					{
						// if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(lhs, null, sub);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_MONTHS)
						{
							throw new ParseException("A type of MONTHS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_YEARS)
						{
							throw new ParseException("A type of YEARS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							// String name = "._E" + suffix++;
							// ExtendObjectOperator operator = new
							// ExtendObjectOperator(DateParser.parse(dateString),
							// name, meta);
							// ArrayList<Object> row = new ArrayList<Object>();
							// row.add(name);
							// row.add(operator);
							// row.add(TYPE_INLINE);
							// row.add(complexID++);
							// row.add(lhs);
							// row.add(-1);
							// row.add(sub);
							// row.add(false);
							// complex.add(row);
							// op = addComplexColumn(row, op);
							lhsStr = "DATE('" + dateString + "')";
						}
						else
						{
							// colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(lhs);
							row.add(otan.getPrereq());
							row.add(sub);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op, sub);
							lhsStr = otan.getName();
						}
					}
				}

				// do the same for rhs
				if (rhs.isLiteral())
				{
					Literal literal = rhs.getLiteral();
					if (literal.isNull())
					{
						// TODO
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
					// is column unambiguous?
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
							throw new ParseException("Column " + rhsStr + " does not exist");
						}
						else if (matches > 1)
						{
							throw new ParseException("Column " + rhsStr + " is ambiguous");
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
							// could be a complex column
							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									// its a match
									matches++;
								}
							}

							if (matches == 0)
							{
								throw new ParseException("Column " + col.getColumn() + " does not exist");
							}
							else if (matches > 1)
							{
								throw new ParseException("Column " + lhsStr + " is ambiguous");
							}

							for (ArrayList<Object> row : complex)
							{
								// colName, op, type, id, exp, prereq, done
								if (row.get(0).equals("." + col.getColumn()) && row.get(6).equals(sub))
								{
									if (((Boolean)row.get(7)).equals(true))
									{
										throw new ParseException("Internal error during SQL parsing.  A WHERE clause was found referencing a complex column that was marked as done but was not available in the operator tree");
									}

									op = addComplexColumn(row, op, sub);
								}
							}
						}
						else if (matches > 1)
						{
							throw new ParseException("Reference to column " + col.getColumn() + " is ambiguous");
						}

						rhsStr = table + "." + col.getColumn();
					}
				}
				else if (rhs.isCountStar())
				{
					throw new ParseException("Count(*) cannot be used in this context");
				}
				else if (rhs.isList())
				{
					throw new ParseException("A list cannot be used in this context");
				}
				else if (rhs.isSelect())
				{
					SubSelect sub2 = rhs.getSelect();
					if (isCorrelated(sub2))
					{
						throw new ParseException("Restriction: A correlated subquery cannot be used in a predicate that is part of a logical OR");
					}
					else
					{
						ProductOperator extend = new ProductOperator(meta);
						if (!ensuresOnlyOneRow(sub2))
						{
							throw new ParseException("A SubSelect is present which could return more than 1 row, but cannot do so.  Please rewrite it to ensure only 1 row is returned.");
						}
						Operator rhs2 = buildOperatorTreeFromSubSelect(sub2);
						if (rhs2.getCols2Pos().size() != 1)
						{
							throw new ParseException("A SubSelect is present which must return only 1 column, but insted returns more than 1");
						}
						try
						{
							extend.add(op);
							extend.add(rhs2);
						}
						catch (Exception e)
						{
							throw new ParseException(e.getMessage());
						}
						op = extend;
						rhsStr = rhs2.getPos2Col().get(0);
					}
				}
				else
				{
					// check to see if complex already contains this expression
					boolean found = false;
					for (ArrayList<Object> row : complex)
					{
						// colName, op, type, id, exp, prereq, sub, done
						if (row.get(4).equals(rhs) && row.get(6).equals(sub))
						{
							if ((Boolean)row.get(7) == true)
							{
								// found it
								found = true;
								rhsStr = (String)row.get(0);
								break;
							}
						}
					}

					if (!found)
					{
						// if not, build it
						OperatorTypeAndName otan = buildOperatorTreeFromExpression(rhs, null, sub);
						if (otan.getType() == TYPE_DAYS)
						{
							throw new ParseException("A type of DAYS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_MONTHS)
						{
							throw new ParseException("A type of MONTHS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_YEARS)
						{
							throw new ParseException("A type of YEARS is not allowed for a predicate");
						}

						if (otan.getType() == TYPE_DATE)
						{
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							String dateString = sdf.format(((GregorianCalendar)otan.getOp()).getTime());
							// String name = "._E" + suffix++;
							// ExtendObjectOperator operator = new
							// ExtendObjectOperator(DateParser.parse(dateString),
							// name, meta);
							// ArrayList<Object> row = new ArrayList<Object>();
							// row.add(name);
							// row.add(operator);
							// row.add(TYPE_INLINE);
							// row.add(complexID++);
							// row.add(rhs);
							// row.add(-1);
							// row.add(sub);
							// row.add(false);
							// complex.add(row);
							// op = addComplexColumn(row, op);
							rhsStr = "DATE('" + dateString + "')";
						}
						else
						{
							// colName, op, type, id, exp, prereq, sub, done
							ArrayList<Object> row = new ArrayList<Object>();
							row.add(otan.getName());
							row.add(otan.getOp());
							row.add(otan.getType());
							row.add(complexID++);
							row.add(rhs);
							row.add(otan.getPrereq());
							row.add(sub);
							row.add(false);
							complex.add(row);
							op = addComplexColumn(row, op, sub);
							rhsStr = otan.getName();
						}
					}
				}

				// add SelectOperator to top of tree
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
					ors.add(new Filter(lhsStr, o, rhsStr));
				}
				catch (Exception e)
				{
					throw new ParseException(e.getMessage());
				}

				i++;
			}

			try
			{
				SelectOperator select = new SelectOperator(ors, meta);
				select.add(op);
				return select;
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}
	}

	private Operator buildOperatorTreeFromSelect(Select select) throws Exception
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

	private Operator buildOperatorTreeFromSelectClause(SelectClause select, Operator op, SubSelect sub) throws ParseException
	{
		if (!select.isSelectStar())
		{
			ArrayList<String> cols = new ArrayList<String>();
			ArrayList<SelectListEntry> selects = select.getSelectList();
			boolean needsRename = false;
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
						String theName = entry.getName();
						if (!theName.contains("."))
						{
							theName = "." + theName;
						}
						cols.add(theName);
					}
					else
					{
						// unnamed complex column
						for (ArrayList<Object> row : complex)
						{
							if (row.get(4).equals(entry.getExpression()) && row.get(6).equals(sub))
							{
								String name = (String)row.get(0);
								if (name.indexOf('.') == 0)
								{
									name = name.substring(1);
								}
								cols.add(name);
							}
						}
					}
				}
			}

			ArrayList<String> newCols = new ArrayList<String>();
			ArrayList<String> olds = new ArrayList<String>();
			ArrayList<String> news = new ArrayList<String>();
			// HRDBMSWorker.logger.debug("Cols = " + cols);
			for (String col : cols)
			{
				String col2 = null;
				SelectListEntry sle = null;
				for (SelectListEntry entry : selects)
				{
					if (entry.isColumn())
					{
						Column c = entry.getColumn();
						col2 = "";
						if (c.getTable() != null)
						{
							col2 += c.getTable();
						}

						col2 += ("." + c.getColumn());
					}
					else
					{
						if (entry.getName() != null)
						{
							String theName = entry.getName();
							if (!theName.contains("."))
							{
								theName = "." + theName;
							}
							col2 = theName;
						}
						else
						{
							for (ArrayList<Object> row : complex)
							{
								if (row.get(4).equals(entry.getExpression()) && row.get(6).equals(sub))
								{
									col2 = (String)row.get(0);
									if (col2.indexOf('.') == 0)
									{
										col2 = col2.substring(1);
										if (col.equals(col2))
										{
											break;
										}
									}
								}
							}
						}
					}

					if (col.equals(col2))
					{
						sle = entry;
						break;
					}
				}

				Integer pos = op.getCols2Pos().get(col);
				// HRDBMSWorker.logger.debug("Cols2Pos = " + op.getCols2Pos());
				if (pos == null)
				{
					// try without schema
					// must only be 1 match
					if (col.contains("."))
					{
						if (col.indexOf('.') != 0)
						{
							throw new ParseException("Column not found: " + col);
						}
						else
						{
							col = col.substring(1);
						}
					}
					for (String col3 : op.getCols2Pos().keySet())
					{
						int matches = 0;
						String u = col3.substring(col3.indexOf('.') + 1);
						if (col.equals(u))
						{
							newCols.add(col3);
							if (sle != null && sle.getName() != null && !sle.getName().equals(col3))
							{
								needsRename = true;
								olds.add(col3);
								news.add(sle.getName());
							}
							matches++;
						}

						if (matches > 1)
						{
							throw new ParseException("Ambiguous acolumn: " + col);
						}
					}
				}
				else
				{
					newCols.add(col);
					if (sle != null && sle.getName() != null && !sle.getName().equals(col))
					{
						needsRename = true;
						olds.add(col);
						news.add(sle.getName());
					}
				}
			}

			cols = newCols;
			// HRDBMSWorker.logger.debug("After all the magic, cols = " + cols);
			try
			{
				ReorderOperator reorder = new ReorderOperator(cols, meta);
				reorder.add(op);
				op = reorder;
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}

			if (needsRename)
			{
				RenameOperator rename = null;
				try
				{
					rename = new RenameOperator(olds, news, meta);
					rename.add(op);
				}
				catch (Exception e)
				{
					throw new ParseException(e.getMessage());
				}

				op = rename;
			}
		}
		else
		{
			ArrayList<String> cols = new ArrayList<String>(op.getPos2Col().values().size());
			for (String col : op.getPos2Col().values())
			{
				if (!col.startsWith("_"))
				{
					cols.add(col);
				}
			}
			try
			{
				ReorderOperator reorder = new ReorderOperator(cols, meta);
				reorder.add(op);
				op = reorder;
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}

		if (!select.isSelectAll())
		{
			UnionOperator distinct = new UnionOperator(true, meta);
			try
			{
				distinct.add(op);
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			op = distinct;
		}

		return op;
	}

	private Operator buildOperatorTreeFromSingleTable(SingleTable table) throws Exception
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

		if (!meta.verifyTableExistence(schema, tblName, tx))
		{
			if (!meta.verifyViewExistence(schema, tblName, tx))
			{
				throw new ParseException("Table or view " + schema + "." + tblName + " does not exist");
			}

			SQLParser viewParser = new SQLParser(meta.getViewSQL(schema, tblName, tx), connection, tx);
			Operator op = null;
			try
			{
				op = viewParser.parse();
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
			if (!viewParser.doesNotUseCurrentSchema())
			{
				doesNotUseCurrentSchema = false;
			}

			Operator retval = op.children().get(0);
			op.removeChild(retval);

			ArrayList<String> olds = new ArrayList<String>();
			ArrayList<String> news = new ArrayList<String>();

			for (String c : retval.getCols2Pos().keySet())
			{
				if (!c.contains("."))
				{
					olds.add(c);
					news.add("." + c);
				}
			}

			if (news.size() == 0)
			{
				return retval;
			}
			else
			{
				RenameOperator rename = new RenameOperator(olds, news, meta);
				rename.add(retval);
				return rename;
			}
		}

		TableScanOperator op = null;
		try
		{
			op = new TableScanOperator(schema, tblName, meta, tx);
		}
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}

		if (table.getAlias() != null)
		{
			op.setAlias(table.getAlias());
		}

		return op;
	}

	private Operator buildOperatorTreeFromSubSelect(SubSelect select) throws Exception
	{
		Operator op = buildOperatorTreeFromFrom(select.getFrom(), select);
		getComplexColumns(select.getSelect(), select, select.getHaving());
		// HRDBMSWorker.logger.debug(complex);
		op = buildNGBExtends(op, select);
		// HRDBMSWorker.logger.debug("Did buildNGBExtends");
		// HRDBMSWorker.logger.debug(complex);
		if (select.getWhere() != null)
		{
			op = buildOperatorTreeFromWhere(select.getWhere(), op, select);
		}

		// handle groupBy
		if (updateGroupByNeeded(select))
		{
			op = buildOperatorTreeFromGroupBy(select.getGroupBy(), op, select);
		}

		// handle extends that haven't been done
		op = buildNGBExtends(op, select);
		// handle having
		if (select.getHaving() != null)
		{
			op = buildOperatorTreeFromHaving(select.getHaving(), op, select);
		}

		op = buildOperatorTreeFromSelectClause(select.getSelect(), op, select);
		// handle orderBy
		if (select.getOrderBy() != null)
		{
			op = buildOperatorTreeFromOrderBy(select.getOrderBy(), op);
		}

		// handle fetchFirst
		if (select.getFetchFirst() != null)
		{
			op = buildOperatorTreeFromFetchFirst(select.getFetchFirst(), op);
		}
		return op;
	}

	private Operator buildOperatorTreeFromTableReference(TableReference table, SubSelect sub) throws Exception
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
				// rename cols
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
						String end = (table.getSelect().getCols().get(i)).getColumn();
						if (!end.contains("."))
						{
							end = "." + end;
						}

						newCol += end;
						newCols.add(newCol);
						i++;
					}

					try
					{
						RenameOperator rename = new RenameOperator(original, newCols, meta);
						rename.add(op);
						return rename;
					}
					catch (Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
				else
				{
					// rename cols and table
					int i = 0;
					for (String col : op.getPos2Col().values())
					{
						original.add(col);
						String newCol = table.getAlias();
						String end = (table.getSelect().getCols().get(i)).getColumn();
						if (!end.contains("."))
						{
							end = "." + end;
						}

						newCol += end;
						newCols.add(newCol);
						i++;
					}

					try
					{
						RenameOperator rename = new RenameOperator(original, newCols, meta);
						rename.add(op);
						return rename;
					}
					catch (Exception e)
					{
						throw new ParseException(e.getMessage());
					}
				}
			}
			else
			{
				// check for need to rename table
				if (table.getAlias() != null)
				{
					// rename table
					ArrayList<String> original = new ArrayList<String>();
					ArrayList<String> newCols = new ArrayList<String>();
					for (String col : op.getPos2Col().values())
					{
						original.add(col);
						String newCol = col.substring(col.indexOf('.') + 1);
						newCols.add(table.getAlias() + "." + newCol);
					}

					try
					{
						RenameOperator rename = new RenameOperator(original, newCols, meta);
						rename.add(op);
						return rename;
					}
					catch (Exception e)
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

		Operator op1 = buildOperatorTreeFromTableReference(table.getLHS(), sub);
		Operator op2 = buildOperatorTreeFromTableReference(table.getRHS(), sub);
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
			catch (Exception e)
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
				Operator o = buildOperatorTreeFromSearchCondition(table.getSearch(), product, sub);
				if (table.getAlias() != null)
				{
					return handleAlias(table.getAlias(), o);
				}

				return o;
			}
			catch (Exception e)
			{
				throw new ParseException(e.getMessage());
			}
		}

		if (op.equals("L"))
		{
			// TODO
		}

		if (op.equals("R"))
		{
			// TODO
		}

		if (op.equals("F"))
		{
			// TODO
		}

		return null;
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

		if (!meta.verifyTableExistence(schema, tbl, tx))
		{
			throw new ParseException("Table does not exist");
		}

		TableScanOperator scan = new TableScanOperator(schema, tbl, meta, tx);
		Operator op = null;
		if (update.getWhere() != null)
		{
			op = buildOperatorTreeFromWhere(update.getWhere(), scan, null);
		}
		else
		{
			op = scan;
		}
		scan.getRID();
		Operator op2;
		if (op != scan)
		{
			op2 = scan.firstParent();
			op2.removeChild(scan);
			op2.add(scan);
			while (op2 != op)
			{
				Operator op3 = op2.parent();
				op3.removeChild(op2);
				op3.add(op2);
				op2 = op3;
			}
		}

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
			OperatorTypeAndName otan = buildOperatorTreeFromExpression(exp, null, null);
			if (otan.getType() != SQLParser.TYPE_INLINE && otan.getType() != SQLParser.TYPE_DATE)
			{
				throw new ParseException("Invalid expression in update statement");
			}

			if (otan.getType() == SQLParser.TYPE_DATE)
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

		if (!meta.verifyUpdate(schema, tbl, update.getCols(), buildList, op, tx))
		{
			throw new ParseException("The number of columns and/or data types do not match the columns being updated");
		}

		ArrayList<String> cols = new ArrayList<String>();
		for (String col : op.getPos2Col().values())
		{
			cols.add(col);
		}

		// cols.add("_RID1");
		// cols.add("_RID2");
		// cols.add("_RID3");
		// cols.add("_RID4");

		ReorderOperator reorder = new ReorderOperator(cols, meta);
		reorder.add(op);
		op = reorder;
		RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
		retval.add(op);
		Phase1 p1 = new Phase1(retval, tx);
		p1.optimize();
		new Phase2(retval, tx).optimize();
		new Phase3(retval, tx).optimize();
		new Phase4(retval, tx).optimize();
		new Phase5(retval, tx, p1.likelihoodCache).optimize();
		UpdateOperator uOp = new UpdateOperator(schema, tbl, update.getCols(), buildList, meta);
		Operator child = retval.children().get(0);
		retval.removeChild(child);
		uOp.add(child);
		return uOp;
	}

	private Operator buildOperatorTreeFromWhere(Where where, Operator op, SubSelect sub) throws Exception
	{
		SearchCondition search = where.getSearch();
		return buildOperatorTreeFromSearchCondition(search, op, sub);
	}

	private void checkSizeOfNewCols(ArrayList<Column> newCols, Operator op) throws ParseException
	{
		if (newCols.size() != op.getPos2Col().size())
		{
			throw new ParseException("The common table expression has the wrong number of columns");
		}
	}

	private Operator connectWithAntiJoin(Operator left, Operator right, SearchCondition join) throws ParseException
	{
		// assume join is already cnf
		// and contains only columns
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
				lhs = getMatchingCol(left, lhs);
				rhs = getMatchingCol(right, rhs);
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
					{
						if (e instanceof ParseException)
						{
							throw (ParseException)e;
						}
						else
						{
							throw new ParseException(e.getMessage());
						}
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
						lhs = getMatchingCol(left, lhs);
						rhs = getMatchingCol(right, rhs);
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch (Exception e)
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
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}

	private Operator connectWithAntiJoin(Operator left, Operator right, SearchCondition join, Filter filter) throws ParseException
	{
		// assume join is already cnf
		// and contains only columns
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
				lhs = getMatchingCol(left, lhs);
				rhs = getMatchingCol(right, rhs);
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
						lhs = getMatchingCol(left, lhs);
						rhs = getMatchingCol(right, rhs);
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch (Exception e)
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
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}

	private Operator connectWithSemiJoin(Operator left, Operator right, SearchCondition join) throws ParseException
	{
		// assume join is already cnf
		// and contains only columns
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
				lhs = getMatchingCol(left, lhs);
				rhs = getMatchingCol(right, rhs);
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
						lhs = getMatchingCol(left, lhs);
						rhs = getMatchingCol(right, rhs);
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch (Exception e)
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
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}

	private Operator connectWithSemiJoin(Operator left, Operator right, SearchCondition join, Filter filter) throws ParseException
	{
		// assume join is already cnf
		// and contains only columns
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
			lhs = getMatchingCol(left, lhs);
			rhs = getMatchingCol(right, rhs);
			try
			{
				verifyTypes(lhs, left, rhs, right);
				Filter f = new Filter(lhs, o, rhs);
				hm.put(f, f);
			}
			catch (Exception e)
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
				lhs = getMatchingCol(left, lhs);
				rhs = getMatchingCol(right, rhs);
				try
				{
					verifyTypes(lhs, left, rhs, right);
					Filter f = new Filter(lhs, o, rhs);
					hm.put(f, f);
				}
				catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
					lhs = getMatchingCol(left, lhs);
					rhs = getMatchingCol(right, rhs);
					try
					{
						verifyTypes(lhs, left, rhs, right);
						Filter f = new Filter(lhs, o, rhs);
						hm.put(f, f);
					}
					catch (Exception e)
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
						lhs = getMatchingCol(left, lhs);
						rhs = getMatchingCol(right, rhs);
						try
						{
							verifyTypes(lhs, left, rhs, right);
							Filter f = new Filter(lhs, o, rhs);
							hm.put(f, f);
						}
						catch (Exception e)
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
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}
	}

	private boolean containsAggregation(Expression exp)
	{
		if (exp.isColumn())
		{
			return false;
		}
		else if (exp.isCountStar())
		{
			return true;
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

	private boolean containsAggregation(SearchClause clause)
	{
		if (clause.getPredicate() != null)
		{
			Predicate p = clause.getPredicate();
			if (p.getLHS() == null)
			{
				return false;
			}
			if (p.getRHS() == null)
			{
				return false;
			}
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
				return false;
			}
			else
			{
				if (colString.indexOf('.') != 0)
				{
					return true;
				}
				else
				{
					colString = colString.substring(1);
				}

				int matches = 0;
				for (String c : cols2Pos.keySet())
				{
					if (c.contains("."))
					{
						c = c.substring(c.indexOf('.') + 1);
					}

					if (c.equals(colString))
					{
						matches++;
					}
				}

				if (matches == 0)
				{
					return true;
				}

				if (matches == 1)
				{
					return false;
				}

				throw new ParseException("Ambiguous reference to " + colString);
			}
		}

		// if it contains a correlated column, it is a ParseException, otherwise
		// return false
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
		else if (exp.isCase())
		{
			for (Case c : exp.getCases())
			{
				if (containsCorrelatedCol(c.getResult(), cols2Pos))
				{
					throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
				}
			}

			if (containsCorrelatedCol(exp.getDefault(), cols2Pos))
			{
				throw new ParseException("Restriction: A correlated join in a correlated subquery may only refer to columns");
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
			// we must have mixed ands and ors
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
			// starts with and
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
				// A is all anded predicates
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

				// figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					// B is all anded predicates
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
					// B all ors
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
					// A AND B CNF
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
				// A all ored preds
				// figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					// B is all anded predicates
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
					// B all ors
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
					// A ors B CNF
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
				// A is CNF
				// figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					// A CNF B AND
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
					// A CNF B OR
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
					// A CNF B CNF
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
			// starts with or
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
				// A is all anded predicates
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

				// figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					// B is all anded predicates
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
					// B all ors
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
					// A AND B CNF
					s.getConnected().remove(0);
					// build list of ored clauses in B
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
				// A all ored preds
				// figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					// B is all anded predicates
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
					// B all ors
					s.getConnected().remove(0);
					ArrayList<ConnectedSearchClause> cscs = sc.getSearch().getConnected();
					cscs.add(new ConnectedSearchClause(sc2.getSearch().getClause(), false));
					cscs.addAll(sc2.getSearch().getConnected());
					convertToCNF(s);
					return;
				}
				else
				{
					// A ors B CNF
					s.getConnected().remove(0);
					// build list of ored clauses in B
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
				// A is CNF
				// figure out what B is
				SearchClause sc2 = s.getConnected().get(0).getSearch();
				if (sc2.getPredicate() != null || (allPredicates(sc2.getSearch()) && allAnd(sc2.getSearch())))
				{
					// A CNF B AND
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
					// build list of ored clauses in A
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
					// A CNF B OR
					s.getConnected().remove(0);
					// build list of ored clauses in A
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
					// A CNF B CNF
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

	private boolean ensuresOnlyOneRow(SubSelect sub) throws Exception
	{
		if (sub.getFetchFirst() != null && sub.getFetchFirst().getNumber() == 1)
		{
			return true;
		}

		if (sub.getGroupBy() != null)
		{
			return false;
		}

		// if we find a column in the select list that uses a aggregation
		// function, return true
		// else return false
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
			else
			{
				throw new Exception("Unexpected expression type in ensureOnlyOneRow()");
			}

			if (doesAggregation(args)) // NULL args?
			{
				return true;
			}
		}

		return false;
	}

	private ArrayList<Object> getBottomRow(ArrayList<Object> row)
	{
		while ((Integer)row.get(5) != -1)
		{
			row = getRow((Integer)row.get(5));
		}

		return row;
	}

	private void getComplexColumns(SelectClause select, SubSelect sub, Having having) throws ParseException
	{
		ArrayList<SelectListEntry> selects = select.getSelectList();
		for (SelectListEntry s : selects)
		{
			if (!s.isColumn())
			{
				// complex column
				Expression exp = s.getExpression();
				OperatorTypeAndName op = buildOperatorTreeFromExpression(exp, s.getName(), sub);
				if (op.getType() == TYPE_DAYS)
				{
					throw new ParseException("A type of DAYS is not allowed for a column in a select list");
				}

				if (op.getType() == TYPE_MONTHS)
				{
					throw new ParseException("A type of MONTHS is not allowed for a column in a select list");
				}

				if (op.getType() == TYPE_YEARS)
				{
					throw new ParseException("A type of YEARS is not allowed for a column in a select list");
				}

				if (op.getType() == TYPE_DATE)
				{
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String dateString = sdf.format(((GregorianCalendar)op.getOp()).getTime());

					if (s.getName() != null)
					{
						String sgetName = s.getName();
						if (!sgetName.contains("."))
						{
							sgetName = "." + sgetName;
						}

						ExtendObjectOperator operator = new ExtendObjectOperator(DateParser.parse(dateString), sgetName, meta);
						ArrayList<Object> row = new ArrayList<Object>();
						row.add(sgetName);
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
					// colName, op, type, id, exp, prereq, sub, done
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

		if (having != null)
		{
			SearchCondition sc = having.getSearch();
			processHavingSC(sc, sub);
		}
	}

	private String getMatchingCol(Operator op, String col)
	{
		if (op.getCols2Pos().keySet().contains(col))
		{
			return col;
		}
		else
		{
			if (col.contains("."))
			{
				col = col.substring(col.indexOf('.') + 1);
			}

			for (String col2 : op.getCols2Pos().keySet())
			{
				String col3 = null;
				if (col2.contains("."))
				{
					col3 = col2.substring(col2.indexOf('.') + 1);
				}
				else
				{
					col3 = col2;
				}

				if (col.equals(col3))
				{
					return col2;
				}
			}

			HRDBMSWorker.logger.debug("Could not find " + col + " in " + op.getCols2Pos().keySet());
			Phase1.printTree(op, 0);
			HRDBMSWorker.logger.debug(complex);
			return col;
		}
	}

	private ArrayList<String> getReferences(Expression exp)
	{
		ArrayList<String> retval = new ArrayList<String>();
		if (exp.isCase())
		{
			for (Case c : exp.getCases())
			{
				retval.addAll(getReferences(c.getCondition()));
				retval.addAll(getReferences(c.getResult()));
			}

			return retval;
		}
		else if (exp.isColumn())
		{
			String name = "";
			Column c = exp.getColumn();
			if (c.getTable() != null)
			{
				name += c.getTable();
			}

			name += ("." + c.getColumn());
			retval.add(name);
			return retval;
		}
		else if (exp.isCountStar())
		{
			return retval;
		}
		else if (exp.isExpression())
		{
			retval.addAll(getReferences(exp.getLHS()));
			retval.addAll(getReferences(exp.getRHS()));
			return retval;
		}
		else if (exp.isFunction())
		{
			Function f = exp.getFunction();
			for (Expression e : f.getArgs())
			{
				retval.addAll(getReferences(e));
			}

			return retval;
		}
		else if (exp.isList())
		{
			ArrayList<Expression> list = exp.getList();
			for (Expression e : list)
			{
				retval.addAll(getReferences(e));
			}

			return retval;
		}
		else if (exp.isLiteral())
		{
			return retval;
		}
		else if (exp.isSelect())
		{
			retval.add("_________________________________");
			return retval;
		}

		return null;
	}

	private ArrayList<String> getReferences(SearchClause sc)
	{
		if (sc.getSearch() != null)
		{
			return getReferences(sc.getSearch());
		}
		else
		{
			Predicate p = sc.getPredicate();
			ArrayList<String> retval = new ArrayList<String>();
			retval.addAll(getReferences(p.getLHS()));
			retval.addAll(getReferences(p.getRHS()));
			return retval;
		}
	}

	private ArrayList<String> getReferences(SearchCondition sc)
	{
		ArrayList<String> retval = new ArrayList<String>();
		retval.addAll(getReferences(sc.getClause()));
		if (sc.getConnected() != null && sc.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : sc.getConnected())
			{
				retval.addAll(getReferences(csc.getSearch()));
			}
		}

		return retval;
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

	/*
	 * private String getOneCol(SubSelect sub) throws ParseException {
	 * SelectClause select = sub.getSelect(); if (select.isSelectStar()) { throw
	 * new ParseException(
	 * "SELECT * is not allowed in a subselect that must return 1 column"); }
	 * 
	 * ArrayList<SelectListEntry> list = select.getSelectList(); if (list.size()
	 * != 1) { throw new ParseException(
	 * "A subselect was used in a context where it must return 1 column, but instead returns a different number of columns"
	 * ); }
	 * 
	 * SelectListEntry entry = list.get(0); if (entry.isColumn()) { String
	 * retval = ""; Column col = entry.getColumn(); if (col.getTable() != null)
	 * { retval += col.getTable(); }
	 * 
	 * retval += ("." + col.getColumn()); return retval; }
	 * 
	 * String retval = entry.getName(); if (!retval.contains(".")) { retval =
	 * "." + retval; }
	 * 
	 * return retval; }
	 */

	private String getType(String col, HashMap<String, String> cols2Types) throws ParseException
	{
		String retval = cols2Types.get(col);

		if (retval != null)
		{
			return retval;
		}

		if (col.indexOf('.') > 0)
		{
			throw new ParseException("Column " + col + " not found");
		}

		if (col.startsWith("."))
		{
			col = col.substring(1);
		}

		int matches = 0;
		for (Map.Entry entry : cols2Types.entrySet())
		{
			String name2 = (String)entry.getKey();
			if (name2.contains("."))
			{
				name2 = name2.substring(name2.indexOf('.') + 1);
			}

			if (col.equals(name2))
			{
				matches++;
				retval = (String)entry.getValue();
			}
		}

		if (matches == 0)
		{
			throw new ParseException("Column " + col + " not found");
		}

		if (matches > 1)
		{
			throw new ParseException("Column " + col + " is ambiguous");
		}

		return retval;
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
		catch (Exception e)
		{
			throw new ParseException(e.getMessage());
		}
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

	private boolean isAllEquals(SearchCondition join)
	{
		if (!"E".equals(join.getClause().getPredicate().getOp()))
		{
			return false;
		}

		if (join.getConnected() != null && join.getConnected().size() > 0)
		{
			for (ConnectedSearchClause csc : join.getConnected())
			{
				if (!"E".equals(csc.getSearch().getPredicate().getOp()))
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
			// single predicate
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

		if (s.getConnected() != null && s.getConnected().size() > 0)
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

	private boolean isCorrelated(SubSelect select) throws Exception
	{
		try
		{
			buildOperatorTreeFromSubSelect(select);
		}
		catch (ParseException e)
		{
			String msg = e.getMessage();
			if (msg.startsWith("Column ") && msg.endsWith(" does not exist"))
			{
				// delete any entries in complex with sub=select
				int i = 0;
				while (i < complex.size())
				{
					ArrayList<Object> row = complex.get(i);
					if (select.equals(row.get(6)))
					{
						complex.remove(i);
						continue;
					}

					i++;
				}
				return true;
			}

			HRDBMSWorker.logger.debug("", e);
			throw new ParseException(e.getMessage());
		}

		// delete any entries in complex with sub=select
		int i = 0;
		while (i < complex.size())
		{
			ArrayList<Object> row = complex.get(i);
			if (select.equals(row.get(6)))
			{
				complex.remove(i);
				continue;
			}

			i++;
		}
		return false;
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

	private void processHavingExpression(Expression e, SubSelect sub) throws ParseException
	{
		if (e.isCountStar())
		{
			// see if complex has a row for this expression, if not add one
			boolean ok = false;
			for (ArrayList<Object> row : complex)
			{
				if (row.get(4).equals(e) && row.get(6).equals(sub))
				{
					ok = true;
					break;
				}
			}

			if (!ok)
			{
				// add it
				OperatorTypeAndName op = buildOperatorTreeFromExpression(e, null, sub);
				ArrayList<Object> row = new ArrayList<Object>();
				row.add(op.getName());
				row.add(op.getOp());
				row.add(op.getType());
				row.add(complexID++);
				row.add(e);
				row.add(op.getPrereq());
				row.add(sub);
				row.add(false);
				complex.add(row);
			}
		}
		else if (e.isExpression())
		{
			Expression e2 = e.getLHS();
			processHavingExpression(e2, sub);
			e2 = e.getRHS();
			processHavingExpression(e2, sub);
		}
		else if (e.isFunction())
		{
			Function f = e.getFunction();
			String name = f.getName();
			if (name.equals("AVG") || name.equals("SUM") || name.equals("COUNT") || name.equals("MAX") || name.equals("MIN"))
			{
				// see if complex has a row for this expression, if not add one
				boolean ok = false;
				for (ArrayList<Object> row : complex)
				{
					if (row.get(4).equals(e) && row.get(6).equals(sub))
					{
						ok = true;
						break;
					}
				}

				if (!ok)
				{
					// add it
					OperatorTypeAndName op = buildOperatorTreeFromExpression(e, null, sub);
					ArrayList<Object> row = new ArrayList<Object>();
					row.add(op.getName());
					row.add(op.getOp());
					row.add(op.getType());
					row.add(complexID++);
					row.add(e);
					row.add(op.getPrereq());
					row.add(sub);
					row.add(false);
					complex.add(row);
				}
			}
			else
			{
				ArrayList<Expression> args = f.getArgs();
				if (args != null && args.size() > 0)
				{
					for (Expression arg : args)
					{
						processHavingExpression(arg, sub);
					}
				}
			}
		}
		else if (e.isList())
		{
			ArrayList<Expression> list = e.getList();
			for (Expression e2 : list)
			{
				processHavingExpression(e2, sub);
			}
		}
		else if (e.isCase())
		{
			ArrayList<Case> cases = e.getCases();
			for (Case c : cases)
			{
				processHavingExpression(c.getResult(), sub);
				processHavingSC(c.getCondition(), sub);
			}

			processHavingExpression(e.getDefault(), sub);
		}
	}

	private void processHavingPredicate(Predicate p, SubSelect sub) throws ParseException
	{
		Expression l = p.getLHS();
		Expression r = p.getRHS();
		if (l != null)
		{
			processHavingExpression(l, sub);
		}

		if (r != null)
		{
			processHavingExpression(r, sub);
		}
	}

	private void processHavingSC(SearchCondition sc, SubSelect sub) throws ParseException
	{
		SearchClause search = sc.getClause();
		if (search.getPredicate() != null)
		{
			processHavingPredicate(search.getPredicate(), sub);
		}
		else
		{
			processHavingSC(search.getSearch(), sub);
		}

		if (sc.getConnected() != null && sc.getConnected().size() > 0)
		{
			for (ConnectedSearchClause search2 : sc.getConnected())
			{
				SearchClause search3 = search2.getSearch();
				if (search3.getPredicate() != null)
				{
					processHavingPredicate(search3.getPredicate(), sub);
				}
				else
				{
					processHavingSC(search3.getSearch(), sub);
				}
			}
		}
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

	private SearchCondition removeCorrelatedSearchCondition(SubSelect select, HashMap<String, Integer> cols2Pos) throws ParseException
	{
		SearchCondition search = select.getWhere().getSearch();
		convertToCNF(search);
		// for the clause and any connected
		ArrayList<ConnectedSearchClause> searches = new ArrayList<ConnectedSearchClause>();
		ArrayList<ConnectedSearchClause> cscs = new ArrayList<ConnectedSearchClause>();
		searches.add(new ConnectedSearchClause(search.getClause(), true));
		if (search.getConnected() != null && search.getConnected().size() > 0)
		{
			searches.addAll(search.getConnected());
		}

		for (ConnectedSearchClause csc : searches)
		{
			// if it's a predicate and contains a correlated column, add to
			// retval
			if (csc.getSearch().getPredicate() != null)
			{
				Predicate p = csc.getSearch().getPredicate();
				if (p.getLHS() != null && containsCorrelatedCol(p.getLHS(), cols2Pos))
				{
					if (p.getRHS() == null || !p.getRHS().isColumn())
					{
						throw new ParseException("Restriction: A correlated join condition in a correlated subquery can only refer to a column");
					}

					if (containsCorrelatedCol(p.getRHS(), cols2Pos))
					{
						throw new ParseException("A correlated predicate cannot have correlated columns on both sides");
					}

					cscs.add(csc);
				}
				else if (p.getRHS() != null && containsCorrelatedCol(p.getRHS(), cols2Pos))
				{
					if (p.getLHS() == null || !p.getLHS().isColumn())
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
				// if it's a search condition, every predicate in the ored group
				// must contain a correlated col
				// add whole group to retval
				boolean correlated = false;
				SearchCondition s = csc.getSearch().getSearch();
				Predicate p = s.getClause().getPredicate();
				if (p.getLHS() != null && containsCorrelatedCol(p.getLHS(), cols2Pos))
				{
					correlated = true;
				}
				else if (p.getRHS() != null && containsCorrelatedCol(p.getRHS(), cols2Pos))
				{
					correlated = true;
				}

				if (!correlated)
				{
					for (ConnectedSearchClause csc2 : s.getConnected())
					{
						p = csc2.getSearch().getPredicate();
						if (p.getLHS() != null && containsCorrelatedCol(p.getLHS(), cols2Pos))
						{
							correlated = true;
							break;
						}
						else if (p.getRHS() != null && containsCorrelatedCol(p.getRHS(), cols2Pos))
						{
							correlated = true;
							break;
						}
					}
				}

				if (correlated)
				{
					// Verify that every predicate is correlated
					p = s.getClause().getPredicate();
					if (p.getLHS() == null || p.getRHS() == null)
					{
						throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
					}
					if (!containsCorrelatedCol(p.getLHS(), cols2Pos) && !containsCorrelatedCol(p.getRHS(), cols2Pos))
					{
						throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
					}

					for (ConnectedSearchClause csc2 : s.getConnected())
					{
						p = csc2.getSearch().getPredicate();
						if (p.getLHS() == null || p.getRHS() == null)
						{
							throw new ParseException("Restriction: In a correlated subquery all predicates in a list of logically ORed predicates must either be local or correlated");
						}
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

						if (containsCorrelatedCol(p.getRHS(), cols2Pos))
						{
							throw new ParseException("A correlated predicate cannot have correlated columns on both sides");
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

							if (containsCorrelatedCol(p.getRHS(), cols2Pos))
							{
								throw new ParseException("A correlated predicate cannot have correlated columns on both sides");
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

		// update search and build retval
		for (ConnectedSearchClause csc : cscs)
		{
			searches.remove(csc);
		}

		if (searches.size() > 0)
		{
			SearchClause first = searches.remove(0).getSearch();
			search.setClause(first);
			search.setConnected(searches);
		}
		else
		{
			select.setWhere(null);
		}

		SearchClause first = cscs.remove(0).getSearch();
		return new SearchCondition(first, cscs);
	}

	private SearchCondition rewriteCorrelatedSubSelect(SubSelect select) throws Exception
	{
		// update select list and search conditions and group by
		FromClause from = select.getFrom();
		Operator temp = buildOperatorTreeFromFrom(from, select);
		SearchCondition retval = removeCorrelatedSearchCondition(select, temp.getCols2Pos());
		updateSelectList(retval, select);
		updateGroupBy(retval, select);
		retval = retval.clone();
		return retval;
	}

	private void searchFromForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, FromClause from)
	{
		for (TableReference table : from.getTables())
		{
			searchTableRefForCTE(name, cols, cteSelect, table);
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

	private void searchSingleTableForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, SingleTable table, TableReference tref)
	{
		TableName tblName = table.getName();
		if (tblName.getSchema() == null && tblName.getName().equals(name))
		{
			// found a match
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

	private void searchSubSelectForCTE(String name, ArrayList<Column> cols, FullSelect cteSelect, SubSelect select)
	{
		searchFromForCTE(name, cols, cteSelect, select.getFrom());
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

	private void updateGroupBy(SearchCondition join, SubSelect select) throws ParseException
	{
		if (updateGroupByNeeded(select))
		{
			if (!allPredicates(join) || !allAnd(join))
			{
				throw new ParseException("Restriction: A correlated subquery is not allowed if it involves aggregation and also contains correlated predicates that are not all anded together");
			}

			// verify all predicates are also equijoins
			if (!isAllEquals(join))
			{
				throw new ParseException("Restriction: Correlated subqueries with aggregation can only have equality operators on the correlated predicates");
			}

			// if group by already exists, add to it
			if (select.getGroupBy() != null)
			{
				GroupBy groupBy = select.getGroupBy();
				groupBy.getCols().addAll(getRightColumns(join));
			}
			else
			{
				// otherwise create it
				select.setGroupBy(new GroupBy(getRightColumns(join)));
			}
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
			boolean found = false;
			for (SelectListEntry entry : list)
			{
				// do we already have this col
				if (entry.getName() != null)
				{
					continue;
				}

				if (!entry.isColumn())
				{
					continue;
				}

				if (entry.getColumn().equals(col))
				{
					found = true;
				}
			}

			if (!found)
			{
				list.add(new SelectListEntry(col, null));
			}
		}
	}

	private void verifyColumnsAreTheSame(Operator lhs, Operator rhs) throws ParseException
	{
		TreeMap<Integer, String> lhsPos2Col = lhs.getPos2Col();
		TreeMap<Integer, String> rhsPos2Col = rhs.getPos2Col();

		if (lhsPos2Col.size() != rhsPos2Col.size())
		{
			throw new ParseException("Cannot combine table with different number of columns");
		}

		int i = 0;
		for (String col : lhsPos2Col.values())
		{
			if (!lhs.getCols2Types().get(col).equals(rhs.getCols2Types().get(rhsPos2Col.get(new Integer(i)))))
			{
				throw new ParseException("Column types do not match when combining tables");
			}

			i++;
		}
	}

	private void verifyTypes(String lhs, Operator lOp, String rhs, Operator rOp) throws ParseException
	{
		String lhsType = null;
		String rhsType = null;
		if (Character.isDigit(lhs.charAt(0)) || lhs.charAt(0) == '-')
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
				if (lhs.contains("."))
				{
					lhs = lhs.substring(lhs.indexOf(".") + 1);
				}
				else
				{
				}

				int matches = 0;
				for (String col : lOp.getCols2Types().keySet())
				{
					String col2;
					if (col.contains("."))
					{
						col2 = col.substring(col.indexOf('.') + 1);
					}
					else
					{
						col2 = col;
					}

					if (col2.equals(lhs))
					{
						type = lOp.getCols2Types().get(col);
						matches++;
					}
				}

				if (matches != 1)
				{
					if (matches == 0)
					{
						HRDBMSWorker.logger.debug("Cols2Types is " + lOp.getCols2Types());
						throw new ParseException("Column " + lhs + " does not exist");
					}
					else
					{
						throw new ParseException("Column " + lhs + " is ambiguous");
					}
				}
			}

			if (type.equals("CHAR"))
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

		if (Character.isDigit(rhs.charAt(0)) || rhs.charAt(0) == '-')
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
				if (rhs.contains("."))
				{
					rhs = rhs.substring(rhs.indexOf(".") + 1);
				}
				else
				{
				}

				int matches = 0;
				for (String col : rOp.getCols2Types().keySet())
				{
					String col2;
					if (col.contains("."))
					{
						col2 = col.substring(col.indexOf('.') + 1);
					}
					else
					{
						col2 = col;
					}

					if (col2.equals(rhs))
					{
						type = rOp.getCols2Types().get(col);
						matches++;
					}
				}

				if (matches != 1)
				{
					if (matches == 0)
					{
						throw new ParseException("Column " + rhs + " does not exist");
					}
					else
					{
						throw new ParseException("Column " + rhs + " is ambiguous");
					}
				}
			}

			if (type.equals("CHAR"))
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
		if (Character.isDigit(lhs.charAt(0)) || lhs.charAt(0) == '-')
		{
			lhsType = "NUMBER";
		}
		else if (lhs.charAt(0) == '\'')
		{
			lhsType = "STRING";
		}
		else if (lhs.startsWith("DATE('"))
		{
			lhsType = "DATE";
		}
		else
		{
			String type = o.getCols2Types().get(lhs);
			if (type == null)
			{
				throw new ParseException("Column " + lhs + " does not exist");
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

		if (Character.isDigit(rhs.charAt(0)) || rhs.charAt(0) == '-')
		{
			rhsType = "NUMBER";
		}
		else if (rhs.charAt(0) == '\'')
		{
			rhsType = "STRING";
		}
		else if (rhs.startsWith("DATE('"))
		{
			rhsType = "DATE";
		}
		else
		{
			String type = o.getCols2Types().get(rhs);
			if (type == null)
			{
				HRDBMSWorker.logger.debug("Looking for " + rhs + " in " + o.getCols2Types());
				printTree(o, 0);
				throw new ParseException("Column " + rhs + " does not exist");
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
			if (op.equals("LI") || op.equals("NL") || op.equals("E") || op.equals("NE") || op.equals("G") || op.equals("GE") || op.equals("L") || op.equals("LE"))
			{
				return;
			}

			throw new ParseException("Invalid operator for comparing 2 string: " + op);
		}
		else if (lhsType.equals("DATE") && rhsType.equals("DATE"))
		{
			if (op.equals("E") || op.equals("NE") || op.equals("G") || op.equals("GE") || op.equals("L") || op.equals("LE"))
			{
				return;
			}

			throw new ParseException("Invalid operator for comparing 2 dates: " + op);
		}
		else
		{
			throw new ParseException("Invalid comparison between " + lhsType + " and " + rhsType);
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

		public int getPrereq()
		{
			return prereq;
		}

		public int getType()
		{
			return type;
		}
	}
}
