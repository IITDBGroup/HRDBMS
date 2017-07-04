package com.exascale.optimizer.parse;

import java.util.*;

import com.exascale.optimizer.*;
import com.exascale.optimizer.externalTable.*;
import com.exascale.optimizer.load.Load;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import com.exascale.exceptions.ParseException;
import com.exascale.tables.SQL;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

/** Parse commands sent by HRDBMS CLI into a plan for the database to run. */
public class SQLParser extends AbstractParseController
{
	public static final int TYPE_INLINE = 0;
	public static final int TYPE_GROUPBY = 1;
	public static final int TYPE_DATE = 2;
	public static final int TYPE_DAYS = 3;
	public static final int TYPE_MONTHS = 4;
	public static final int TYPE_YEARS = 5;
	private SQL sql;
	private final MetaData meta = new MetaData();
	private boolean authorized = false;

	public SQLParser() { super(null, null, new MetaData(), new Model()); }

	public SQLParser(final SQL sql, final ConnectionWorker connection, final Transaction tx)
	{
		super(connection, tx, new MetaData(), new Model());
		this.sql = sql;
	}

	public SQLParser(final String sql, final ConnectionWorker connection, final Transaction tx)
	{
		super(connection, tx, new MetaData(), new Model());
		this.sql = new SQL(sql);
	}

	public void authorize()
	{
		authorized = true;
	}

	public boolean doesNotUseCurrentSchema()
	{
		return model.doesNotUseCurrentSchema;
	}

	/** Main point of entry */
	public ArrayList<Operator> parse() throws Exception
	{
		final ANTLRInputStream input = new ANTLRInputStream(sql.toString());
		final SelectLexer lexer = new SelectLexer(input);
		final CommonTokenStream tokens = new CommonTokenStream(lexer);
		final SelectParser parser = new SelectParser(tokens);
		parser.setErrorHandler(new BailErrorStrategy());
		final ParseTree tree = parser.select();
		final SelectVisitorImpl visitor = new SelectVisitorImpl();
		final SQLStatement stmt = (SQLStatement)visitor.visit(tree);

		OperatorTrees operatorTrees = new OperatorTrees(connection, tx, meta, model);
		if (stmt instanceof Select)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromSelect((Select)stmt);
			final RootOperator retval = new RootOperator(meta.generateCard(op, tx, op), new MetaData());
			retval.add(op);
			// Utils.printTree(op, 0); // DEBUG
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(retval);
			return ops;
		}

		if (stmt instanceof Insert)
		{
			final Insert insert = (Insert)stmt;
			final TableName table = insert.getTable();
			if (!authorized && table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("Catalog updates are not allowed");
			}
			final ArrayList<Operator> op = operatorTrees.buildOperatorTreeFromInsert((Insert)stmt);
			return op;
		}

		if (stmt instanceof Update)
		{
			final Update update = (Update)stmt;
			final TableName table = update.getTable();
			if (!authorized && table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("Catalog updates are not allowed");
			}
			final ArrayList<Operator> ops = operatorTrees.buildOperatorTreeFromUpdate((Update)stmt);
			return ops;
		}

		if (stmt instanceof Delete)
		{
			final Delete delete = (Delete)stmt;
			final TableName table = delete.getTable();
			if (!authorized && table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("Catalog updates are not allowed");
			}
			final Operator op = operatorTrees.buildOperatorTreeFromDelete((Delete)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof Runstats)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromRunstats((Runstats)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof CreateTable)
		{
			final CreateTable createTable = (CreateTable)stmt;
			final TableName table = createTable.getTable();
			if (table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("You cannot create new tables in the SYS schema");  // Reserved for internal use
			}
			final Operator op = operatorTrees.buildOperatorTreeFromCreateTable((CreateTable)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof CreateExternalTable)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromCreateExternalTable((CreateExternalTable)stmt);
			return new ArrayList<>(Arrays.asList(op));
		}

		if (stmt instanceof DropTable)
		{
			final DropTable dropTable = (DropTable)stmt;
			final TableName table = dropTable.getTable();
			if (table.getSchema() != null && table.getSchema().equals("SYS"))
			{
				throw new ParseException("You cannot drop tables in the SYS schema");
			}
			final Operator op = operatorTrees.buildOperatorTreeFromDropTable((DropTable)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof CreateIndex)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromCreateIndex((CreateIndex)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof DropIndex)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromDropIndex((DropIndex)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof CreateView)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromCreateView((CreateView)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof DropView)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromDropView((DropView)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		if (stmt instanceof Load)
		{
			final Operator op = operatorTrees.buildOperatorTreeFromLoad((Load)stmt);
			final ArrayList<Operator> ops = new ArrayList<Operator>(1);
			ops.add(op);
			return ops;
		}

		return null;
	}

	public static final class OperatorTypeAndName
	{
		Object op;
		int type;
		String name;
		int prereq;

		public OperatorTypeAndName(final Object op, final int type, final String name, final int prereq)
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

	public static class Model {
		private int complexID = 0;
		private final ArrayList<ArrayList<Object>> complex = new ArrayList<ArrayList<Object>>();
		private int rewriteCounter = 0;
		private int suffix = 0;
		private boolean doesNotUseCurrentSchema = true;

		public ArrayList<ArrayList<Object>> getComplex() { return complex; }
		public int getRewrites() { return rewriteCounter; }
		public int getAndIncrementRewrites() { return rewriteCounter++; }
		public int getAndIncrementSuffix() { return suffix++; }
		public void useCurrentSchema() { doesNotUseCurrentSchema = false; }
		public int getAndIncrementComplexId() { return complexID++; }
	}
}