package com.exascale.optimizer.parse;

import java.util.ArrayList;
import java.util.List;

import com.exascale.optimizer.*;
import com.exascale.optimizer.externalTable.*;
import com.exascale.optimizer.load.Load;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import com.exascale.misc.Utils;

public class SelectVisitorImpl extends SelectBaseVisitor<Object>
{
	@Override
	public Expression visitAddSub(final SelectParser.AddSubContext ctx)
	{
		return new Expression((Expression)visit(ctx.expression(0)), ctx.op.getText(), (Expression)visit(ctx.expression(1)));
	}

	@Override
	public Case visitCaseCase(final SelectParser.CaseCaseContext ctx)
	{
		return new Case((SearchCondition)visit(ctx.searchCondition()), (Expression)visit(ctx.expression()));
	}

	@Override
	public Expression visitCaseExp(final SelectParser.CaseExpContext ctx)
	{
		final List<Case> cases = new ArrayList<Case>(ctx.caseCase().size());
		for (final SelectParser.CaseCaseContext context : ctx.caseCase())
		{
			cases.add((Case)visit(context));
		}
		return new Expression(cases, (Expression)visit(ctx.expression()));
	}

	@Override
	public Column visitCol1Part(final SelectParser.Col1PartContext ctx)
	{
		return new Column(ctx.IDENTIFIER().getText());
	}

	@Override
	public Column visitCol2Part(final SelectParser.Col2PartContext ctx)
	{
		return new Column(ctx.IDENTIFIER(0).getText(), ctx.IDENTIFIER(1).getText());
	}

	@Override
	public ColDef visitColDef(final SelectParser.ColDefContext ctx)
	{
		final Column col = (Column)visit(ctx.columnName());
		final String type = (String)visit(ctx.dataType());
		boolean nullable = true;
		boolean pk = false;
		if (ctx.notNull() != null)
		{
			nullable = false;
		}
		if (ctx.primary() != null)
		{
			pk = true;
		}

		return new ColDef(col, type, nullable, pk);
	}

	@Override
	public List<Column> visitColList(final SelectParser.ColListContext ctx)
	{
		final List<Column> cols = new ArrayList<Column>();
		for (final SelectParser.ColumnNameContext context : ctx.columnName())
		{
			cols.add((Column)visit(context));
		}

		return cols;
	}

	@Override
	public Expression visitColLiteral(final SelectParser.ColLiteralContext ctx)
	{
		return new Expression((Column)visit(ctx.columnName()));
	}

	@Override
	public List<Integer> visitColOrder(final SelectParser.ColOrderContext ctx)
	{
		final List<Integer> colOrder = new ArrayList<Integer>();
		for (final TerminalNode node : ctx.INTEGER())
		{
			colOrder.add(Integer.parseInt(node.getText().trim()));
		}

		return colOrder;
	}

	@Override
	public CTE visitCommonTableExpression(final SelectParser.CommonTableExpressionContext ctx)
	{
		final String tableName = ctx.IDENTIFIER().getText();
		final List<Column> cols = new ArrayList<Column>();

		if (ctx.columnName() != null)
		{
			for (final SelectParser.ColumnNameContext context : ctx.columnName())
			{
				cols.add((Column)visit(context));
			}
		}

		final FullSelect select = (FullSelect)visit(ctx.fullSelect());
		return new CTE(tableName, cols, select);
	}

	@Override
	public Expression visitConcat(final SelectParser.ConcatContext ctx)
	{
		return new Expression((Expression)visit(ctx.expression(0)), "||", (Expression)visit(ctx.expression(1)));
	}

	@Override
	public ConnectedSearchClause visitConnectedSearchClause(final SelectParser.ConnectedSearchClauseContext ctx)
	{
		final boolean and = (ctx.AND() != null);
		return new ConnectedSearchClause((SearchClause)visit(ctx.searchClause()), and);
	}

	@Override
	public ConnectedSelect visitConnectedSelect(final SelectParser.ConnectedSelectContext ctx)
	{
		final String combo = ctx.TABLECOMBINATION().getText();
		if (ctx.subSelect() != null)
		{
			return new ConnectedSelect((SubSelect)visit(ctx.subSelect()), combo);
		}
		else
		{
			return new ConnectedSelect((FullSelect)visit(ctx.fullSelect()), combo);
		}
	}

	@Override
	public String visitCorrelationClause(final SelectParser.CorrelationClauseContext ctx)
	{
		return ctx.IDENTIFIER().getText();
	}

	@Override
	public Expression visitCountDistinct(final SelectParser.CountDistinctContext ctx)
	{
		final List<Expression> arguments = new ArrayList<Expression>(1);
		arguments.add((Expression)visit(ctx.expression()));
		return new Expression(new Function("COUNT", arguments, true));
	}

	@Override
	public Expression visitCountStar(final SelectParser.CountStarContext ctx)
	{
		return new Expression();
	}

	@Override
	public CreateIndex visitCreateIndex(final SelectParser.CreateIndexContext ctx)
	{
		final TableName index = (TableName)visit(ctx.tableName(0));
		final TableName table = (TableName)visit(ctx.tableName(1));
		final List<IndexDef> defs = new ArrayList<IndexDef>();
		for (final SelectParser.IndexDefContext context : ctx.indexDef())
		{
			defs.add((IndexDef)visit(context));
		}

		boolean unique = false;
		if (ctx.UNIQUE() != null)
		{
			unique = true;
		}
		return new CreateIndex(index, table, defs, unique);
	}

	@Override
	public CreateTable visitCreateTable(final SelectParser.CreateTableContext ctx)
	{
		final TableName table = (TableName)visit(ctx.tableName());
		final List<ColDef> cols = new ArrayList<ColDef>();
		for (final SelectParser.ColDefContext context : ctx.colDef())
		{
			cols.add((ColDef)visit(context));
		}
		PrimaryKey pk = null;
		if (ctx.primaryKey() != null)
		{
			pk = (PrimaryKey)visit(ctx.primaryKey());
		}

		String nodeGroupExp = "NONE";
		String nodeExp = "";
		String deviceExp = "";

		if (ctx.groupExp() != null)
		{
			nodeGroupExp = ctx.groupExp().getText();
		}

		nodeExp = ctx.nodeExp().getText();
		deviceExp = ctx.deviceExp().getText();

		int type = 0;
		if (ctx.COLUMN() != null)
		{
			type = 1;
		}

		List<Integer> colOrder = null;
		if (ctx.colOrder() != null)
		{
			colOrder = (List<Integer>)visit(ctx.colOrder());
		}

		CreateTable retval = null;
		if (colOrder == null)
		{
			retval = new CreateTable(table, cols, pk, nodeGroupExp, nodeExp, deviceExp, type);
		}
		else
		{
			retval = new CreateTable(table, cols, pk, nodeGroupExp, nodeExp, deviceExp, type, colOrder);
		}

		if (ctx.organization() != null)
		{
			final List<Integer> organization = (List<Integer>)visit(ctx.organization());
			retval.setOrganization(organization);
		}

		return retval;
	}

	public CreateExternalTable visitCreateExternalTable(SelectParser.CreateExternalTableContext ctx)
	{
		TableName table = (TableName)visit(ctx.tableName());
		ArrayList<ColDef> cols = new ArrayList<ColDef>();
		for (SelectParser.ColDefContext context : ctx.colDef())
		{
			cols.add((ColDef)visit(context));
		}
		
		if(ctx.generalExtTableSpec() != null)
		{
			GeneralExtTableSpec generalExtTableSpec =
					(GeneralExtTableSpec)visit(ctx.generalExtTableSpec());
			return new CreateExternalTable(table, cols, generalExtTableSpec);
		}		
		else
		{
			String javaClassName = ctx.javaClassExtTableSpec().javaClassName().getText();
			String params = ctx.javaClassExtTableSpec().json().getText();
			JSONUtils.validate(javaClassName, params);
			JavaClassExtTableSpec javaClassExtTableSpec = new JavaClassExtTableSpec(javaClassName, params);
			if (javaClassName.equals(HDFSCsvExternal.class.getCanonicalName())) {
				cols.add(new ColDef(new Column("_HDFS_BLOCK_ID"), "INT", false, false));
			}
			return new CreateExternalTable(table, cols, javaClassExtTableSpec);
		}
	}

	@Override
	public CreateView visitCreateView(final SelectParser.CreateViewContext ctx)
	{
		final TableName view = (TableName)visit(ctx.tableName());
		final FullSelect select = (FullSelect)visit(ctx.fullSelect());
		final int a = ctx.fullSelect().start.getStartIndex();
		final int b = ctx.fullSelect().stop.getStopIndex();
		final Interval interval = new Interval(a, b);
		final CharStream input = ctx.fullSelect().start.getInputStream();
		final String text = input.getText(interval);
		return new CreateView(view, select, text);
	}

	@Override
	public TableReference visitCrossJoin(final SelectParser.CrossJoinContext ctx)
	{
		final TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		final TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		String alias = null;
		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, "CP", rhs, null, alias);
	}

	@Override
	public TableReference visitCrossJoinP(final SelectParser.CrossJoinPContext ctx)
	{
		final TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		final TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		String alias = null;

		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, "CP", rhs, null, alias);
	}

	@Override
	public String visitDataType(final SelectParser.DataTypeContext ctx)
	{
		if (ctx.int2() != null)
		{
			return "INT";
		}

		if (ctx.long2() != null)
		{
			return "LONG";
		}

		if (ctx.float2() != null)
		{
			return "FLOAT";
		}

		if (ctx.date2() != null)
		{
			return "DATE";
		}

		if (ctx.char2() != null)
		{
			return "CHAR(" + Utils.parseLong(ctx.char2().INTEGER().getText()) + ")";
		}

		return null;
	}

	@Override
	public Delete visitDelete(final SelectParser.DeleteContext ctx)
	{
		final TableName table = (TableName)visit(ctx.tableName());
		Where where = null;
		if (ctx.whereClause() != null)
		{
			where = (Where)visit(ctx.whereClause());
		}

		return new Delete(table, where);
	}

	@Override
	public DropIndex visitDropIndex(final SelectParser.DropIndexContext ctx)
	{
		return new DropIndex((TableName)visit(ctx.tableName()));
	}

	@Override
	public DropTable visitDropTable(final SelectParser.DropTableContext ctx)
	{
		return new DropTable((TableName)visit(ctx.tableName()));
	}

	@Override
	public DropView visitDropView(final SelectParser.DropViewContext ctx)
	{
		return new DropView((TableName)visit(ctx.tableName()));
	}

	@Override
	public ExistsPredicate visitExistsPredicate(final SelectParser.ExistsPredicateContext ctx)
	{
		return new ExistsPredicate((SubSelect)visit(ctx.subSelect()));
	}

	@Override
	public Expression visitExpSelect(final SelectParser.ExpSelectContext ctx)
	{
		return new Expression((SubSelect)visit(ctx.subSelect()));
	}

	@Override
	public FetchFirst visitFetchFirst(final SelectParser.FetchFirstContext ctx)
	{
		long num = 1;
		if (ctx.INTEGER() != null)
		{
			num = Utils.parseLong(ctx.INTEGER().getText());
		}

		return new FetchFirst(num);
	}

	@Override
	public FromClause visitFromClause(final SelectParser.FromClauseContext ctx)
	{
		final List<TableReference> tables = new ArrayList<TableReference>(ctx.tableReference().size());
		for (final SelectParser.TableReferenceContext context : ctx.tableReference())
		{
			tables.add((TableReference)visit(context));
		}

		return new FromClause(tables);
	}

	@Override
	public FullSelect visitFullSelect(final SelectParser.FullSelectContext ctx)
	{
		// (subSelect | '(' fullSelect ')') (connectedSelect)* (orderBy)?
		// (fetchFirst)? ;
		SubSelect sub = null;
		FullSelect full = null;
		final List<ConnectedSelect> connected = new ArrayList<ConnectedSelect>();
		OrderBy orderBy = null;
		FetchFirst fetchFirst = null;

		if (ctx.subSelect() != null)
		{
			sub = (SubSelect)visit(ctx.subSelect());
		}
		else
		{
			full = (FullSelect)visit(ctx.fullSelect());
		}

		if (ctx.connectedSelect() != null)
		{
			for (final SelectParser.ConnectedSelectContext context : ctx.connectedSelect())
			{
				connected.add((ConnectedSelect)visit(context));
			}
		}

		if (ctx.orderBy() != null)
		{
			orderBy = (OrderBy)visit(ctx.orderBy());
		}

		if (ctx.fetchFirst() != null)
		{
			fetchFirst = (FetchFirst)visit(ctx.fetchFirst());
		}

		return new FullSelect(sub, full, connected, orderBy, fetchFirst);
	}

	@Override
	public Expression visitFunction(final SelectParser.FunctionContext ctx)
	{
		final List<Expression> arguments = new ArrayList<Expression>(ctx.expression().size());
		for (final SelectParser.ExpressionContext context : ctx.expression())
		{
			arguments.add((Expression)visit(context));
		}
		return new Expression(new Function(ctx.identifier().getText(), arguments));
	}

	@Override
	public GroupBy visitGroupBy(final SelectParser.GroupByContext ctx)
	{
		final List<Column> cols = new ArrayList<Column>(ctx.columnName().size());
		for (final SelectParser.ColumnNameContext context : ctx.columnName())
		{
			cols.add((Column)visit(context));
		}

		return new GroupBy(cols);
	}

	@Override
	public Having visitHavingClause(final SelectParser.HavingClauseContext ctx)
	{
		return new Having((SearchCondition)visit(ctx.searchCondition()));
	}

	public IndexDef visitIndexDef(final SelectParser.IndexDefContext ctx)
	{
		final Column col = (Column)visit(ctx.columnName());
		boolean dir = true;
		if (ctx.DIRECTION() != null)
		{
			if (ctx.DIRECTION().getText().equals("DESC"))
			{
				dir = false;
			}
		}

		return new IndexDef(col, dir);
	}

	@Override
	public Insert visitInsert(final SelectParser.InsertContext ctx)
	{
		if (ctx.fullSelect() != null)
		{
			return new Insert((TableName)visit(ctx.tableName()), (FullSelect)visit(ctx.fullSelect()));
		}

		if (ctx.valuesList().size() == 1)
		{
			final List<Expression> exps = new ArrayList<Expression>();
			for (final SelectParser.ExpressionContext exp : ctx.valuesList().get(0).expression())
			{
				exps.add((Expression)visit(exp));
			}
			return new Insert((TableName)visit(ctx.tableName()), exps);
		}
		else
		{
			final List<List<Expression>> mExps = new ArrayList<List<Expression>>();
			for (final SelectParser.ValuesListContext ctx2 : ctx.valuesList())
			{
				final List<Expression> exps = new ArrayList<Expression>();
				for (final SelectParser.ExpressionContext exp : ctx2.expression())
				{
					exps.add((Expression)visit(exp));
				}

				mExps.add(exps);
			}

			return new Insert((TableName)visit(ctx.tableName()), mExps, true);
		}
	}

	@Override
	public Expression visitIsLiteral(final SelectParser.IsLiteralContext ctx)
	{
		return new Expression((Literal)visit(ctx.literal()));
	}

	@Override
	public TableReference visitIsSingleTable(final SelectParser.IsSingleTableContext ctx)
	{
		return new TableReference((SingleTable)visit(ctx.singleTable()));
	}

	@Override
	public TableReference visitJoin(final SelectParser.JoinContext ctx)
	{
		String op = ctx.JOINTYPE().getText();
		if (op.equals("INNER"))
		{
			op = "I";
		}
		else if (op.equals("LEFT"))
		{
			op = "L";
		}
		else if (op.equals("RIGHT"))
		{
			op = "R";
		}
		else if (op.equals("FULL"))
		{
			op = "F";
		}
		else if (op.equals("LEFT OUTER"))
		{
			op = "L";
		}
		else if (op.equals("RIGHT OUTER"))
		{
			op = "R";
		}
		else if (op.equals("FULL OUTER"))
		{
			op = "F";
		}

		final TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		final TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		final SearchCondition search = (SearchCondition)visit(ctx.searchCondition());
		String alias = null;
		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, op, rhs, search, alias);
	}

	@Override
	public TableReference visitJoinP(final SelectParser.JoinPContext ctx)
	{
		String op = ctx.JOINTYPE().getText();
		if (op.equals("INNER"))
		{
			op = "I";
		}
		else if (op.equals("LEFT"))
		{
			op = "L";
		}
		else if (op.equals("RIGHT"))
		{
			op = "R";
		}
		else if (op.equals("FULL"))
		{
			op = "F";
		}
		else if (op.equals("LEFT OUTER"))
		{
			op = "L";
		}
		else if (op.equals("RIGHT OUTER"))
		{
			op = "R";
		}
		else if (op.equals("FULL OUTER"))
		{
			op = "F";
		}

		final TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		final TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		final SearchCondition search = (SearchCondition)visit(ctx.searchCondition());
		String alias = null;
		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, op, rhs, search, alias);
	}

	@Override
	public Expression visitList(final SelectParser.ListContext ctx)
	{
		final List<Expression> exps = new ArrayList<Expression>(ctx.expression().size());
		for (final SelectParser.ExpressionContext context : ctx.expression())
		{
			exps.add((Expression)visit(context));
		}

		return new Expression(exps);
	}

	@Override
	public Load visitLoad(final SelectParser.LoadContext ctx)
	{
		boolean replace = true;
		if (ctx.RESUME() != null)
		{
			replace = false;
		}

		String delimited = "|";
		if (ctx.any() != null)
		{
			delimited = ctx.any().getText();
		}

		final TableName table = (TableName)visit(ctx.tableName());
		String glob = null;
		if(ctx.remainder() != null) {
			glob = ctx.remainder().getText();
			final int first = glob.indexOf('\'');
			final int second = glob.indexOf('\'', first + 1);
			glob = glob.substring(first + 1, second);
		}

		TableName extTable = null;
		if(ctx.externalTableName() != null) {
			extTable = (TableName) visit(ctx.externalTableName().tableName());
		}

		return new Load(table, replace, delimited, glob, extTable);
	}

	@Override
	public Expression visitMulDiv(final SelectParser.MulDivContext ctx)
	{
		return new Expression((Expression)visit(ctx.expression(0)), ctx.op.getText(), (Expression)visit(ctx.expression(1)));
	}

	@Override
	public TableReference visitNestedTable(final SelectParser.NestedTableContext ctx)
	{
		final FullSelect fs = (FullSelect)visit(ctx.fullSelect());
		if (ctx.correlationClause() != null)
		{
			return new TableReference(fs, (String)visit(ctx.correlationClause()));
		}
		else
		{
			return new TableReference(fs);
		}
	}

	@Override
	public Predicate visitNormalPredicate(final SelectParser.NormalPredicateContext ctx)
	{
		String op = ctx.operator().getText();
		if (op.equals("="))
		{
			op = "E";
		}
		else if (op.equals("<>"))
		{
			op = "NE";
		}
		else if (op.equals("!="))
		{
			op = "NE";
		}
		else if (op.equals("<="))
		{
			op = "LE";
		}
		else if (op.equals("<"))
		{
			op = "L";
		}
		else if (op.equals(">="))
		{
			op = "GE";
		}
		else if (op.equals(">"))
		{
			op = "G";
		}
		else if (op.equals("EQUALS"))
		{
			op = "E";
		}
		else if (op.equals("NOT EQUALS"))
		{
			op = "NE";
		}
		else if (op.equals("LIKE"))
		{
			op = "LI";
		}
		else if (op.equals("NOT LIKE"))
		{
			op = "NL";
		}
		else if (op.equals("IN"))
		{
			op = "IN";
		}
		else if (op.equals("NOT IN"))
		{
			op = "NI";
		}

		return new Predicate((Expression)visit(ctx.expression(0)), op, (Expression)visit(ctx.expression(1)));
	}

	@Override
	public Expression visitNullExp(final SelectParser.NullExpContext ctx)
	{
		return new Expression(new Literal(null));
	}

	@Override
	public Predicate visitNullPredicate(final SelectParser.NullPredicateContext ctx)
	{
		String op;
		if (ctx.NULLOPERATOR().getText().equals("IS NULL"))
		{
			op = "E";
		}
		else
		{
			op = "NE";
		}

		return new Predicate((Expression)visit(ctx.expression()), op, new Expression(new Literal()));
	}

	@Override
	public Literal visitNumericLiteral(final SelectParser.NumericLiteralContext ctx)
	{
		final String number = ctx.getText();
		if (number.indexOf('.') != -1)
		{
			return new Literal(Double.parseDouble(number));
		}
		else
		{
			return new Literal(Utils.parseLong(number));
		}
	}

	@Override
	public OrderBy visitOrderBy(final SelectParser.OrderByContext ctx)
	{
		final List<SortKey> keys = new ArrayList<SortKey>(ctx.sortKey().size());
		for (final SelectParser.SortKeyContext context : ctx.sortKey())
		{
			keys.add((SortKey)visit(context));
		}

		return new OrderBy(keys);
	}

	@Override
	public List<Integer> visitOrganization(final SelectParser.OrganizationContext ctx)
	{
		final List<Integer> organization = new ArrayList<Integer>();
		for (final TerminalNode node : ctx.INTEGER())
		{
			organization.add(Integer.parseInt(node.getText().trim()));
		}

		return organization;
	}

	@Override
	public Expression visitPExpression(final SelectParser.PExpressionContext ctx)
	{
		return (Expression)visit(ctx.expression());
	}

	@Override
	public PrimaryKey visitPrimaryKey(final SelectParser.PrimaryKeyContext ctx)
	{
		final List<Column> cols = new ArrayList<Column>();
		for (final SelectParser.ColumnNameContext context : ctx.columnName())
		{
			cols.add((Column)visit(context));
		}

		return new PrimaryKey(cols);
	}

	@Override
	public Runstats visitRunstats(final SelectParser.RunstatsContext ctx)
	{
		return new Runstats((TableName)visit(ctx.tableName()));
	}

	@Override
	public SearchClause visitSearchClause(final SelectParser.SearchClauseContext ctx)
	{
		boolean negated = false;
		Predicate predicate;
		SearchCondition condition;
		if (ctx.NOT() != null)
		{
			negated = true;
		}

		if (ctx.predicate() != null)
		{
			predicate = (Predicate)visit(ctx.predicate());
			return new SearchClause(predicate, negated);
		}
		else
		{
			condition = (SearchCondition)visit(ctx.searchCondition());
			return new SearchClause(condition, negated);
		}
	}

	@Override
	public SearchCondition visitSearchCondition(final SelectParser.SearchConditionContext ctx)
	{
		final SearchClause search = (SearchClause)visit(ctx.searchClause());
		final List<ConnectedSearchClause> connected = new ArrayList<ConnectedSearchClause>();
		if (ctx.connectedSearchClause() != null)
		{
			for (final SelectParser.ConnectedSearchClauseContext context : ctx.connectedSearchClause())
			{
				connected.add((ConnectedSearchClause)visit(context));
			}
		}

		return new SearchCondition(search, connected);
	}

	@Override
	public SQLStatement visitSelect(final SelectParser.SelectContext ctx)
	{
		if (ctx.insert() != null)
		{
			return (Insert)visit(ctx.insert());
		}
		else if (ctx.update() != null)
		{
			return (Update)visit(ctx.update());
		}
		else if (ctx.delete() != null)
		{
			return (Delete)visit(ctx.delete());
		}
		else if (ctx.createIndex() != null)
		{
			return (CreateIndex)visit(ctx.createIndex());
		}
        else if (ctx.createExternalTable() != null)
        {
            return (CreateExternalTable)visit(ctx.createExternalTable());
        }
        else if (ctx.createTable() != null)
		{
			return (CreateTable)visit(ctx.createTable());
		}
		else if (ctx.createView() != null)
		{
			return (CreateView)visit(ctx.createView());
		}
		else if (ctx.dropIndex() != null)
		{
			return (DropIndex)visit(ctx.dropIndex());
		}
		else if (ctx.dropTable() != null)
		{
			return (DropTable)visit(ctx.dropTable());
		}
		else if (ctx.dropView() != null)
		{
			return (DropView)visit(ctx.dropView());
		}
		else if (ctx.load() != null)
		{
			return (Load)visit(ctx.load());
		}
		else if (ctx.runstats() != null)
		{
			return (Runstats)visit(ctx.runstats());
		}

		final List<CTE> ctes = new ArrayList<CTE>();

		if (ctx.commonTableExpression() != null)
		{
			for (final SelectParser.CommonTableExpressionContext context : ctx.commonTableExpression())
			{
				ctes.add((CTE)visit(context));
			}
		}

		final FullSelect select = (FullSelect)visit(ctx.fullSelect());
		return new Select(ctes, select);
	}

	@Override
	public SelectClause visitSelectClause(final SelectParser.SelectClauseContext ctx)
	{
		boolean selectAll = true;
		boolean selectStar = false;
		final List<SelectListEntry> selectList = new ArrayList<SelectListEntry>();
		if (ctx.selecthow() != null)
		{
			if (!ctx.selecthow().getText().equals("ALL"))
			{
				selectAll = false;
			}
		}

		if (ctx.STAR() != null)
		{
			selectStar = true;
		}
		else
		{
			for (final SelectParser.SelectListEntryContext context : ctx.selectListEntry())
			{
				selectList.add((SelectListEntry)visit(context));
			}
		}

		return new SelectClause(selectAll, selectStar, selectList);
	}

	@Override
	public SelectListEntry visitSelectColumn(final SelectParser.SelectColumnContext ctx)
	{
		final Column col = (Column)visit(ctx.columnName());
		String alias = null;
		if (ctx.IDENTIFIER() != null)
		{
			alias = ctx.IDENTIFIER().getText();
		}
		return new SelectListEntry(col, alias);
	}

	@Override
	public SelectListEntry visitSelectExpression(final SelectParser.SelectExpressionContext ctx)
	{
		String alias = null;
		if (ctx.IDENTIFIER() != null)
		{
			alias = ctx.IDENTIFIER().getText();
		}
		return new SelectListEntry((Expression)visit(ctx.expression()), alias);
	}

	@Override
	public SingleTable visitSingleTable(final SelectParser.SingleTableContext ctx)
	{
		if (ctx.correlationClause() != null)
		{
			return new SingleTable((TableName)visit(ctx.tableName()), (String)visit(ctx.correlationClause()));
		}
		else
		{
			return new SingleTable((TableName)visit(ctx.tableName()));
		}
	}

	@Override
	public SortKey visitSortKeyCol(final SelectParser.SortKeyColContext ctx)
	{
		boolean direction = true;
		if (ctx.DIRECTION() != null)
		{
			if (!ctx.DIRECTION().getText().equals("ASC"))
			{
				direction = false;
			}
		}
		return new SortKey((Column)visit(ctx.columnName()), direction);
	}

	@Override
	public SortKey visitSortKeyInt(final SelectParser.SortKeyIntContext ctx)
	{
		boolean direction = true;
		if (ctx.DIRECTION() != null)
		{
			if (!ctx.DIRECTION().getText().equals("ASC"))
			{
				direction = false;
			}
		}
		return new SortKey(Utils.parseInt(ctx.INTEGER().getText()), direction);
	}

	@Override
	public Literal visitStringLiteral(final SelectParser.StringLiteralContext ctx)
	{
		String retval = ctx.STRING().getText();
		retval = retval.substring(1, retval.length() - 1);
		return new Literal(retval.replace("\\'", "'"));
	}

	@Override
	public SubSelect visitSubSelect(final SelectParser.SubSelectContext ctx)
	{
		final SelectClause select = (SelectClause)visit(ctx.selectClause());
		final FromClause from = (FromClause)visit(ctx.fromClause());
		Where where = null;
		GroupBy groupBy = null;
		Having having = null;
		OrderBy orderBy = null;
		FetchFirst fetchFirst = null;

		if (ctx.whereClause() != null)
		{
			where = (Where)visit(ctx.whereClause());
		}

		if (ctx.groupBy() != null)
		{
			groupBy = (GroupBy)visit(ctx.groupBy());
		}

		if (ctx.havingClause() != null)
		{
			having = (Having)visit(ctx.havingClause());
		}

		if (ctx.orderBy() != null)
		{
			orderBy = (OrderBy)visit(ctx.orderBy());
		}

		if (ctx.fetchFirst() != null)
		{
			fetchFirst = (FetchFirst)visit(ctx.fetchFirst());
		}

		return new SubSelect(select, from, where, groupBy, having, orderBy, fetchFirst);
	}

	@Override
	public TableName visitTable1Part(final SelectParser.Table1PartContext ctx)
	{
		return new TableName(ctx.IDENTIFIER().getText());
	}

	@Override
	public TableName visitTable2Part(final SelectParser.Table2PartContext ctx)
	{
		return new TableName(ctx.IDENTIFIER(0).getText(), ctx.IDENTIFIER(1).getText());
	}

	@Override
	public Update visitUpdate(final SelectParser.UpdateContext ctx)
	{
		final TableName table = (TableName)visit(ctx.tableName());

		if (ctx.setClause().size() == 1)
		{
			List<Column> cols;
			if (ctx.setClause(0).colList() == null)
			{
				cols = new ArrayList<Column>(1);
				cols.add((Column)visit(ctx.setClause(0).columnName()));
			}
			else
			{
				cols = (List<Column>)visit(ctx.setClause(0).colList());
			}
			final Expression exp = (Expression)visit(ctx.setClause(0).expression());
			Where where = null;
			if (ctx.setClause(0).whereClause() != null)
			{
				where = (Where)visit(ctx.setClause(0).whereClause());
			}

			return new Update(table, cols, exp, where);
		}

		final List<List<Column>> cols2 = new ArrayList<List<Column>>();
		final List<Expression> exps2 = new ArrayList<Expression>();
		final List<Where> wheres2 = new ArrayList<Where>();
		for (final SelectParser.SetClauseContext ctx2 : ctx.setClause())
		{
			List<Column> cols;
			if (ctx2.colList() == null)
			{
				cols = new ArrayList<Column>(1);
				cols.add((Column)visit(ctx2.columnName()));
			}
			else
			{
				cols = (List<Column>)visit(ctx2.colList());
			}
			final Expression exp = (Expression)visit(ctx2.expression());
			Where where = null;
			if (ctx2.whereClause() != null)
			{
				where = (Where)visit(ctx2.whereClause());
			}

			cols2.add(cols);
			exps2.add(exp);
			wheres2.add(where);
		}

		return new Update(table, cols2, exps2, wheres2);
	}

	@Override
	public Where visitWhereClause(final SelectParser.WhereClauseContext ctx)
	{
		return new Where((SearchCondition)visit(ctx.searchCondition()));
	}
}
