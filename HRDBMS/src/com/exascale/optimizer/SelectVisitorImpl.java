package com.exascale.optimizer;

import java.util.ArrayList;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import com.exascale.misc.Utils;

public class SelectVisitorImpl extends SelectBaseVisitor<Object>
{
	@Override
	public Expression visitAddSub(SelectParser.AddSubContext ctx)
	{
		return new Expression((Expression)visit(ctx.expression(0)), ctx.op.getText(), (Expression)visit(ctx.expression(1)));
	}

	@Override
	public Case visitCaseCase(SelectParser.CaseCaseContext ctx)
	{
		return new Case((SearchCondition)visit(ctx.searchCondition()), (Expression)visit(ctx.expression()));
	}

	@Override
	public Expression visitCaseExp(SelectParser.CaseExpContext ctx)
	{
		ArrayList<Case> cases = new ArrayList<Case>(ctx.caseCase().size());
		for (SelectParser.CaseCaseContext context : ctx.caseCase())
		{
			cases.add((Case)visit(context));
		}
		return new Expression(cases, (Expression)visit(ctx.expression()));
	}

	@Override
	public Column visitCol1Part(SelectParser.Col1PartContext ctx)
	{
		return new Column(ctx.IDENTIFIER().getText());
	}

	@Override
	public Column visitCol2Part(SelectParser.Col2PartContext ctx)
	{
		return new Column(ctx.IDENTIFIER(0).getText(), ctx.IDENTIFIER(1).getText());
	}

	@Override
	public ColDef visitColDef(SelectParser.ColDefContext ctx)
	{
		Column col = (Column)visit(ctx.columnName());
		String type = (String)visit(ctx.dataType());
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
	public ArrayList<Column> visitColList(SelectParser.ColListContext ctx)
	{
		ArrayList<Column> cols = new ArrayList<Column>();
		for (SelectParser.ColumnNameContext context : ctx.columnName())
		{
			cols.add((Column)visit(context));
		}

		return cols;
	}

	@Override
	public Expression visitColLiteral(SelectParser.ColLiteralContext ctx)
	{
		return new Expression((Column)visit(ctx.columnName()));
	}

	@Override
	public ArrayList<Integer> visitColOrder(SelectParser.ColOrderContext ctx)
	{
		ArrayList<Integer> colOrder = new ArrayList<Integer>();
		for (TerminalNode node : ctx.INTEGER())
		{
			colOrder.add(Integer.parseInt(node.getText().trim()));
		}

		return colOrder;
	}

	@Override
	public CTE visitCommonTableExpression(SelectParser.CommonTableExpressionContext ctx)
	{
		String tableName = ctx.IDENTIFIER().getText();
		ArrayList<Column> cols = new ArrayList<Column>();

		if (ctx.columnName() != null)
		{
			for (SelectParser.ColumnNameContext context : ctx.columnName())
			{
				cols.add((Column)visit(context));
			}
		}

		FullSelect select = (FullSelect)visit(ctx.fullSelect());
		return new CTE(tableName, cols, select);
	}

	@Override
	public Expression visitConcat(SelectParser.ConcatContext ctx)
	{
		return new Expression((Expression)visit(ctx.expression(0)), "||", (Expression)visit(ctx.expression(1)));
	}

	@Override
	public ConnectedSearchClause visitConnectedSearchClause(SelectParser.ConnectedSearchClauseContext ctx)
	{
		boolean and = (ctx.AND() != null);
		return new ConnectedSearchClause((SearchClause)visit(ctx.searchClause()), and);
	}

	@Override
	public ConnectedSelect visitConnectedSelect(SelectParser.ConnectedSelectContext ctx)
	{
		String combo = ctx.TABLECOMBINATION().getText();
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
	public String visitCorrelationClause(SelectParser.CorrelationClauseContext ctx)
	{
		return ctx.IDENTIFIER().getText();
	}

	@Override
	public Expression visitCountDistinct(SelectParser.CountDistinctContext ctx)
	{
		ArrayList<Expression> arguments = new ArrayList<Expression>(1);
		arguments.add((Expression)visit(ctx.expression()));
		return new Expression(new Function("COUNT", arguments, true));
	}

	@Override
	public Expression visitCountStar(SelectParser.CountStarContext ctx)
	{
		return new Expression();
	}

	@Override
	public CreateIndex visitCreateIndex(SelectParser.CreateIndexContext ctx)
	{
		TableName index = (TableName)visit(ctx.tableName(0));
		TableName table = (TableName)visit(ctx.tableName(1));
		ArrayList<IndexDef> defs = new ArrayList<IndexDef>();
		for (SelectParser.IndexDefContext context : ctx.indexDef())
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
	public CreateTable visitCreateTable(SelectParser.CreateTableContext ctx)
	{
		TableName table = (TableName)visit(ctx.tableName());
		ArrayList<ColDef> cols = new ArrayList<ColDef>();
		for (SelectParser.ColDefContext context : ctx.colDef())
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

		ArrayList<Integer> colOrder = null;
		if (ctx.colOrder() != null)
		{
			colOrder = (ArrayList<Integer>)visit(ctx.colOrder());
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
			ArrayList<Integer> organization = (ArrayList<Integer>)visit(ctx.organization());
			retval.setOrganization(organization);
		}

		return retval;
	}

	@Override
	public CreateView visitCreateView(SelectParser.CreateViewContext ctx)
	{
		TableName view = (TableName)visit(ctx.tableName());
		FullSelect select = (FullSelect)visit(ctx.fullSelect());
		int a = ctx.fullSelect().start.getStartIndex();
		int b = ctx.fullSelect().stop.getStopIndex();
		Interval interval = new Interval(a, b);
		CharStream input = ctx.fullSelect().start.getInputStream();
		String text = input.getText(interval);
		return new CreateView(view, select, text);
	}

	@Override
	public TableReference visitCrossJoin(SelectParser.CrossJoinContext ctx)
	{
		TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		String alias = null;
		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, "CP", rhs, null, alias);
	}

	@Override
	public TableReference visitCrossJoinP(SelectParser.CrossJoinPContext ctx)
	{
		TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		String alias = null;

		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, "CP", rhs, null, alias);
	}

	@Override
	public String visitDataType(SelectParser.DataTypeContext ctx)
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
	public Delete visitDelete(SelectParser.DeleteContext ctx)
	{
		TableName table = (TableName)visit(ctx.tableName());
		Where where = null;
		if (ctx.whereClause() != null)
		{
			where = (Where)visit(ctx.whereClause());
		}

		return new Delete(table, where);
	}

	@Override
	public DropIndex visitDropIndex(SelectParser.DropIndexContext ctx)
	{
		return new DropIndex((TableName)visit(ctx.tableName()));
	}

	@Override
	public DropTable visitDropTable(SelectParser.DropTableContext ctx)
	{
		return new DropTable((TableName)visit(ctx.tableName()));
	}

	@Override
	public DropView visitDropView(SelectParser.DropViewContext ctx)
	{
		return new DropView((TableName)visit(ctx.tableName()));
	}

	@Override
	public ExistsPredicate visitExistsPredicate(SelectParser.ExistsPredicateContext ctx)
	{
		return new ExistsPredicate((SubSelect)visit(ctx.subSelect()));
	}

	@Override
	public Expression visitExpSelect(SelectParser.ExpSelectContext ctx)
	{
		return new Expression((SubSelect)visit(ctx.subSelect()));
	}

	@Override
	public FetchFirst visitFetchFirst(SelectParser.FetchFirstContext ctx)
	{
		long num = 1;
		if (ctx.INTEGER() != null)
		{
			num = Utils.parseLong(ctx.INTEGER().getText());
		}

		return new FetchFirst(num);
	}

	@Override
	public FromClause visitFromClause(SelectParser.FromClauseContext ctx)
	{
		ArrayList<TableReference> tables = new ArrayList<TableReference>(ctx.tableReference().size());
		for (SelectParser.TableReferenceContext context : ctx.tableReference())
		{
			tables.add((TableReference)visit(context));
		}

		return new FromClause(tables);
	}

	@Override
	public FullSelect visitFullSelect(SelectParser.FullSelectContext ctx)
	{
		// (subSelect | '(' fullSelect ')') (connectedSelect)* (orderBy)?
		// (fetchFirst)? ;
		SubSelect sub = null;
		FullSelect full = null;
		ArrayList<ConnectedSelect> connected = new ArrayList<ConnectedSelect>();
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
			for (SelectParser.ConnectedSelectContext context : ctx.connectedSelect())
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
	public Expression visitFunction(SelectParser.FunctionContext ctx)
	{
		ArrayList<Expression> arguments = new ArrayList<Expression>(ctx.expression().size());
		for (SelectParser.ExpressionContext context : ctx.expression())
		{
			arguments.add((Expression)visit(context));
		}
		return new Expression(new Function(ctx.identifier().getText(), arguments));
	}

	@Override
	public GroupBy visitGroupBy(SelectParser.GroupByContext ctx)
	{
		ArrayList<Column> cols = new ArrayList<Column>(ctx.columnName().size());
		for (SelectParser.ColumnNameContext context : ctx.columnName())
		{
			cols.add((Column)visit(context));
		}

		return new GroupBy(cols);
	}

	@Override
	public Having visitHavingClause(SelectParser.HavingClauseContext ctx)
	{
		return new Having((SearchCondition)visit(ctx.searchCondition()));
	}

	@Override
	public IndexDef visitIndexDef(SelectParser.IndexDefContext ctx)
	{
		Column col = (Column)visit(ctx.columnName());
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
	public Insert visitInsert(SelectParser.InsertContext ctx)
	{
		if (ctx.fullSelect() != null)
		{
			return new Insert((TableName)visit(ctx.tableName()), (FullSelect)visit(ctx.fullSelect()));
		}
		
		if (ctx.valuesList().size() == 1)
		{
			ArrayList<Expression> exps = new ArrayList<Expression>();
			for (SelectParser.ExpressionContext exp : ctx.valuesList().get(0).expression())
			{
				exps.add((Expression)visit(exp));
			}
			return new Insert((TableName)visit(ctx.tableName()), exps);
		}
		else
		{
			ArrayList<ArrayList<Expression>> mExps = new ArrayList<ArrayList<Expression>>();
			for (SelectParser.ValuesListContext ctx2 : ctx.valuesList())
			{
				ArrayList<Expression> exps = new ArrayList<Expression>();
				for (SelectParser.ExpressionContext exp : ctx2.expression())
				{
					exps.add((Expression)visit(exp));
				}
				
				mExps.add(exps);
			}
			
			return new Insert((TableName)visit(ctx.tableName()), mExps, true);
		}
	}

	@Override
	public Expression visitIsLiteral(SelectParser.IsLiteralContext ctx)
	{
		return new Expression((Literal)visit(ctx.literal()));
	}

	@Override
	public TableReference visitIsSingleTable(SelectParser.IsSingleTableContext ctx)
	{
		return new TableReference((SingleTable)visit(ctx.singleTable()));
	}

	@Override
	public TableReference visitJoin(SelectParser.JoinContext ctx)
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

		TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		SearchCondition search = (SearchCondition)visit(ctx.searchCondition());
		String alias = null;
		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, op, rhs, search, alias);
	}

	@Override
	public TableReference visitJoinP(SelectParser.JoinPContext ctx)
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

		TableReference lhs = (TableReference)visit(ctx.tableReference(0));
		TableReference rhs = (TableReference)visit(ctx.tableReference(1));
		SearchCondition search = (SearchCondition)visit(ctx.searchCondition());
		String alias = null;
		if (ctx.correlationClause() != null)
		{
			alias = (String)visit(ctx.correlationClause());
		}

		return new TableReference(lhs, op, rhs, search, alias);
	}

	@Override
	public Expression visitList(SelectParser.ListContext ctx)
	{
		ArrayList<Expression> exps = new ArrayList<Expression>(ctx.expression().size());
		for (SelectParser.ExpressionContext context : ctx.expression())
		{
			exps.add((Expression)visit(context));
		}

		return new Expression(exps);
	}

	@Override
	public Load visitLoad(SelectParser.LoadContext ctx)
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

		TableName table = (TableName)visit(ctx.tableName());
		String glob = ctx.remainder().getText();
		int first = glob.indexOf('\'');
		int second = glob.indexOf('\'', first + 1);
		glob = glob.substring(first + 1, second);

		return new Load(table, replace, delimited, glob);
	}

	@Override
	public Expression visitMulDiv(SelectParser.MulDivContext ctx)
	{
		return new Expression((Expression)visit(ctx.expression(0)), ctx.op.getText(), (Expression)visit(ctx.expression(1)));
	}

	@Override
	public TableReference visitNestedTable(SelectParser.NestedTableContext ctx)
	{
		FullSelect fs = (FullSelect)visit(ctx.fullSelect());
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
	public Predicate visitNormalPredicate(SelectParser.NormalPredicateContext ctx)
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
	public Expression visitNullExp(SelectParser.NullExpContext ctx)
	{
		return new Expression(new Literal(null));
	}

	@Override
	public Predicate visitNullPredicate(SelectParser.NullPredicateContext ctx)
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
	public Literal visitNumericLiteral(SelectParser.NumericLiteralContext ctx)
	{
		String number = ctx.getText();
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
	public OrderBy visitOrderBy(SelectParser.OrderByContext ctx)
	{
		ArrayList<SortKey> keys = new ArrayList<SortKey>(ctx.sortKey().size());
		for (SelectParser.SortKeyContext context : ctx.sortKey())
		{
			keys.add((SortKey)visit(context));
		}

		return new OrderBy(keys);
	}

	@Override
	public ArrayList<Integer> visitOrganization(SelectParser.OrganizationContext ctx)
	{
		ArrayList<Integer> organization = new ArrayList<Integer>();
		for (TerminalNode node : ctx.INTEGER())
		{
			organization.add(Integer.parseInt(node.getText().trim()));
		}

		return organization;
	}

	@Override
	public Expression visitPExpression(SelectParser.PExpressionContext ctx)
	{
		return (Expression)visit(ctx.expression());
	}

	@Override
	public PrimaryKey visitPrimaryKey(SelectParser.PrimaryKeyContext ctx)
	{
		ArrayList<Column> cols = new ArrayList<Column>();
		for (SelectParser.ColumnNameContext context : ctx.columnName())
		{
			cols.add((Column)visit(context));
		}

		return new PrimaryKey(cols);
	}

	@Override
	public Runstats visitRunstats(SelectParser.RunstatsContext ctx)
	{
		return new Runstats((TableName)visit(ctx.tableName()));
	}

	@Override
	public SearchClause visitSearchClause(SelectParser.SearchClauseContext ctx)
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
	public SearchCondition visitSearchCondition(SelectParser.SearchConditionContext ctx)
	{
		SearchClause search = (SearchClause)visit(ctx.searchClause());
		ArrayList<ConnectedSearchClause> connected = new ArrayList<ConnectedSearchClause>();
		if (ctx.connectedSearchClause() != null)
		{
			for (SelectParser.ConnectedSearchClauseContext context : ctx.connectedSearchClause())
			{
				connected.add((ConnectedSearchClause)visit(context));
			}
		}

		return new SearchCondition(search, connected);
	}

	@Override
	public SQLStatement visitSelect(SelectParser.SelectContext ctx)
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

		ArrayList<CTE> ctes = new ArrayList<CTE>();

		if (ctx.commonTableExpression() != null)
		{
			for (SelectParser.CommonTableExpressionContext context : ctx.commonTableExpression())
			{
				ctes.add((CTE)visit(context));
			}
		}

		FullSelect select = (FullSelect)visit(ctx.fullSelect());
		return new Select(ctes, select);
	}

	@Override
	public SelectClause visitSelectClause(SelectParser.SelectClauseContext ctx)
	{
		boolean selectAll = true;
		boolean selectStar = false;
		ArrayList<SelectListEntry> selectList = new ArrayList<SelectListEntry>();
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
			for (SelectParser.SelectListEntryContext context : ctx.selectListEntry())
			{
				selectList.add((SelectListEntry)visit(context));
			}
		}

		return new SelectClause(selectAll, selectStar, selectList);
	}

	@Override
	public SelectListEntry visitSelectColumn(SelectParser.SelectColumnContext ctx)
	{
		Column col = (Column)visit(ctx.columnName());
		String alias = null;
		if (ctx.IDENTIFIER() != null)
		{
			alias = ctx.IDENTIFIER().getText();
		}
		return new SelectListEntry(col, alias);
	}

	@Override
	public SelectListEntry visitSelectExpression(SelectParser.SelectExpressionContext ctx)
	{
		String alias = null;
		if (ctx.IDENTIFIER() != null)
		{
			alias = ctx.IDENTIFIER().getText();
		}
		return new SelectListEntry((Expression)visit(ctx.expression()), alias);
	}

	@Override
	public SingleTable visitSingleTable(SelectParser.SingleTableContext ctx)
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
	public SortKey visitSortKeyCol(SelectParser.SortKeyColContext ctx)
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
	public SortKey visitSortKeyInt(SelectParser.SortKeyIntContext ctx)
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
	public Literal visitStringLiteral(SelectParser.StringLiteralContext ctx)
	{
		String retval = ctx.STRING().getText();
		retval = retval.substring(1, retval.length() - 1);
		return new Literal(retval.replace("\\'", "'"));
	}

	@Override
	public SubSelect visitSubSelect(SelectParser.SubSelectContext ctx)
	{
		SelectClause select = (SelectClause)visit(ctx.selectClause());
		FromClause from = (FromClause)visit(ctx.fromClause());
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
	public TableName visitTable1Part(SelectParser.Table1PartContext ctx)
	{
		return new TableName(ctx.IDENTIFIER().getText());
	}

	@Override
	public TableName visitTable2Part(SelectParser.Table2PartContext ctx)
	{
		return new TableName(ctx.IDENTIFIER(0).getText(), ctx.IDENTIFIER(1).getText());
	}

	@Override
	public Update visitUpdate(SelectParser.UpdateContext ctx)
	{
		TableName table = (TableName)visit(ctx.tableName());
		
		if (ctx.setClause().size() == 1)
		{
			ArrayList<Column> cols;
			if (ctx.setClause(0).colList() == null)
			{
				cols = new ArrayList<Column>(1);
				cols.add((Column)visit(ctx.setClause(0).columnName()));
			}
			else
			{
				cols = (ArrayList<Column>)visit(ctx.setClause(0).colList());
			}
			Expression exp = (Expression)visit(ctx.setClause(0).expression());
			Where where = null;
			if (ctx.setClause(0).whereClause() != null)
			{
				where = (Where)visit(ctx.setClause(0).whereClause());
			}

			return new Update(table, cols, exp, where);
		}
		
		ArrayList<ArrayList<Column>> cols2 = new ArrayList<ArrayList<Column>>();
		ArrayList<Expression> exps2 = new ArrayList<Expression>();
		ArrayList<Where> wheres2 = new ArrayList<Where>();
		for (SelectParser.SetClauseContext ctx2 : ctx.setClause())
		{
			ArrayList<Column> cols;
			if (ctx2.colList() == null)
			{
				cols = new ArrayList<Column>(1);
				cols.add((Column)visit(ctx2.columnName()));
			}
			else
			{
				cols = (ArrayList<Column>)visit(ctx2.colList());
			}
			Expression exp = (Expression)visit(ctx2.expression());
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
	public Where visitWhereClause(SelectParser.WhereClauseContext ctx)
	{
		return new Where((SearchCondition)visit(ctx.searchCondition()));
	}
}
