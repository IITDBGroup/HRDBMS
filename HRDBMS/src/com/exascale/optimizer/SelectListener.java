package com.exascale.optimizer;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SelectParser}.
 */
public interface SelectListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SelectParser#JoinP}.
	 * @param ctx the parse tree
	 */
	void enterJoinP(@NotNull SelectParser.JoinPContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#JoinP}.
	 * @param ctx the parse tree
	 */
	void exitJoinP(@NotNull SelectParser.JoinPContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#correlationClause}.
	 * @param ctx the parse tree
	 */
	void enterCorrelationClause(@NotNull SelectParser.CorrelationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#correlationClause}.
	 * @param ctx the parse tree
	 */
	void exitCorrelationClause(@NotNull SelectParser.CorrelationClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(@NotNull SelectParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(@NotNull SelectParser.WhereClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Col1Part}.
	 * @param ctx the parse tree
	 */
	void enterCol1Part(@NotNull SelectParser.Col1PartContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Col1Part}.
	 * @param ctx the parse tree
	 */
	void exitCol1Part(@NotNull SelectParser.Col1PartContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#select}.
	 * @param ctx the parse tree
	 */
	void enterSelect(@NotNull SelectParser.SelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#select}.
	 * @param ctx the parse tree
	 */
	void exitSelect(@NotNull SelectParser.SelectContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#IsSingleTable}.
	 * @param ctx the parse tree
	 */
	void enterIsSingleTable(@NotNull SelectParser.IsSingleTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#IsSingleTable}.
	 * @param ctx the parse tree
	 */
	void exitIsSingleTable(@NotNull SelectParser.IsSingleTableContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#NullExp}.
	 * @param ctx the parse tree
	 */
	void enterNullExp(@NotNull SelectParser.NullExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#NullExp}.
	 * @param ctx the parse tree
	 */
	void exitNullExp(@NotNull SelectParser.NullExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#dropView}.
	 * @param ctx the parse tree
	 */
	void enterDropView(@NotNull SelectParser.DropViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dropView}.
	 * @param ctx the parse tree
	 */
	void exitDropView(@NotNull SelectParser.DropViewContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#operator}.
	 * @param ctx the parse tree
	 */
	void enterOperator(@NotNull SelectParser.OperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#operator}.
	 * @param ctx the parse tree
	 */
	void exitOperator(@NotNull SelectParser.OperatorContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#dropIndex}.
	 * @param ctx the parse tree
	 */
	void enterDropIndex(@NotNull SelectParser.DropIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dropIndex}.
	 * @param ctx the parse tree
	 */
	void exitDropIndex(@NotNull SelectParser.DropIndexContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#long2}.
	 * @param ctx the parse tree
	 */
	void enterLong2(@NotNull SelectParser.Long2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#long2}.
	 * @param ctx the parse tree
	 */
	void exitLong2(@NotNull SelectParser.Long2Context ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Concat}.
	 * @param ctx the parse tree
	 */
	void enterConcat(@NotNull SelectParser.ConcatContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Concat}.
	 * @param ctx the parse tree
	 */
	void exitConcat(@NotNull SelectParser.ConcatContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Col2Part}.
	 * @param ctx the parse tree
	 */
	void enterCol2Part(@NotNull SelectParser.Col2PartContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Col2Part}.
	 * @param ctx the parse tree
	 */
	void exitCol2Part(@NotNull SelectParser.Col2PartContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#int2}.
	 * @param ctx the parse tree
	 */
	void enterInt2(@NotNull SelectParser.Int2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#int2}.
	 * @param ctx the parse tree
	 */
	void exitInt2(@NotNull SelectParser.Int2Context ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#connectedSearchClause}.
	 * @param ctx the parse tree
	 */
	void enterConnectedSearchClause(@NotNull SelectParser.ConnectedSearchClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#connectedSearchClause}.
	 * @param ctx the parse tree
	 */
	void exitConnectedSearchClause(@NotNull SelectParser.ConnectedSearchClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#connectedSelect}.
	 * @param ctx the parse tree
	 */
	void enterConnectedSelect(@NotNull SelectParser.ConnectedSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#connectedSelect}.
	 * @param ctx the parse tree
	 */
	void exitConnectedSelect(@NotNull SelectParser.ConnectedSelectContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#List}.
	 * @param ctx the parse tree
	 */
	void enterList(@NotNull SelectParser.ListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#List}.
	 * @param ctx the parse tree
	 */
	void exitList(@NotNull SelectParser.ListContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(@NotNull SelectParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(@NotNull SelectParser.IdentifierContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#rangeType}.
	 * @param ctx the parse tree
	 */
	void enterRangeType(@NotNull SelectParser.RangeTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#rangeType}.
	 * @param ctx the parse tree
	 */
	void exitRangeType(@NotNull SelectParser.RangeTypeContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#notNull}.
	 * @param ctx the parse tree
	 */
	void enterNotNull(@NotNull SelectParser.NotNullContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#notNull}.
	 * @param ctx the parse tree
	 */
	void exitNotNull(@NotNull SelectParser.NotNullContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterDataType(@NotNull SelectParser.DataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitDataType(@NotNull SelectParser.DataTypeContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#groupDef}.
	 * @param ctx the parse tree
	 */
	void enterGroupDef(@NotNull SelectParser.GroupDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#groupDef}.
	 * @param ctx the parse tree
	 */
	void exitGroupDef(@NotNull SelectParser.GroupDefContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#integerSet}.
	 * @param ctx the parse tree
	 */
	void enterIntegerSet(@NotNull SelectParser.IntegerSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#integerSet}.
	 * @param ctx the parse tree
	 */
	void exitIntegerSet(@NotNull SelectParser.IntegerSetContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#SortKeyCol}.
	 * @param ctx the parse tree
	 */
	void enterSortKeyCol(@NotNull SelectParser.SortKeyColContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#SortKeyCol}.
	 * @param ctx the parse tree
	 */
	void exitSortKeyCol(@NotNull SelectParser.SortKeyColContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#setClause}.
	 * @param ctx the parse tree
	 */
	void enterSetClause(@NotNull SelectParser.SetClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#setClause}.
	 * @param ctx the parse tree
	 */
	void exitSetClause(@NotNull SelectParser.SetClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#groupExp}.
	 * @param ctx the parse tree
	 */
	void enterGroupExp(@NotNull SelectParser.GroupExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#groupExp}.
	 * @param ctx the parse tree
	 */
	void exitGroupExp(@NotNull SelectParser.GroupExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#rangeExp}.
	 * @param ctx the parse tree
	 */
	void enterRangeExp(@NotNull SelectParser.RangeExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#rangeExp}.
	 * @param ctx the parse tree
	 */
	void exitRangeExp(@NotNull SelectParser.RangeExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#valuesList}.
	 * @param ctx the parse tree
	 */
	void enterValuesList(@NotNull SelectParser.ValuesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#valuesList}.
	 * @param ctx the parse tree
	 */
	void exitValuesList(@NotNull SelectParser.ValuesListContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#StringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(@NotNull SelectParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#StringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(@NotNull SelectParser.StringLiteralContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#colList}.
	 * @param ctx the parse tree
	 */
	void enterColList(@NotNull SelectParser.ColListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#colList}.
	 * @param ctx the parse tree
	 */
	void exitColList(@NotNull SelectParser.ColListContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#columnSet}.
	 * @param ctx the parse tree
	 */
	void enterColumnSet(@NotNull SelectParser.ColumnSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#columnSet}.
	 * @param ctx the parse tree
	 */
	void exitColumnSet(@NotNull SelectParser.ColumnSetContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#SortKeyInt}.
	 * @param ctx the parse tree
	 */
	void enterSortKeyInt(@NotNull SelectParser.SortKeyIntContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#SortKeyInt}.
	 * @param ctx the parse tree
	 */
	void exitSortKeyInt(@NotNull SelectParser.SortKeyIntContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#nodeExp}.
	 * @param ctx the parse tree
	 */
	void enterNodeExp(@NotNull SelectParser.NodeExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#nodeExp}.
	 * @param ctx the parse tree
	 */
	void exitNodeExp(@NotNull SelectParser.NodeExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#hashExp}.
	 * @param ctx the parse tree
	 */
	void enterHashExp(@NotNull SelectParser.HashExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#hashExp}.
	 * @param ctx the parse tree
	 */
	void exitHashExp(@NotNull SelectParser.HashExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#caseCase}.
	 * @param ctx the parse tree
	 */
	void enterCaseCase(@NotNull SelectParser.CaseCaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#caseCase}.
	 * @param ctx the parse tree
	 */
	void exitCaseCase(@NotNull SelectParser.CaseCaseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#primary}.
	 * @param ctx the parse tree
	 */
	void enterPrimary(@NotNull SelectParser.PrimaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#primary}.
	 * @param ctx the parse tree
	 */
	void exitPrimary(@NotNull SelectParser.PrimaryContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#char2}.
	 * @param ctx the parse tree
	 */
	void enterChar2(@NotNull SelectParser.Char2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#char2}.
	 * @param ctx the parse tree
	 */
	void exitChar2(@NotNull SelectParser.Char2Context ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#createTable}.
	 * @param ctx the parse tree
	 */
	void enterCreateTable(@NotNull SelectParser.CreateTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createTable}.
	 * @param ctx the parse tree
	 */
	void exitCreateTable(@NotNull SelectParser.CreateTableContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#NestedTable}.
	 * @param ctx the parse tree
	 */
	void enterNestedTable(@NotNull SelectParser.NestedTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#NestedTable}.
	 * @param ctx the parse tree
	 */
	void exitNestedTable(@NotNull SelectParser.NestedTableContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#singleTable}.
	 * @param ctx the parse tree
	 */
	void enterSingleTable(@NotNull SelectParser.SingleTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#singleTable}.
	 * @param ctx the parse tree
	 */
	void exitSingleTable(@NotNull SelectParser.SingleTableContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#realNodeExp}.
	 * @param ctx the parse tree
	 */
	void enterRealNodeExp(@NotNull SelectParser.RealNodeExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#realNodeExp}.
	 * @param ctx the parse tree
	 */
	void exitRealNodeExp(@NotNull SelectParser.RealNodeExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#ExpSelect}.
	 * @param ctx the parse tree
	 */
	void enterExpSelect(@NotNull SelectParser.ExpSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#ExpSelect}.
	 * @param ctx the parse tree
	 */
	void exitExpSelect(@NotNull SelectParser.ExpSelectContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#NumericLiteral}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(@NotNull SelectParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#NumericLiteral}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(@NotNull SelectParser.NumericLiteralContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#MulDiv}.
	 * @param ctx the parse tree
	 */
	void enterMulDiv(@NotNull SelectParser.MulDivContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#MulDiv}.
	 * @param ctx the parse tree
	 */
	void exitMulDiv(@NotNull SelectParser.MulDivContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#IsLiteral}.
	 * @param ctx the parse tree
	 */
	void enterIsLiteral(@NotNull SelectParser.IsLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#IsLiteral}.
	 * @param ctx the parse tree
	 */
	void exitIsLiteral(@NotNull SelectParser.IsLiteralContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#ExistsPredicate}.
	 * @param ctx the parse tree
	 */
	void enterExistsPredicate(@NotNull SelectParser.ExistsPredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#ExistsPredicate}.
	 * @param ctx the parse tree
	 */
	void exitExistsPredicate(@NotNull SelectParser.ExistsPredicateContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#insert}.
	 * @param ctx the parse tree
	 */
	void enterInsert(@NotNull SelectParser.InsertContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#insert}.
	 * @param ctx the parse tree
	 */
	void exitInsert(@NotNull SelectParser.InsertContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#update}.
	 * @param ctx the parse tree
	 */
	void enterUpdate(@NotNull SelectParser.UpdateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#update}.
	 * @param ctx the parse tree
	 */
	void exitUpdate(@NotNull SelectParser.UpdateContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#orderBy}.
	 * @param ctx the parse tree
	 */
	void enterOrderBy(@NotNull SelectParser.OrderByContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#orderBy}.
	 * @param ctx the parse tree
	 */
	void exitOrderBy(@NotNull SelectParser.OrderByContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#runstats}.
	 * @param ctx the parse tree
	 */
	void enterRunstats(@NotNull SelectParser.RunstatsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#runstats}.
	 * @param ctx the parse tree
	 */
	void exitRunstats(@NotNull SelectParser.RunstatsContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#SelectColumn}.
	 * @param ctx the parse tree
	 */
	void enterSelectColumn(@NotNull SelectParser.SelectColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#SelectColumn}.
	 * @param ctx the parse tree
	 */
	void exitSelectColumn(@NotNull SelectParser.SelectColumnContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterGroupBy(@NotNull SelectParser.GroupByContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitGroupBy(@NotNull SelectParser.GroupByContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Table2Part}.
	 * @param ctx the parse tree
	 */
	void enterTable2Part(@NotNull SelectParser.Table2PartContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Table2Part}.
	 * @param ctx the parse tree
	 */
	void exitTable2Part(@NotNull SelectParser.Table2PartContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#delete}.
	 * @param ctx the parse tree
	 */
	void enterDelete(@NotNull SelectParser.DeleteContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#delete}.
	 * @param ctx the parse tree
	 */
	void exitDelete(@NotNull SelectParser.DeleteContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#realGroupExp}.
	 * @param ctx the parse tree
	 */
	void enterRealGroupExp(@NotNull SelectParser.RealGroupExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#realGroupExp}.
	 * @param ctx the parse tree
	 */
	void exitRealGroupExp(@NotNull SelectParser.RealGroupExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#NormalPredicate}.
	 * @param ctx the parse tree
	 */
	void enterNormalPredicate(@NotNull SelectParser.NormalPredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#NormalPredicate}.
	 * @param ctx the parse tree
	 */
	void exitNormalPredicate(@NotNull SelectParser.NormalPredicateContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#PExpression}.
	 * @param ctx the parse tree
	 */
	void enterPExpression(@NotNull SelectParser.PExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#PExpression}.
	 * @param ctx the parse tree
	 */
	void exitPExpression(@NotNull SelectParser.PExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#fetchFirst}.
	 * @param ctx the parse tree
	 */
	void enterFetchFirst(@NotNull SelectParser.FetchFirstContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#fetchFirst}.
	 * @param ctx the parse tree
	 */
	void exitFetchFirst(@NotNull SelectParser.FetchFirstContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#searchClause}.
	 * @param ctx the parse tree
	 */
	void enterSearchClause(@NotNull SelectParser.SearchClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#searchClause}.
	 * @param ctx the parse tree
	 */
	void exitSearchClause(@NotNull SelectParser.SearchClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Function}.
	 * @param ctx the parse tree
	 */
	void enterFunction(@NotNull SelectParser.FunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Function}.
	 * @param ctx the parse tree
	 */
	void exitFunction(@NotNull SelectParser.FunctionContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#ColLiteral}.
	 * @param ctx the parse tree
	 */
	void enterColLiteral(@NotNull SelectParser.ColLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#ColLiteral}.
	 * @param ctx the parse tree
	 */
	void exitColLiteral(@NotNull SelectParser.ColLiteralContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#load}.
	 * @param ctx the parse tree
	 */
	void enterLoad(@NotNull SelectParser.LoadContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#load}.
	 * @param ctx the parse tree
	 */
	void exitLoad(@NotNull SelectParser.LoadContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(@NotNull SelectParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(@NotNull SelectParser.SelectClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#colDef}.
	 * @param ctx the parse tree
	 */
	void enterColDef(@NotNull SelectParser.ColDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#colDef}.
	 * @param ctx the parse tree
	 */
	void exitColDef(@NotNull SelectParser.ColDefContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Table1Part}.
	 * @param ctx the parse tree
	 */
	void enterTable1Part(@NotNull SelectParser.Table1PartContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Table1Part}.
	 * @param ctx the parse tree
	 */
	void exitTable1Part(@NotNull SelectParser.Table1PartContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#createView}.
	 * @param ctx the parse tree
	 */
	void enterCreateView(@NotNull SelectParser.CreateViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createView}.
	 * @param ctx the parse tree
	 */
	void exitCreateView(@NotNull SelectParser.CreateViewContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#CrossJoin}.
	 * @param ctx the parse tree
	 */
	void enterCrossJoin(@NotNull SelectParser.CrossJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#CrossJoin}.
	 * @param ctx the parse tree
	 */
	void exitCrossJoin(@NotNull SelectParser.CrossJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#dropTable}.
	 * @param ctx the parse tree
	 */
	void enterDropTable(@NotNull SelectParser.DropTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dropTable}.
	 * @param ctx the parse tree
	 */
	void exitDropTable(@NotNull SelectParser.DropTableContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#commonTableExpression}.
	 * @param ctx the parse tree
	 */
	void enterCommonTableExpression(@NotNull SelectParser.CommonTableExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#commonTableExpression}.
	 * @param ctx the parse tree
	 */
	void exitCommonTableExpression(@NotNull SelectParser.CommonTableExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#float2}.
	 * @param ctx the parse tree
	 */
	void enterFloat2(@NotNull SelectParser.Float2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#float2}.
	 * @param ctx the parse tree
	 */
	void exitFloat2(@NotNull SelectParser.Float2Context ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(@NotNull SelectParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(@NotNull SelectParser.HavingClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#CountStar}.
	 * @param ctx the parse tree
	 */
	void enterCountStar(@NotNull SelectParser.CountStarContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#CountStar}.
	 * @param ctx the parse tree
	 */
	void exitCountStar(@NotNull SelectParser.CountStarContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(@NotNull SelectParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(@NotNull SelectParser.FromClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#CrossJoinP}.
	 * @param ctx the parse tree
	 */
	void enterCrossJoinP(@NotNull SelectParser.CrossJoinPContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#CrossJoinP}.
	 * @param ctx the parse tree
	 */
	void exitCrossJoinP(@NotNull SelectParser.CrossJoinPContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#AddSub}.
	 * @param ctx the parse tree
	 */
	void enterAddSub(@NotNull SelectParser.AddSubContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#AddSub}.
	 * @param ctx the parse tree
	 */
	void exitAddSub(@NotNull SelectParser.AddSubContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#fullSelect}.
	 * @param ctx the parse tree
	 */
	void enterFullSelect(@NotNull SelectParser.FullSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#fullSelect}.
	 * @param ctx the parse tree
	 */
	void exitFullSelect(@NotNull SelectParser.FullSelectContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#searchCondition}.
	 * @param ctx the parse tree
	 */
	void enterSearchCondition(@NotNull SelectParser.SearchConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#searchCondition}.
	 * @param ctx the parse tree
	 */
	void exitSearchCondition(@NotNull SelectParser.SearchConditionContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#CaseExp}.
	 * @param ctx the parse tree
	 */
	void enterCaseExp(@NotNull SelectParser.CaseExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#CaseExp}.
	 * @param ctx the parse tree
	 */
	void exitCaseExp(@NotNull SelectParser.CaseExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#deviceExp}.
	 * @param ctx the parse tree
	 */
	void enterDeviceExp(@NotNull SelectParser.DeviceExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#deviceExp}.
	 * @param ctx the parse tree
	 */
	void exitDeviceExp(@NotNull SelectParser.DeviceExpContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#indexDef}.
	 * @param ctx the parse tree
	 */
	void enterIndexDef(@NotNull SelectParser.IndexDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#indexDef}.
	 * @param ctx the parse tree
	 */
	void exitIndexDef(@NotNull SelectParser.IndexDefContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#Join}.
	 * @param ctx the parse tree
	 */
	void enterJoin(@NotNull SelectParser.JoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#Join}.
	 * @param ctx the parse tree
	 */
	void exitJoin(@NotNull SelectParser.JoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#date2}.
	 * @param ctx the parse tree
	 */
	void enterDate2(@NotNull SelectParser.Date2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#date2}.
	 * @param ctx the parse tree
	 */
	void exitDate2(@NotNull SelectParser.Date2Context ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#SelectExpression}.
	 * @param ctx the parse tree
	 */
	void enterSelectExpression(@NotNull SelectParser.SelectExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#SelectExpression}.
	 * @param ctx the parse tree
	 */
	void exitSelectExpression(@NotNull SelectParser.SelectExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#colOrder}.
	 * @param ctx the parse tree
	 */
	void enterColOrder(@NotNull SelectParser.ColOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#colOrder}.
	 * @param ctx the parse tree
	 */
	void exitColOrder(@NotNull SelectParser.ColOrderContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#selecthow}.
	 * @param ctx the parse tree
	 */
	void enterSelecthow(@NotNull SelectParser.SelecthowContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#selecthow}.
	 * @param ctx the parse tree
	 */
	void exitSelecthow(@NotNull SelectParser.SelecthowContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#createIndex}.
	 * @param ctx the parse tree
	 */
	void enterCreateIndex(@NotNull SelectParser.CreateIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createIndex}.
	 * @param ctx the parse tree
	 */
	void exitCreateIndex(@NotNull SelectParser.CreateIndexContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#any}.
	 * @param ctx the parse tree
	 */
	void enterAny(@NotNull SelectParser.AnyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#any}.
	 * @param ctx the parse tree
	 */
	void exitAny(@NotNull SelectParser.AnyContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#subSelect}.
	 * @param ctx the parse tree
	 */
	void enterSubSelect(@NotNull SelectParser.SubSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#subSelect}.
	 * @param ctx the parse tree
	 */
	void exitSubSelect(@NotNull SelectParser.SubSelectContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#organization}.
	 * @param ctx the parse tree
	 */
	void enterOrganization(@NotNull SelectParser.OrganizationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#organization}.
	 * @param ctx the parse tree
	 */
	void exitOrganization(@NotNull SelectParser.OrganizationContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#rangeSet}.
	 * @param ctx the parse tree
	 */
	void enterRangeSet(@NotNull SelectParser.RangeSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#rangeSet}.
	 * @param ctx the parse tree
	 */
	void exitRangeSet(@NotNull SelectParser.RangeSetContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#NullPredicate}.
	 * @param ctx the parse tree
	 */
	void enterNullPredicate(@NotNull SelectParser.NullPredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#NullPredicate}.
	 * @param ctx the parse tree
	 */
	void exitNullPredicate(@NotNull SelectParser.NullPredicateContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#CountDistinct}.
	 * @param ctx the parse tree
	 */
	void enterCountDistinct(@NotNull SelectParser.CountDistinctContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#CountDistinct}.
	 * @param ctx the parse tree
	 */
	void exitCountDistinct(@NotNull SelectParser.CountDistinctContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#remainder}.
	 * @param ctx the parse tree
	 */
	void enterRemainder(@NotNull SelectParser.RemainderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#remainder}.
	 * @param ctx the parse tree
	 */
	void exitRemainder(@NotNull SelectParser.RemainderContext ctx);

	/**
	 * Enter a parse tree produced by {@link SelectParser#primaryKey}.
	 * @param ctx the parse tree
	 */
	void enterPrimaryKey(@NotNull SelectParser.PrimaryKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#primaryKey}.
	 * @param ctx the parse tree
	 */
	void exitPrimaryKey(@NotNull SelectParser.PrimaryKeyContext ctx);
}