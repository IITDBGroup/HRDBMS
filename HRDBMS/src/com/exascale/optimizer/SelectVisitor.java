package com.exascale.optimizer;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SelectParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface SelectVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link SelectParser#JoinP}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinP(@NotNull SelectParser.JoinPContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#correlationClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCorrelationClause(@NotNull SelectParser.CorrelationClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#whereClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereClause(@NotNull SelectParser.WhereClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Col1Part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCol1Part(@NotNull SelectParser.Col1PartContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#select}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect(@NotNull SelectParser.SelectContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#IsSingleTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsSingleTable(@NotNull SelectParser.IsSingleTableContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#NullExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullExp(@NotNull SelectParser.NullExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#dropView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropView(@NotNull SelectParser.DropViewContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#operator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperator(@NotNull SelectParser.OperatorContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#dropIndex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropIndex(@NotNull SelectParser.DropIndexContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#long2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLong2(@NotNull SelectParser.Long2Context ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Concat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcat(@NotNull SelectParser.ConcatContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Col2Part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCol2Part(@NotNull SelectParser.Col2PartContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#int2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInt2(@NotNull SelectParser.Int2Context ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#connectedSearchClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConnectedSearchClause(@NotNull SelectParser.ConnectedSearchClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#connectedSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConnectedSelect(@NotNull SelectParser.ConnectedSelectContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#List}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitList(@NotNull SelectParser.ListContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(@NotNull SelectParser.IdentifierContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#rangeType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeType(@NotNull SelectParser.RangeTypeContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#notNull}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotNull(@NotNull SelectParser.NotNullContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDataType(@NotNull SelectParser.DataTypeContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#groupDef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupDef(@NotNull SelectParser.GroupDefContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#integerSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerSet(@NotNull SelectParser.IntegerSetContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#SortKeyCol}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortKeyCol(@NotNull SelectParser.SortKeyColContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#groupExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupExp(@NotNull SelectParser.GroupExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#rangeExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeExp(@NotNull SelectParser.RangeExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#StringLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(@NotNull SelectParser.StringLiteralContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#colList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColList(@NotNull SelectParser.ColListContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#columnSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnSet(@NotNull SelectParser.ColumnSetContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#SortKeyInt}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortKeyInt(@NotNull SelectParser.SortKeyIntContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#nodeExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeExp(@NotNull SelectParser.NodeExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#hashExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHashExp(@NotNull SelectParser.HashExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#caseCase}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCaseCase(@NotNull SelectParser.CaseCaseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimary(@NotNull SelectParser.PrimaryContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#char2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitChar2(@NotNull SelectParser.Char2Context ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#createTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTable(@NotNull SelectParser.CreateTableContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#NestedTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedTable(@NotNull SelectParser.NestedTableContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#singleTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleTable(@NotNull SelectParser.SingleTableContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#realNodeExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealNodeExp(@NotNull SelectParser.RealNodeExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#ExpSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpSelect(@NotNull SelectParser.ExpSelectContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#NumericLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(@NotNull SelectParser.NumericLiteralContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#MulDiv}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMulDiv(@NotNull SelectParser.MulDivContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#IsLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsLiteral(@NotNull SelectParser.IsLiteralContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#ExistsPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExistsPredicate(@NotNull SelectParser.ExistsPredicateContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#insert}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsert(@NotNull SelectParser.InsertContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#update}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdate(@NotNull SelectParser.UpdateContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#orderBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderBy(@NotNull SelectParser.OrderByContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#runstats}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRunstats(@NotNull SelectParser.RunstatsContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#SelectColumn}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectColumn(@NotNull SelectParser.SelectColumnContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupBy(@NotNull SelectParser.GroupByContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Table2Part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable2Part(@NotNull SelectParser.Table2PartContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#delete}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDelete(@NotNull SelectParser.DeleteContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#realGroupExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealGroupExp(@NotNull SelectParser.RealGroupExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#NormalPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNormalPredicate(@NotNull SelectParser.NormalPredicateContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#PExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPExpression(@NotNull SelectParser.PExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#fetchFirst}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFetchFirst(@NotNull SelectParser.FetchFirstContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#searchClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchClause(@NotNull SelectParser.SearchClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Function}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction(@NotNull SelectParser.FunctionContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#ColLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColLiteral(@NotNull SelectParser.ColLiteralContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#load}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoad(@NotNull SelectParser.LoadContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#selectClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectClause(@NotNull SelectParser.SelectClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#colDef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColDef(@NotNull SelectParser.ColDefContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Table1Part}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable1Part(@NotNull SelectParser.Table1PartContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#createView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateView(@NotNull SelectParser.CreateViewContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#CrossJoin}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCrossJoin(@NotNull SelectParser.CrossJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#dropTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTable(@NotNull SelectParser.DropTableContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#commonTableExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommonTableExpression(@NotNull SelectParser.CommonTableExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#float2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloat2(@NotNull SelectParser.Float2Context ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#havingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHavingClause(@NotNull SelectParser.HavingClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#CountStar}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCountStar(@NotNull SelectParser.CountStarContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#fromClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromClause(@NotNull SelectParser.FromClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#CrossJoinP}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCrossJoinP(@NotNull SelectParser.CrossJoinPContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#AddSub}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddSub(@NotNull SelectParser.AddSubContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#fullSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFullSelect(@NotNull SelectParser.FullSelectContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#searchCondition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchCondition(@NotNull SelectParser.SearchConditionContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#CaseExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCaseExp(@NotNull SelectParser.CaseExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#deviceExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeviceExp(@NotNull SelectParser.DeviceExpContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#indexDef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIndexDef(@NotNull SelectParser.IndexDefContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#Join}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoin(@NotNull SelectParser.JoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#date2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDate2(@NotNull SelectParser.Date2Context ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#SelectExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectExpression(@NotNull SelectParser.SelectExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#colOrder}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColOrder(@NotNull SelectParser.ColOrderContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#selecthow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelecthow(@NotNull SelectParser.SelecthowContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#createIndex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateIndex(@NotNull SelectParser.CreateIndexContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#any}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAny(@NotNull SelectParser.AnyContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#subSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubSelect(@NotNull SelectParser.SubSelectContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#rangeSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeSet(@NotNull SelectParser.RangeSetContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#NullPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullPredicate(@NotNull SelectParser.NullPredicateContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#CountDistinct}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCountDistinct(@NotNull SelectParser.CountDistinctContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#remainder}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRemainder(@NotNull SelectParser.RemainderContext ctx);

	/**
	 * Visit a parse tree produced by {@link SelectParser#primaryKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimaryKey(@NotNull SelectParser.PrimaryKeyContext ctx);
}