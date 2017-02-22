// Generated from src/com/exascale/optimizer/Select.g4 by ANTLR 4.5.3
package com.exascale.optimizer;
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
	 * Visit a parse tree produced by {@link SelectParser#select}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect(SelectParser.SelectContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#runstats}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRunstats(SelectParser.RunstatsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#insert}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsert(SelectParser.InsertContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#valuesList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValuesList(SelectParser.ValuesListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#update}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdate(SelectParser.UpdateContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#setClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetClause(SelectParser.SetClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#delete}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDelete(SelectParser.DeleteContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#createTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTable(SelectParser.CreateTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#organization}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrganization(SelectParser.OrganizationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#colOrder}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColOrder(SelectParser.ColOrderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#groupExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupExp(SelectParser.GroupExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#realGroupExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealGroupExp(SelectParser.RealGroupExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#groupDef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupDef(SelectParser.GroupDefContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#rangeExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeExp(SelectParser.RangeExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#nodeExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeExp(SelectParser.NodeExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#realNodeExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealNodeExp(SelectParser.RealNodeExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#integerSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerSet(SelectParser.IntegerSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#hashExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHashExp(SelectParser.HashExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#columnSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnSet(SelectParser.ColumnSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#rangeType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeType(SelectParser.RangeTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#rangeSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRangeSet(SelectParser.RangeSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#deviceExp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeviceExp(SelectParser.DeviceExpContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#dropTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTable(SelectParser.DropTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#createView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateView(SelectParser.CreateViewContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#dropView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropView(SelectParser.DropViewContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#createIndex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateIndex(SelectParser.CreateIndexContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#dropIndex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropIndex(SelectParser.DropIndexContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#load}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoad(SelectParser.LoadContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#any}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAny(SelectParser.AnyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#remainder}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRemainder(SelectParser.RemainderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#indexDef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIndexDef(SelectParser.IndexDefContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#colDef}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColDef(SelectParser.ColDefContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#primaryKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimaryKey(SelectParser.PrimaryKeyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#notNull}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotNull(SelectParser.NotNullContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimary(SelectParser.PrimaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDataType(SelectParser.DataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#char2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitChar2(SelectParser.Char2Context ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#int2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInt2(SelectParser.Int2Context ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#long2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLong2(SelectParser.Long2Context ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#date2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDate2(SelectParser.Date2Context ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#float2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloat2(SelectParser.Float2Context ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#colList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColList(SelectParser.ColListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#commonTableExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommonTableExpression(SelectParser.CommonTableExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#fullSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFullSelect(SelectParser.FullSelectContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#connectedSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConnectedSelect(SelectParser.ConnectedSelectContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#subSelect}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubSelect(SelectParser.SubSelectContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#selectClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectClause(SelectParser.SelectClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#selecthow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelecthow(SelectParser.SelecthowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code SelectColumn}
	 * labeled alternative in {@link SelectParser#selectListEntry}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectColumn(SelectParser.SelectColumnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code SelectExpression}
	 * labeled alternative in {@link SelectParser#selectListEntry}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectExpression(SelectParser.SelectExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#fromClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromClause(SelectParser.FromClauseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code JoinP}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinP(SelectParser.JoinPContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NestedTable}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedTable(SelectParser.NestedTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code CrossJoinP}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCrossJoinP(SelectParser.CrossJoinPContext ctx);
	/**
	 * Visit a parse tree produced by the {@code IsSingleTable}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsSingleTable(SelectParser.IsSingleTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Join}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoin(SelectParser.JoinContext ctx);
	/**
	 * Visit a parse tree produced by the {@code CrossJoin}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCrossJoin(SelectParser.CrossJoinContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#singleTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleTable(SelectParser.SingleTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#whereClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereClause(SelectParser.WhereClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#groupBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupBy(SelectParser.GroupByContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#havingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHavingClause(SelectParser.HavingClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#orderBy}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderBy(SelectParser.OrderByContext ctx);
	/**
	 * Visit a parse tree produced by the {@code SortKeyInt}
	 * labeled alternative in {@link SelectParser#sortKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortKeyInt(SelectParser.SortKeyIntContext ctx);
	/**
	 * Visit a parse tree produced by the {@code SortKeyCol}
	 * labeled alternative in {@link SelectParser#sortKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortKeyCol(SelectParser.SortKeyColContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#correlationClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCorrelationClause(SelectParser.CorrelationClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#fetchFirst}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFetchFirst(SelectParser.FetchFirstContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Table1Part}
	 * labeled alternative in {@link SelectParser#tableName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable1Part(SelectParser.Table1PartContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Table2Part}
	 * labeled alternative in {@link SelectParser#tableName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable2Part(SelectParser.Table2PartContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Col1Part}
	 * labeled alternative in {@link SelectParser#columnName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCol1Part(SelectParser.Col1PartContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Col2Part}
	 * labeled alternative in {@link SelectParser#columnName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCol2Part(SelectParser.Col2PartContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#searchCondition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchCondition(SelectParser.SearchConditionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#connectedSearchClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConnectedSearchClause(SelectParser.ConnectedSearchClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#searchClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchClause(SelectParser.SearchClauseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NormalPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNormalPredicate(SelectParser.NormalPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NullPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullPredicate(SelectParser.NullPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ExistsPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExistsPredicate(SelectParser.ExistsPredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#operator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperator(SelectParser.OperatorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code CountStar}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCountStar(SelectParser.CountStarContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ExpSelect}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpSelect(SelectParser.ExpSelectContext ctx);
	/**
	 * Visit a parse tree produced by the {@code MulDiv}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMulDiv(SelectParser.MulDivContext ctx);
	/**
	 * Visit a parse tree produced by the {@code AddSub}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddSub(SelectParser.AddSubContext ctx);
	/**
	 * Visit a parse tree produced by the {@code IsLiteral}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIsLiteral(SelectParser.IsLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code CaseExp}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCaseExp(SelectParser.CaseExpContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NullExp}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullExp(SelectParser.NullExpContext ctx);
	/**
	 * Visit a parse tree produced by the {@code PExpression}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPExpression(SelectParser.PExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Concat}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcat(SelectParser.ConcatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Function}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunction(SelectParser.FunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ColLiteral}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColLiteral(SelectParser.ColLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code List}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitList(SelectParser.ListContext ctx);
	/**
	 * Visit a parse tree produced by the {@code CountDistinct}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCountDistinct(SelectParser.CountDistinctContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#caseCase}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCaseCase(SelectParser.CaseCaseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SelectParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(SelectParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NumericLiteral}
	 * labeled alternative in {@link SelectParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(SelectParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code StringLiteral}
	 * labeled alternative in {@link SelectParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(SelectParser.StringLiteralContext ctx);
}