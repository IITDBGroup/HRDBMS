// Generated from /home/michael/ms-project/Bitbucket/hrdbms/HRDBMS/src/com/exascale/optimizer/Select.g4 by ANTLR 4.6
package com.exascale.optimizer;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SelectParser}.
 */
public interface SelectListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SelectParser#select}.
	 * @param ctx the parse tree
	 */
	void enterSelect(SelectParser.SelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#select}.
	 * @param ctx the parse tree
	 */
	void exitSelect(SelectParser.SelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#runstats}.
	 * @param ctx the parse tree
	 */
	void enterRunstats(SelectParser.RunstatsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#runstats}.
	 * @param ctx the parse tree
	 */
	void exitRunstats(SelectParser.RunstatsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#insert}.
	 * @param ctx the parse tree
	 */
	void enterInsert(SelectParser.InsertContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#insert}.
	 * @param ctx the parse tree
	 */
	void exitInsert(SelectParser.InsertContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#update}.
	 * @param ctx the parse tree
	 */
	void enterUpdate(SelectParser.UpdateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#update}.
	 * @param ctx the parse tree
	 */
	void exitUpdate(SelectParser.UpdateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#delete}.
	 * @param ctx the parse tree
	 */
	void enterDelete(SelectParser.DeleteContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#delete}.
	 * @param ctx the parse tree
	 */
	void exitDelete(SelectParser.DeleteContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#createTable}.
	 * @param ctx the parse tree
	 */
	void enterCreateTable(SelectParser.CreateTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createTable}.
	 * @param ctx the parse tree
	 */
	void exitCreateTable(SelectParser.CreateTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#organization}.
	 * @param ctx the parse tree
	 */
	void enterOrganization(SelectParser.OrganizationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#organization}.
	 * @param ctx the parse tree
	 */
	void exitOrganization(SelectParser.OrganizationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#createExternalTable}.
	 * @param ctx the parse tree
	 */
	void enterCreateExternalTable(SelectParser.CreateExternalTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createExternalTable}.
	 * @param ctx the parse tree
	 */
	void exitCreateExternalTable(SelectParser.CreateExternalTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#generalExtTableSpec}.
	 * @param ctx the parse tree
	 */
	void enterGeneralExtTableSpec(SelectParser.GeneralExtTableSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#generalExtTableSpec}.
	 * @param ctx the parse tree
	 */
	void exitGeneralExtTableSpec(SelectParser.GeneralExtTableSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#javaClassExtTableSpec}.
	 * @param ctx the parse tree
	 */
	void enterJavaClassExtTableSpec(SelectParser.JavaClassExtTableSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#javaClassExtTableSpec}.
	 * @param ctx the parse tree
	 */
	void exitJavaClassExtTableSpec(SelectParser.JavaClassExtTableSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#javaClassName}.
	 * @param ctx the parse tree
	 */
	void enterJavaClassName(SelectParser.JavaClassNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#javaClassName}.
	 * @param ctx the parse tree
	 */
	void exitJavaClassName(SelectParser.JavaClassNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#keyValueList}.
	 * @param ctx the parse tree
	 */
	void enterKeyValueList(SelectParser.KeyValueListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#keyValueList}.
	 * @param ctx the parse tree
	 */
	void exitKeyValueList(SelectParser.KeyValueListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#anything}.
	 * @param ctx the parse tree
	 */
	void enterAnything(SelectParser.AnythingContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#anything}.
	 * @param ctx the parse tree
	 */
	void exitAnything(SelectParser.AnythingContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#sourceList}.
	 * @param ctx the parse tree
	 */
	void enterSourceList(SelectParser.SourceListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#sourceList}.
	 * @param ctx the parse tree
	 */
	void exitSourceList(SelectParser.SourceListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#colOrder}.
	 * @param ctx the parse tree
	 */
	void enterColOrder(SelectParser.ColOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#colOrder}.
	 * @param ctx the parse tree
	 */
	void exitColOrder(SelectParser.ColOrderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#groupExp}.
	 * @param ctx the parse tree
	 */
	void enterGroupExp(SelectParser.GroupExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#groupExp}.
	 * @param ctx the parse tree
	 */
	void exitGroupExp(SelectParser.GroupExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#realGroupExp}.
	 * @param ctx the parse tree
	 */
	void enterRealGroupExp(SelectParser.RealGroupExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#realGroupExp}.
	 * @param ctx the parse tree
	 */
	void exitRealGroupExp(SelectParser.RealGroupExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#groupDef}.
	 * @param ctx the parse tree
	 */
	void enterGroupDef(SelectParser.GroupDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#groupDef}.
	 * @param ctx the parse tree
	 */
	void exitGroupDef(SelectParser.GroupDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#rangeExp}.
	 * @param ctx the parse tree
	 */
	void enterRangeExp(SelectParser.RangeExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#rangeExp}.
	 * @param ctx the parse tree
	 */
	void exitRangeExp(SelectParser.RangeExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#nodeExp}.
	 * @param ctx the parse tree
	 */
	void enterNodeExp(SelectParser.NodeExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#nodeExp}.
	 * @param ctx the parse tree
	 */
	void exitNodeExp(SelectParser.NodeExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#realNodeExp}.
	 * @param ctx the parse tree
	 */
	void enterRealNodeExp(SelectParser.RealNodeExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#realNodeExp}.
	 * @param ctx the parse tree
	 */
	void exitRealNodeExp(SelectParser.RealNodeExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#integerSet}.
	 * @param ctx the parse tree
	 */
	void enterIntegerSet(SelectParser.IntegerSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#integerSet}.
	 * @param ctx the parse tree
	 */
	void exitIntegerSet(SelectParser.IntegerSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#hashExp}.
	 * @param ctx the parse tree
	 */
	void enterHashExp(SelectParser.HashExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#hashExp}.
	 * @param ctx the parse tree
	 */
	void exitHashExp(SelectParser.HashExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#columnSet}.
	 * @param ctx the parse tree
	 */
	void enterColumnSet(SelectParser.ColumnSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#columnSet}.
	 * @param ctx the parse tree
	 */
	void exitColumnSet(SelectParser.ColumnSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#rangeType}.
	 * @param ctx the parse tree
	 */
	void enterRangeType(SelectParser.RangeTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#rangeType}.
	 * @param ctx the parse tree
	 */
	void exitRangeType(SelectParser.RangeTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#rangeSet}.
	 * @param ctx the parse tree
	 */
	void enterRangeSet(SelectParser.RangeSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#rangeSet}.
	 * @param ctx the parse tree
	 */
	void exitRangeSet(SelectParser.RangeSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#deviceExp}.
	 * @param ctx the parse tree
	 */
	void enterDeviceExp(SelectParser.DeviceExpContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#deviceExp}.
	 * @param ctx the parse tree
	 */
	void exitDeviceExp(SelectParser.DeviceExpContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#dropTable}.
	 * @param ctx the parse tree
	 */
	void enterDropTable(SelectParser.DropTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dropTable}.
	 * @param ctx the parse tree
	 */
	void exitDropTable(SelectParser.DropTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#createView}.
	 * @param ctx the parse tree
	 */
	void enterCreateView(SelectParser.CreateViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createView}.
	 * @param ctx the parse tree
	 */
	void exitCreateView(SelectParser.CreateViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#dropView}.
	 * @param ctx the parse tree
	 */
	void enterDropView(SelectParser.DropViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dropView}.
	 * @param ctx the parse tree
	 */
	void exitDropView(SelectParser.DropViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#createIndex}.
	 * @param ctx the parse tree
	 */
	void enterCreateIndex(SelectParser.CreateIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#createIndex}.
	 * @param ctx the parse tree
	 */
	void exitCreateIndex(SelectParser.CreateIndexContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#dropIndex}.
	 * @param ctx the parse tree
	 */
	void enterDropIndex(SelectParser.DropIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dropIndex}.
	 * @param ctx the parse tree
	 */
	void exitDropIndex(SelectParser.DropIndexContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#load}.
	 * @param ctx the parse tree
	 */
	void enterLoad(SelectParser.LoadContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#load}.
	 * @param ctx the parse tree
	 */
	void exitLoad(SelectParser.LoadContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#any}.
	 * @param ctx the parse tree
	 */
	void enterAny(SelectParser.AnyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#any}.
	 * @param ctx the parse tree
	 */
	void exitAny(SelectParser.AnyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#remainder}.
	 * @param ctx the parse tree
	 */
	void enterRemainder(SelectParser.RemainderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#remainder}.
	 * @param ctx the parse tree
	 */
	void exitRemainder(SelectParser.RemainderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#indexDef}.
	 * @param ctx the parse tree
	 */
	void enterIndexDef(SelectParser.IndexDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#indexDef}.
	 * @param ctx the parse tree
	 */
	void exitIndexDef(SelectParser.IndexDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#colDef}.
	 * @param ctx the parse tree
	 */
	void enterColDef(SelectParser.ColDefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#colDef}.
	 * @param ctx the parse tree
	 */
	void exitColDef(SelectParser.ColDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#primaryKey}.
	 * @param ctx the parse tree
	 */
	void enterPrimaryKey(SelectParser.PrimaryKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#primaryKey}.
	 * @param ctx the parse tree
	 */
	void exitPrimaryKey(SelectParser.PrimaryKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#notNull}.
	 * @param ctx the parse tree
	 */
	void enterNotNull(SelectParser.NotNullContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#notNull}.
	 * @param ctx the parse tree
	 */
	void exitNotNull(SelectParser.NotNullContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#primary}.
	 * @param ctx the parse tree
	 */
	void enterPrimary(SelectParser.PrimaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#primary}.
	 * @param ctx the parse tree
	 */
	void exitPrimary(SelectParser.PrimaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterDataType(SelectParser.DataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitDataType(SelectParser.DataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#char2}.
	 * @param ctx the parse tree
	 */
	void enterChar2(SelectParser.Char2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#char2}.
	 * @param ctx the parse tree
	 */
	void exitChar2(SelectParser.Char2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#int2}.
	 * @param ctx the parse tree
	 */
	void enterInt2(SelectParser.Int2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#int2}.
	 * @param ctx the parse tree
	 */
	void exitInt2(SelectParser.Int2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#long2}.
	 * @param ctx the parse tree
	 */
	void enterLong2(SelectParser.Long2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#long2}.
	 * @param ctx the parse tree
	 */
	void exitLong2(SelectParser.Long2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#date2}.
	 * @param ctx the parse tree
	 */
	void enterDate2(SelectParser.Date2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#date2}.
	 * @param ctx the parse tree
	 */
	void exitDate2(SelectParser.Date2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#float2}.
	 * @param ctx the parse tree
	 */
	void enterFloat2(SelectParser.Float2Context ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#float2}.
	 * @param ctx the parse tree
	 */
	void exitFloat2(SelectParser.Float2Context ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#colList}.
	 * @param ctx the parse tree
	 */
	void enterColList(SelectParser.ColListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#colList}.
	 * @param ctx the parse tree
	 */
	void exitColList(SelectParser.ColListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#commonTableExpression}.
	 * @param ctx the parse tree
	 */
	void enterCommonTableExpression(SelectParser.CommonTableExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#commonTableExpression}.
	 * @param ctx the parse tree
	 */
	void exitCommonTableExpression(SelectParser.CommonTableExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#fullSelect}.
	 * @param ctx the parse tree
	 */
	void enterFullSelect(SelectParser.FullSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#fullSelect}.
	 * @param ctx the parse tree
	 */
	void exitFullSelect(SelectParser.FullSelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#connectedSelect}.
	 * @param ctx the parse tree
	 */
	void enterConnectedSelect(SelectParser.ConnectedSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#connectedSelect}.
	 * @param ctx the parse tree
	 */
	void exitConnectedSelect(SelectParser.ConnectedSelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#subSelect}.
	 * @param ctx the parse tree
	 */
	void enterSubSelect(SelectParser.SubSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#subSelect}.
	 * @param ctx the parse tree
	 */
	void exitSubSelect(SelectParser.SubSelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(SelectParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(SelectParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#selecthow}.
	 * @param ctx the parse tree
	 */
	void enterSelecthow(SelectParser.SelecthowContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#selecthow}.
	 * @param ctx the parse tree
	 */
	void exitSelecthow(SelectParser.SelecthowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code SelectColumn}
	 * labeled alternative in {@link SelectParser#selectListEntry}.
	 * @param ctx the parse tree
	 */
	void enterSelectColumn(SelectParser.SelectColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code SelectColumn}
	 * labeled alternative in {@link SelectParser#selectListEntry}.
	 * @param ctx the parse tree
	 */
	void exitSelectColumn(SelectParser.SelectColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code SelectExpression}
	 * labeled alternative in {@link SelectParser#selectListEntry}.
	 * @param ctx the parse tree
	 */
	void enterSelectExpression(SelectParser.SelectExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code SelectExpression}
	 * labeled alternative in {@link SelectParser#selectListEntry}.
	 * @param ctx the parse tree
	 */
	void exitSelectExpression(SelectParser.SelectExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(SelectParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(SelectParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code JoinP}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void enterJoinP(SelectParser.JoinPContext ctx);
	/**
	 * Exit a parse tree produced by the {@code JoinP}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void exitJoinP(SelectParser.JoinPContext ctx);
	/**
	 * Enter a parse tree produced by the {@code NestedTable}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void enterNestedTable(SelectParser.NestedTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code NestedTable}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void exitNestedTable(SelectParser.NestedTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code CrossJoinP}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void enterCrossJoinP(SelectParser.CrossJoinPContext ctx);
	/**
	 * Exit a parse tree produced by the {@code CrossJoinP}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void exitCrossJoinP(SelectParser.CrossJoinPContext ctx);
	/**
	 * Enter a parse tree produced by the {@code IsSingleTable}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void enterIsSingleTable(SelectParser.IsSingleTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code IsSingleTable}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void exitIsSingleTable(SelectParser.IsSingleTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Join}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void enterJoin(SelectParser.JoinContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Join}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void exitJoin(SelectParser.JoinContext ctx);
	/**
	 * Enter a parse tree produced by the {@code CrossJoin}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void enterCrossJoin(SelectParser.CrossJoinContext ctx);
	/**
	 * Exit a parse tree produced by the {@code CrossJoin}
	 * labeled alternative in {@link SelectParser#tableReference}.
	 * @param ctx the parse tree
	 */
	void exitCrossJoin(SelectParser.CrossJoinContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#singleTable}.
	 * @param ctx the parse tree
	 */
	void enterSingleTable(SelectParser.SingleTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#singleTable}.
	 * @param ctx the parse tree
	 */
	void exitSingleTable(SelectParser.SingleTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(SelectParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(SelectParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void enterGroupBy(SelectParser.GroupByContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#groupBy}.
	 * @param ctx the parse tree
	 */
	void exitGroupBy(SelectParser.GroupByContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(SelectParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(SelectParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#orderBy}.
	 * @param ctx the parse tree
	 */
	void enterOrderBy(SelectParser.OrderByContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#orderBy}.
	 * @param ctx the parse tree
	 */
	void exitOrderBy(SelectParser.OrderByContext ctx);
	/**
	 * Enter a parse tree produced by the {@code SortKeyInt}
	 * labeled alternative in {@link SelectParser#sortKey}.
	 * @param ctx the parse tree
	 */
	void enterSortKeyInt(SelectParser.SortKeyIntContext ctx);
	/**
	 * Exit a parse tree produced by the {@code SortKeyInt}
	 * labeled alternative in {@link SelectParser#sortKey}.
	 * @param ctx the parse tree
	 */
	void exitSortKeyInt(SelectParser.SortKeyIntContext ctx);
	/**
	 * Enter a parse tree produced by the {@code SortKeyCol}
	 * labeled alternative in {@link SelectParser#sortKey}.
	 * @param ctx the parse tree
	 */
	void enterSortKeyCol(SelectParser.SortKeyColContext ctx);
	/**
	 * Exit a parse tree produced by the {@code SortKeyCol}
	 * labeled alternative in {@link SelectParser#sortKey}.
	 * @param ctx the parse tree
	 */
	void exitSortKeyCol(SelectParser.SortKeyColContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#correlationClause}.
	 * @param ctx the parse tree
	 */
	void enterCorrelationClause(SelectParser.CorrelationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#correlationClause}.
	 * @param ctx the parse tree
	 */
	void exitCorrelationClause(SelectParser.CorrelationClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#fetchFirst}.
	 * @param ctx the parse tree
	 */
	void enterFetchFirst(SelectParser.FetchFirstContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#fetchFirst}.
	 * @param ctx the parse tree
	 */
	void exitFetchFirst(SelectParser.FetchFirstContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Table1Part}
	 * labeled alternative in {@link SelectParser#tableName}.
	 * @param ctx the parse tree
	 */
	void enterTable1Part(SelectParser.Table1PartContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Table1Part}
	 * labeled alternative in {@link SelectParser#tableName}.
	 * @param ctx the parse tree
	 */
	void exitTable1Part(SelectParser.Table1PartContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Table2Part}
	 * labeled alternative in {@link SelectParser#tableName}.
	 * @param ctx the parse tree
	 */
	void enterTable2Part(SelectParser.Table2PartContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Table2Part}
	 * labeled alternative in {@link SelectParser#tableName}.
	 * @param ctx the parse tree
	 */
	void exitTable2Part(SelectParser.Table2PartContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Col1Part}
	 * labeled alternative in {@link SelectParser#columnName}.
	 * @param ctx the parse tree
	 */
	void enterCol1Part(SelectParser.Col1PartContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Col1Part}
	 * labeled alternative in {@link SelectParser#columnName}.
	 * @param ctx the parse tree
	 */
	void exitCol1Part(SelectParser.Col1PartContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Col2Part}
	 * labeled alternative in {@link SelectParser#columnName}.
	 * @param ctx the parse tree
	 */
	void enterCol2Part(SelectParser.Col2PartContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Col2Part}
	 * labeled alternative in {@link SelectParser#columnName}.
	 * @param ctx the parse tree
	 */
	void exitCol2Part(SelectParser.Col2PartContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#searchCondition}.
	 * @param ctx the parse tree
	 */
	void enterSearchCondition(SelectParser.SearchConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#searchCondition}.
	 * @param ctx the parse tree
	 */
	void exitSearchCondition(SelectParser.SearchConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#connectedSearchClause}.
	 * @param ctx the parse tree
	 */
	void enterConnectedSearchClause(SelectParser.ConnectedSearchClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#connectedSearchClause}.
	 * @param ctx the parse tree
	 */
	void exitConnectedSearchClause(SelectParser.ConnectedSearchClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#searchClause}.
	 * @param ctx the parse tree
	 */
	void enterSearchClause(SelectParser.SearchClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#searchClause}.
	 * @param ctx the parse tree
	 */
	void exitSearchClause(SelectParser.SearchClauseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code NormalPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterNormalPredicate(SelectParser.NormalPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code NormalPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitNormalPredicate(SelectParser.NormalPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code NullPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterNullPredicate(SelectParser.NullPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code NullPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitNullPredicate(SelectParser.NullPredicateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ExistsPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterExistsPredicate(SelectParser.ExistsPredicateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ExistsPredicate}
	 * labeled alternative in {@link SelectParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitExistsPredicate(SelectParser.ExistsPredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#operator}.
	 * @param ctx the parse tree
	 */
	void enterOperator(SelectParser.OperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#operator}.
	 * @param ctx the parse tree
	 */
	void exitOperator(SelectParser.OperatorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code CountStar}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterCountStar(SelectParser.CountStarContext ctx);
	/**
	 * Exit a parse tree produced by the {@code CountStar}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitCountStar(SelectParser.CountStarContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ExpSelect}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpSelect(SelectParser.ExpSelectContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ExpSelect}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpSelect(SelectParser.ExpSelectContext ctx);
	/**
	 * Enter a parse tree produced by the {@code MulDiv}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterMulDiv(SelectParser.MulDivContext ctx);
	/**
	 * Exit a parse tree produced by the {@code MulDiv}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitMulDiv(SelectParser.MulDivContext ctx);
	/**
	 * Enter a parse tree produced by the {@code AddSub}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterAddSub(SelectParser.AddSubContext ctx);
	/**
	 * Exit a parse tree produced by the {@code AddSub}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitAddSub(SelectParser.AddSubContext ctx);
	/**
	 * Enter a parse tree produced by the {@code IsLiteral}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterIsLiteral(SelectParser.IsLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code IsLiteral}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitIsLiteral(SelectParser.IsLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code CaseExp}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterCaseExp(SelectParser.CaseExpContext ctx);
	/**
	 * Exit a parse tree produced by the {@code CaseExp}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitCaseExp(SelectParser.CaseExpContext ctx);
	/**
	 * Enter a parse tree produced by the {@code NullExp}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterNullExp(SelectParser.NullExpContext ctx);
	/**
	 * Exit a parse tree produced by the {@code NullExp}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitNullExp(SelectParser.NullExpContext ctx);
	/**
	 * Enter a parse tree produced by the {@code PExpression}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterPExpression(SelectParser.PExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code PExpression}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitPExpression(SelectParser.PExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Concat}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterConcat(SelectParser.ConcatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Concat}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitConcat(SelectParser.ConcatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Function}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterFunction(SelectParser.FunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Function}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitFunction(SelectParser.FunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ColLiteral}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterColLiteral(SelectParser.ColLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ColLiteral}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitColLiteral(SelectParser.ColLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code List}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterList(SelectParser.ListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code List}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitList(SelectParser.ListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code CountDistinct}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterCountDistinct(SelectParser.CountDistinctContext ctx);
	/**
	 * Exit a parse tree produced by the {@code CountDistinct}
	 * labeled alternative in {@link SelectParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitCountDistinct(SelectParser.CountDistinctContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#caseCase}.
	 * @param ctx the parse tree
	 */
	void enterCaseCase(SelectParser.CaseCaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#caseCase}.
	 * @param ctx the parse tree
	 */
	void exitCaseCase(SelectParser.CaseCaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SelectParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(SelectParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SelectParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(SelectParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code NumericLiteral}
	 * labeled alternative in {@link SelectParser#literal}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(SelectParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code NumericLiteral}
	 * labeled alternative in {@link SelectParser#literal}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(SelectParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code StringLiteral}
	 * labeled alternative in {@link SelectParser#literal}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(SelectParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code StringLiteral}
	 * labeled alternative in {@link SelectParser#literal}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(SelectParser.StringLiteralContext ctx);
}