// Generated from Select.g4 by ANTLR 4.4
package com.exascale.optimizer;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SelectParser extends Parser {
	//static { RuntimeMetaData.checkVersion("4.4", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__65=1, T__64=2, T__63=3, T__62=4, T__61=5, T__60=6, T__59=7, T__58=8, 
		T__57=9, T__56=10, T__55=11, T__54=12, T__53=13, T__52=14, T__51=15, T__50=16, 
		T__49=17, T__48=18, T__47=19, T__46=20, T__45=21, T__44=22, T__43=23, 
		T__42=24, T__41=25, T__40=26, T__39=27, T__38=28, T__37=29, T__36=30, 
		T__35=31, T__34=32, T__33=33, T__32=34, T__31=35, T__30=36, T__29=37, 
		T__28=38, T__27=39, T__26=40, T__25=41, T__24=42, T__23=43, T__22=44, 
		T__21=45, T__20=46, T__19=47, T__18=48, T__17=49, T__16=50, T__15=51, 
		T__14=52, T__13=53, T__12=54, T__11=55, T__10=56, T__9=57, T__8=58, T__7=59, 
		T__6=60, T__5=61, T__4=62, T__3=63, T__2=64, T__1=65, T__0=66, STRING=67, 
		STAR=68, COUNT=69, CONCAT=70, NEGATIVE=71, EQUALS=72, OPERATOR=73, NULLOPERATOR=74, 
		AND=75, OR=76, NOT=77, NULL=78, DIRECTION=79, JOINTYPE=80, CROSSJOIN=81, 
		TABLECOMBINATION=82, COLUMN=83, DISTINCT=84, INTEGER=85, WS=86, UNIQUE=87, 
		REPLACE=88, RESUME=89, NONE=90, ALL=91, ANYTEXT=92, HASH=93, RANGE=94, 
		DATE=95, COLORDER=96, IDENTIFIER=97, JAVACLASSNAMEIDENTIFIER=98, FILEPATHIDENTIFIER=99, 
		ANY=100;
	public static final String[] tokenNames = {
		"<INVALID>", "'DOUBLE'", "'INTEGER'", "'IMPORT'", "'FROM'", "'USING'", 
		"'EXISTS'", "'{'", "'PARAMETERS'", "'FILE'", "'GROUP'", "'CASE'", "'('", 
		"','", "'PRIMARY'", "'DELIMITER'", "'LOAD'", "'VALUES'", "'VARCHAR'", 
		"'UPDATE'", "'DELETE'", "'BIGINT'", "'FIRST'", "'FETCH'", "'HAVING'", 
		"'PATH'", "'INSERT'", "'FIELDS'", "'+'", "'CREATE'", "'hdfs'", "'/'", 
		"'ONLY'", "'TABLE'", "'AS'", "'BY'", "'ELSE'", "'WHERE'", "'INTO'", "'END'", 
		"'ON'", "'JOIN'", "'}'", "'VIEW'", "'THEN'", "'.java'", "'local'", "'KEY'", 
		"'ORDER'", "'SELECT'", "'WITH'", "'DROP'", "'.'", "'WHEN'", "'ROW'", "'CHAR'", 
		"':'", "'INDEX'", "'s3'", "'|'", "'ROWS'", "'EXTERNAL'", "'RUNSTATS'", 
		"'DELIMITED'", "'FLOAT'", "')'", "'SET'", "STRING", "'*'", "'COUNT'", 
		"'||'", "'-'", "'='", "OPERATOR", "NULLOPERATOR", "'AND'", "'OR'", "'NOT'", 
		"'NULL'", "DIRECTION", "JOINTYPE", "'CROSS JOIN'", "TABLECOMBINATION", 
		"'COLUMN'", "'DISTINCT'", "INTEGER", "WS", "'UNIQUE'", "'REPLACE'", "'RESUME'", 
		"'NONE'", "'ALL'", "'ANY'", "'HASH'", "'RANGE'", "'DATE'", "'COLORDER'", 
		"IDENTIFIER", "JAVACLASSNAMEIDENTIFIER", "FILEPATHIDENTIFIER", "ANY"
	};
	public static final int
		RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_update = 3, 
		RULE_delete = 4, RULE_createTable = 5, RULE_createExternalTable = 6, RULE_generalExtTableSpec = 7, 
		RULE_javaClassExtTableSpec = 8, RULE_javaClassName = 9, RULE_keyValueList = 10, 
		RULE_anything = 11, RULE_sourceList = 12, RULE_colOrder = 13, RULE_groupExp = 14, 
		RULE_realGroupExp = 15, RULE_groupDef = 16, RULE_rangeExp = 17, RULE_nodeExp = 18, 
		RULE_realNodeExp = 19, RULE_integerSet = 20, RULE_hashExp = 21, RULE_columnSet = 22, 
		RULE_rangeType = 23, RULE_rangeSet = 24, RULE_deviceExp = 25, RULE_dropTable = 26, 
		RULE_createView = 27, RULE_dropView = 28, RULE_createIndex = 29, RULE_dropIndex = 30, 
		RULE_load = 31, RULE_any = 32, RULE_remainder = 33, RULE_indexDef = 34, 
		RULE_colDef = 35, RULE_primaryKey = 36, RULE_notNull = 37, RULE_primary = 38, 
		RULE_dataType = 39, RULE_char2 = 40, RULE_int2 = 41, RULE_long2 = 42, 
		RULE_date2 = 43, RULE_float2 = 44, RULE_colList = 45, RULE_commonTableExpression = 46, 
		RULE_fullSelect = 47, RULE_connectedSelect = 48, RULE_subSelect = 49, 
		RULE_selectClause = 50, RULE_selecthow = 51, RULE_selectListEntry = 52, 
		RULE_fromClause = 53, RULE_tableReference = 54, RULE_singleTable = 55, 
		RULE_whereClause = 56, RULE_groupBy = 57, RULE_havingClause = 58, RULE_orderBy = 59, 
		RULE_sortKey = 60, RULE_correlationClause = 61, RULE_fetchFirst = 62, 
		RULE_tableName = 63, RULE_columnName = 64, RULE_searchCondition = 65, 
		RULE_connectedSearchClause = 66, RULE_searchClause = 67, RULE_predicate = 68, 
		RULE_operator = 69, RULE_expression = 70, RULE_caseCase = 71, RULE_identifier = 72, 
		RULE_literal = 73;
	public static final String[] ruleNames = {
		"select", "runstats", "insert", "update", "delete", "createTable", "createExternalTable", 
		"generalExtTableSpec", "javaClassExtTableSpec", "javaClassName", "keyValueList", 
		"anything", "sourceList", "colOrder", "groupExp", "realGroupExp", "groupDef", 
		"rangeExp", "nodeExp", "realNodeExp", "integerSet", "hashExp", "columnSet", 
		"rangeType", "rangeSet", "deviceExp", "dropTable", "createView", "dropView", 
		"createIndex", "dropIndex", "load", "any", "remainder", "indexDef", "colDef", 
		"primaryKey", "notNull", "primary", "dataType", "char2", "int2", "long2", 
		"date2", "float2", "colList", "commonTableExpression", "fullSelect", "connectedSelect", 
		"subSelect", "selectClause", "selecthow", "selectListEntry", "fromClause", 
		"tableReference", "singleTable", "whereClause", "groupBy", "havingClause", 
		"orderBy", "sortKey", "correlationClause", "fetchFirst", "tableName", 
		"columnName", "searchCondition", "connectedSearchClause", "searchClause", 
		"predicate", "operator", "expression", "caseCase", "identifier", "literal"
	};

	@Override
	public String getGrammarFileName() { return "Select.g4"; }

	@Override
	public String[] getTokenNames() { return tokenNames; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SelectParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class SelectContext extends ParserRuleContext {
		public RunstatsContext runstats() {
			return getRuleContext(RunstatsContext.class,0);
		}
		public CreateIndexContext createIndex() {
			return getRuleContext(CreateIndexContext.class,0);
		}
		public InsertContext insert() {
			return getRuleContext(InsertContext.class,0);
		}
		public CreateExternalTableContext createExternalTable() {
			return getRuleContext(CreateExternalTableContext.class,0);
		}
		public CreateTableContext createTable() {
			return getRuleContext(CreateTableContext.class,0);
		}
		public CommonTableExpressionContext commonTableExpression(int i) {
			return getRuleContext(CommonTableExpressionContext.class,i);
		}
		public DeleteContext delete() {
			return getRuleContext(DeleteContext.class,0);
		}
		public List<CommonTableExpressionContext> commonTableExpression() {
			return getRuleContexts(CommonTableExpressionContext.class);
		}
		public DropViewContext dropView() {
			return getRuleContext(DropViewContext.class,0);
		}
		public CreateViewContext createView() {
			return getRuleContext(CreateViewContext.class,0);
		}
		public DropTableContext dropTable() {
			return getRuleContext(DropTableContext.class,0);
		}
		public UpdateContext update() {
			return getRuleContext(UpdateContext.class,0);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public DropIndexContext dropIndex() {
			return getRuleContext(DropIndexContext.class,0);
		}
		public LoadContext load() {
			return getRuleContext(LoadContext.class,0);
		}
		public SelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSelect(this);
		}
	}

	public final SelectContext select() throws RecognitionException {
		SelectContext _localctx = new SelectContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_select);
		int _la;
		try {
			setState(172);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(148); insert();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(149); update();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(150); delete();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(151); createTable();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(152); createExternalTable();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(153); createIndex();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(154); createView();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(155); dropTable();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(156); dropIndex();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(157); dropView();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(158); load();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(159); runstats();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				{
				setState(169);
				_la = _input.LA(1);
				if (_la==T__16) {
					{
					setState(160); match(T__16);
					setState(161); commonTableExpression();
					setState(166);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__53) {
						{
						{
						setState(162); match(T__53);
						setState(163); commonTableExpression();
						}
						}
						setState(168);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(171); fullSelect();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RunstatsContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public RunstatsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_runstats; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRunstats(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRunstats(this);
		}
	}

	public final RunstatsContext runstats() throws RecognitionException {
		RunstatsContext _localctx = new RunstatsContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_runstats);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(174); match(T__4);
			setState(175); match(T__26);
			setState(176); tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public InsertContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterInsert(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitInsert(this);
		}
	}

	public final InsertContext insert() throws RecognitionException {
		InsertContext _localctx = new InsertContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_insert);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(178); match(T__40);
			setState(179); match(T__28);
			setState(180); tableName();
			setState(197);
			switch (_input.LA(1)) {
			case T__62:
			case T__54:
			case T__17:
				{
				{
				setState(182);
				_la = _input.LA(1);
				if (_la==T__62) {
					{
					setState(181); match(T__62);
					}
				}

				setState(184); fullSelect();
				}
				}
				break;
			case T__49:
				{
				{
				setState(185); match(T__49);
				setState(186); match(T__54);
				setState(187); expression(0);
				setState(192);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__53) {
					{
					{
					setState(188); match(T__53);
					setState(189); expression(0);
					}
					}
					setState(194);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(195); match(T__1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateContext extends ParserRuleContext {
		public TerminalNode EQUALS() { return getToken(SelectParser.EQUALS, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public ColListContext colList() {
			return getRuleContext(ColListContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public UpdateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterUpdate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitUpdate(this);
		}
	}

	public final UpdateContext update() throws RecognitionException {
		UpdateContext _localctx = new UpdateContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_update);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(199); match(T__47);
			setState(200); tableName();
			setState(201); match(T__0);
			setState(204);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(202); columnName();
				}
				break;
			case T__54:
				{
				setState(203); colList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(206); match(EQUALS);
			setState(207); expression(0);
			setState(209);
			_la = _input.LA(1);
			if (_la==T__29) {
				{
				setState(208); whereClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DeleteContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public DeleteContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDelete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDelete(this);
		}
	}

	public final DeleteContext delete() throws RecognitionException {
		DeleteContext _localctx = new DeleteContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_delete);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(211); match(T__46);
			setState(212); match(T__62);
			setState(213); tableName();
			setState(215);
			_la = _input.LA(1);
			if (_la==T__29) {
				{
				setState(214); whereClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableContext extends ParserRuleContext {
		public TerminalNode COLUMN() { return getToken(SelectParser.COLUMN, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public PrimaryKeyContext primaryKey() {
			return getRuleContext(PrimaryKeyContext.class,0);
		}
		public ColOrderContext colOrder() {
			return getRuleContext(ColOrderContext.class,0);
		}
		public List<ColDefContext> colDef() {
			return getRuleContexts(ColDefContext.class);
		}
		public ColDefContext colDef(int i) {
			return getRuleContext(ColDefContext.class,i);
		}
		public DeviceExpContext deviceExp() {
			return getRuleContext(DeviceExpContext.class,0);
		}
		public GroupExpContext groupExp() {
			return getRuleContext(GroupExpContext.class,0);
		}
		public NodeExpContext nodeExp() {
			return getRuleContext(NodeExpContext.class,0);
		}
		public CreateTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCreateTable(this);
		}
	}

	public final CreateTableContext createTable() throws RecognitionException {
		CreateTableContext _localctx = new CreateTableContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_createTable);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(217); match(T__37);
			setState(219);
			_la = _input.LA(1);
			if (_la==COLUMN) {
				{
				setState(218); match(COLUMN);
				}
			}

			setState(221); match(T__33);
			setState(222); tableName();
			setState(223); match(T__54);
			setState(224); colDef();
			setState(229);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(225); match(T__53);
					setState(226); colDef();
					}
					} 
				}
				setState(231);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			}
			setState(234);
			_la = _input.LA(1);
			if (_la==T__53) {
				{
				setState(232); match(T__53);
				setState(233); primaryKey();
				}
			}

			setState(236); match(T__1);
			setState(238);
			_la = _input.LA(1);
			if (_la==COLORDER) {
				{
				setState(237); colOrder();
				}
			}

			setState(241);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				setState(240); groupExp();
				}
				break;
			}
			setState(243); nodeExp();
			setState(244); deviceExp();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateExternalTableContext extends ParserRuleContext {
		public JavaClassExtTableSpecContext javaClassExtTableSpec() {
			return getRuleContext(JavaClassExtTableSpecContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public List<ColDefContext> colDef() {
			return getRuleContexts(ColDefContext.class);
		}
		public ColDefContext colDef(int i) {
			return getRuleContext(ColDefContext.class,i);
		}
		public GeneralExtTableSpecContext generalExtTableSpec() {
			return getRuleContext(GeneralExtTableSpecContext.class,0);
		}
		public CreateExternalTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createExternalTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCreateExternalTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCreateExternalTable(this);
		}
	}

	public final CreateExternalTableContext createExternalTable() throws RecognitionException {
		CreateExternalTableContext _localctx = new CreateExternalTableContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_createExternalTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(246); match(T__37);
			setState(247); match(T__5);
			setState(248); match(T__33);
			setState(249); tableName();
			setState(250); match(T__54);
			setState(251); colDef();
			setState(256);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(252); match(T__53);
				setState(253); colDef();
				}
				}
				setState(258);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(259); match(T__1);
			setState(262);
			switch (_input.LA(1)) {
			case T__63:
				{
				setState(260); generalExtTableSpec();
				}
				break;
			case T__61:
				{
				setState(261); javaClassExtTableSpec();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GeneralExtTableSpecContext extends ParserRuleContext {
		public List<AnythingContext> anything() {
			return getRuleContexts(AnythingContext.class);
		}
		public SourceListContext sourceList() {
			return getRuleContext(SourceListContext.class,0);
		}
		public TerminalNode FILEPATHIDENTIFIER() { return getToken(SelectParser.FILEPATHIDENTIFIER, 0); }
		public AnythingContext anything(int i) {
			return getRuleContext(AnythingContext.class,i);
		}
		public GeneralExtTableSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_generalExtTableSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterGeneralExtTableSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitGeneralExtTableSpec(this);
		}
	}

	public final GeneralExtTableSpecContext generalExtTableSpec() throws RecognitionException {
		GeneralExtTableSpecContext _localctx = new GeneralExtTableSpecContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_generalExtTableSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(264); match(T__63);
			setState(265); match(T__62);
			setState(266); sourceList();
			setState(267); match(T__39);
			setState(268); match(T__3);
			setState(269); match(T__31);
			setState(270); anything();
			setState(271); match(T__6);
			setState(272); match(T__3);
			setState(273); match(T__31);
			setState(274); anything();
			setState(275); match(T__57);
			setState(276); match(T__41);
			setState(277); match(FILEPATHIDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JavaClassExtTableSpecContext extends ParserRuleContext {
		public KeyValueListContext keyValueList() {
			return getRuleContext(KeyValueListContext.class,0);
		}
		public JavaClassNameContext javaClassName() {
			return getRuleContext(JavaClassNameContext.class,0);
		}
		public JavaClassExtTableSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_javaClassExtTableSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterJavaClassExtTableSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitJavaClassExtTableSpec(this);
		}
	}

	public final JavaClassExtTableSpecContext javaClassExtTableSpec() throws RecognitionException {
		JavaClassExtTableSpecContext _localctx = new JavaClassExtTableSpecContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_javaClassExtTableSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(279); match(T__61);
			setState(280); javaClassName();
			setState(281); match(T__16);
			setState(282); match(T__58);
			setState(283); match(T__54);
			setState(284); keyValueList();
			setState(285); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JavaClassNameContext extends ParserRuleContext {
		public List<TerminalNode> JAVACLASSNAMEIDENTIFIER() { return getTokens(SelectParser.JAVACLASSNAMEIDENTIFIER); }
		public TerminalNode JAVACLASSNAMEIDENTIFIER(int i) {
			return getToken(SelectParser.JAVACLASSNAMEIDENTIFIER, i);
		}
		public JavaClassNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_javaClassName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterJavaClassName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitJavaClassName(this);
		}
	}

	public final JavaClassNameContext javaClassName() throws RecognitionException {
		JavaClassNameContext _localctx = new JavaClassNameContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_javaClassName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287); match(JAVACLASSNAMEIDENTIFIER);
			setState(292);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__14) {
				{
				{
				setState(288); match(T__14);
				setState(289); match(JAVACLASSNAMEIDENTIFIER);
				}
				}
				setState(294);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(295); match(T__21);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeyValueListContext extends ParserRuleContext {
		public List<AnythingContext> anything() {
			return getRuleContexts(AnythingContext.class);
		}
		public AnythingContext anything(int i) {
			return getRuleContext(AnythingContext.class,i);
		}
		public KeyValueListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyValueList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterKeyValueList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitKeyValueList(this);
		}
	}

	public final KeyValueListContext keyValueList() throws RecognitionException {
		KeyValueListContext _localctx = new KeyValueListContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_keyValueList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(297); anything();
			setState(298); match(T__10);
			setState(299); anything();
			setState(307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(300); match(T__53);
				setState(301); anything();
				setState(302); match(T__10);
				setState(303); anything();
				}
				}
				setState(309);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnythingContext extends ParserRuleContext {
		public AnythingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anything; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterAnything(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitAnything(this);
		}
	}

	public final AnythingContext anything() throws RecognitionException {
		AnythingContext _localctx = new AnythingContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_anything);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(310);
			matchWildcard();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceListContext extends ParserRuleContext {
		public SourceListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSourceList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSourceList(this);
		}
	}

	public final SourceListContext sourceList() throws RecognitionException {
		SourceListContext _localctx = new SourceListContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_sourceList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(312);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__36) | (1L << T__20) | (1L << T__8))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColOrderContext extends ParserRuleContext {
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode COLORDER() { return getToken(SelectParser.COLORDER, 0); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public ColOrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colOrder; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterColOrder(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitColOrder(this);
		}
	}

	public final ColOrderContext colOrder() throws RecognitionException {
		ColOrderContext _localctx = new ColOrderContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_colOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(314); match(COLORDER);
			setState(315); match(T__54);
			setState(316); match(INTEGER);
			setState(321);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(317); match(T__53);
				setState(318); match(INTEGER);
				}
				}
				setState(323);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(324); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupExpContext extends ParserRuleContext {
		public TerminalNode NONE() { return getToken(SelectParser.NONE, 0); }
		public RealGroupExpContext realGroupExp() {
			return getRuleContext(RealGroupExpContext.class,0);
		}
		public GroupExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterGroupExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitGroupExp(this);
		}
	}

	public final GroupExpContext groupExp() throws RecognitionException {
		GroupExpContext _localctx = new GroupExpContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_groupExp);
		try {
			setState(328);
			switch (_input.LA(1)) {
			case NONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(326); match(NONE);
				}
				break;
			case T__59:
				enterOuterAlt(_localctx, 2);
				{
				setState(327); realGroupExp();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RealGroupExpContext extends ParserRuleContext {
		public GroupDefContext groupDef(int i) {
			return getRuleContext(GroupDefContext.class,i);
		}
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public List<GroupDefContext> groupDef() {
			return getRuleContexts(GroupDefContext.class);
		}
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public RealGroupExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_realGroupExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRealGroupExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRealGroupExp(this);
		}
	}

	public final RealGroupExpContext realGroupExp() throws RecognitionException {
		RealGroupExpContext _localctx = new RealGroupExpContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_realGroupExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(330); match(T__59);
			setState(331); groupDef();
			setState(336);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(332); match(T__7);
				setState(333); groupDef();
				}
				}
				setState(338);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(339); match(T__24);
			setState(345);
			_la = _input.LA(1);
			if (_la==T__53) {
				{
				setState(340); match(T__53);
				setState(343);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(341); hashExp();
					}
					break;
				case RANGE:
					{
					setState(342); rangeType();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupDefContext extends ParserRuleContext {
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public GroupDefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupDef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterGroupDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitGroupDef(this);
		}
	}

	public final GroupDefContext groupDef() throws RecognitionException {
		GroupDefContext _localctx = new GroupDefContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_groupDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(347); match(T__59);
			setState(348); match(INTEGER);
			setState(353);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(349); match(T__7);
				setState(350); match(INTEGER);
				}
				}
				setState(355);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(356); match(T__24);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeExpContext extends ParserRuleContext {
		public RangeExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRangeExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRangeExp(this);
		}
	}

	public final RangeExpContext rangeExp() throws RecognitionException {
		RangeExpContext _localctx = new RangeExpContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_rangeExp);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(361);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(358);
					matchWildcard();
					}
					} 
				}
				setState(363);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeExpContext extends ParserRuleContext {
		public TerminalNode ANYTEXT() { return getToken(SelectParser.ANYTEXT, 0); }
		public RealNodeExpContext realNodeExp() {
			return getRuleContext(RealNodeExpContext.class,0);
		}
		public NodeExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNodeExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNodeExp(this);
		}
	}

	public final NodeExpContext nodeExp() throws RecognitionException {
		NodeExpContext _localctx = new NodeExpContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_nodeExp);
		try {
			setState(366);
			switch (_input.LA(1)) {
			case ANYTEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(364); match(ANYTEXT);
				}
				break;
			case T__59:
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(365); realNodeExp();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RealNodeExpContext extends ParserRuleContext {
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public IntegerSetContext integerSet() {
			return getRuleContext(IntegerSetContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SelectParser.ALL, 0); }
		public RealNodeExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_realNodeExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRealNodeExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRealNodeExp(this);
		}
	}

	public final RealNodeExpContext realNodeExp() throws RecognitionException {
		RealNodeExpContext _localctx = new RealNodeExpContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_realNodeExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(370);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(368); match(ALL);
				}
				break;
			case T__59:
				{
				setState(369); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(377);
			_la = _input.LA(1);
			if (_la==T__53) {
				{
				setState(372); match(T__53);
				setState(375);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(373); hashExp();
					}
					break;
				case RANGE:
					{
					setState(374); rangeType();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntegerSetContext extends ParserRuleContext {
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public IntegerSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_integerSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterIntegerSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitIntegerSet(this);
		}
	}

	public final IntegerSetContext integerSet() throws RecognitionException {
		IntegerSetContext _localctx = new IntegerSetContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_integerSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(379); match(T__59);
			setState(380); match(INTEGER);
			setState(385);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(381); match(T__7);
				setState(382); match(INTEGER);
				}
				}
				setState(387);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(388); match(T__24);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HashExpContext extends ParserRuleContext {
		public ColumnSetContext columnSet() {
			return getRuleContext(ColumnSetContext.class,0);
		}
		public TerminalNode HASH() { return getToken(SelectParser.HASH, 0); }
		public HashExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hashExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterHashExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitHashExp(this);
		}
	}

	public final HashExpContext hashExp() throws RecognitionException {
		HashExpContext _localctx = new HashExpContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_hashExp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(390); match(HASH);
			setState(391); match(T__53);
			setState(392); columnSet();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnSetContext extends ParserRuleContext {
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public ColumnSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterColumnSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitColumnSet(this);
		}
	}

	public final ColumnSetContext columnSet() throws RecognitionException {
		ColumnSetContext _localctx = new ColumnSetContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_columnSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(394); match(T__59);
			setState(395); columnName();
			setState(400);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(396); match(T__7);
				setState(397); columnName();
				}
				}
				setState(402);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(403); match(T__24);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeTypeContext extends ParserRuleContext {
		public TerminalNode RANGE() { return getToken(SelectParser.RANGE, 0); }
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public RangeSetContext rangeSet() {
			return getRuleContext(RangeSetContext.class,0);
		}
		public RangeTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRangeType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRangeType(this);
		}
	}

	public final RangeTypeContext rangeType() throws RecognitionException {
		RangeTypeContext _localctx = new RangeTypeContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_rangeType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(405); match(RANGE);
			setState(406); match(T__53);
			setState(407); columnName();
			setState(408); match(T__53);
			setState(409); rangeSet();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeSetContext extends ParserRuleContext {
		public RangeExpContext rangeExp(int i) {
			return getRuleContext(RangeExpContext.class,i);
		}
		public List<RangeExpContext> rangeExp() {
			return getRuleContexts(RangeExpContext.class);
		}
		public RangeSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRangeSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRangeSet(this);
		}
	}

	public final RangeSetContext rangeSet() throws RecognitionException {
		RangeSetContext _localctx = new RangeSetContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_rangeSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(411); match(T__59);
			setState(412); rangeExp();
			setState(417);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(413); match(T__7);
				setState(414); rangeExp();
				}
				}
				setState(419);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(420); match(T__24);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DeviceExpContext extends ParserRuleContext {
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public IntegerSetContext integerSet() {
			return getRuleContext(IntegerSetContext.class,0);
		}
		public RangeExpContext rangeExp() {
			return getRuleContext(RangeExpContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SelectParser.ALL, 0); }
		public DeviceExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deviceExp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDeviceExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDeviceExp(this);
		}
	}

	public final DeviceExpContext deviceExp() throws RecognitionException {
		DeviceExpContext _localctx = new DeviceExpContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_deviceExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(424);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(422); match(ALL);
				}
				break;
			case T__59:
				{
				setState(423); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(431);
			_la = _input.LA(1);
			if (_la==T__53) {
				{
				setState(426); match(T__53);
				setState(429);
				switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
				case 1:
					{
					setState(427); hashExp();
					}
					break;
				case 2:
					{
					setState(428); rangeExp();
					}
					break;
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropTableContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public DropTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDropTable(this);
		}
	}

	public final DropTableContext dropTable() throws RecognitionException {
		DropTableContext _localctx = new DropTableContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_dropTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(433); match(T__15);
			setState(434); match(T__33);
			setState(435); tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateViewContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public CreateViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCreateView(this);
		}
	}

	public final CreateViewContext createView() throws RecognitionException {
		CreateViewContext _localctx = new CreateViewContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_createView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(437); match(T__37);
			setState(438); match(T__23);
			setState(439); tableName();
			setState(440); match(T__32);
			setState(441); fullSelect();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropViewContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public DropViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDropView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDropView(this);
		}
	}

	public final DropViewContext dropView() throws RecognitionException {
		DropViewContext _localctx = new DropViewContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_dropView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(443); match(T__15);
			setState(444); match(T__23);
			setState(445); tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateIndexContext extends ParserRuleContext {
		public TerminalNode UNIQUE() { return getToken(SelectParser.UNIQUE, 0); }
		public IndexDefContext indexDef(int i) {
			return getRuleContext(IndexDefContext.class,i);
		}
		public List<TableNameContext> tableName() {
			return getRuleContexts(TableNameContext.class);
		}
		public TableNameContext tableName(int i) {
			return getRuleContext(TableNameContext.class,i);
		}
		public List<IndexDefContext> indexDef() {
			return getRuleContexts(IndexDefContext.class);
		}
		public CreateIndexContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createIndex; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCreateIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCreateIndex(this);
		}
	}

	public final CreateIndexContext createIndex() throws RecognitionException {
		CreateIndexContext _localctx = new CreateIndexContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_createIndex);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(447); match(T__37);
			setState(449);
			_la = _input.LA(1);
			if (_la==UNIQUE) {
				{
				setState(448); match(UNIQUE);
				}
			}

			setState(451); match(T__9);
			setState(452); tableName();
			setState(453); match(T__26);
			setState(454); tableName();
			setState(455); match(T__54);
			setState(456); indexDef();
			setState(461);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(457); match(T__53);
				setState(458); indexDef();
				}
				}
				setState(463);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(464); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropIndexContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public DropIndexContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropIndex; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDropIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDropIndex(this);
		}
	}

	public final DropIndexContext dropIndex() throws RecognitionException {
		DropIndexContext _localctx = new DropIndexContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_dropIndex);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(466); match(T__15);
			setState(467); match(T__9);
			setState(468); tableName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LoadContext extends ParserRuleContext {
		public TerminalNode REPLACE() { return getToken(SelectParser.REPLACE, 0); }
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public RemainderContext remainder() {
			return getRuleContext(RemainderContext.class,0);
		}
		public TerminalNode RESUME() { return getToken(SelectParser.RESUME, 0); }
		public AnyContext any() {
			return getRuleContext(AnyContext.class,0);
		}
		public LoadContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_load; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterLoad(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitLoad(this);
		}
	}

	public final LoadContext load() throws RecognitionException {
		LoadContext _localctx = new LoadContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_load);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(470); match(T__50);
			setState(471);
			_la = _input.LA(1);
			if ( !(_la==REPLACE || _la==RESUME) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(472); match(T__28);
			setState(473); tableName();
			setState(476);
			_la = _input.LA(1);
			if (_la==T__51) {
				{
				setState(474); match(T__51);
				setState(475); any();
				}
			}

			setState(478); match(T__62);
			setState(479); remainder();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnyContext extends ParserRuleContext {
		public AnyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterAny(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitAny(this);
		}
	}

	public final AnyContext any() throws RecognitionException {
		AnyContext _localctx = new AnyContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_any);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(481);
			matchWildcard();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RemainderContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(SelectParser.EOF, 0); }
		public RemainderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_remainder; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterRemainder(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitRemainder(this);
		}
	}

	public final RemainderContext remainder() throws RecognitionException {
		RemainderContext _localctx = new RemainderContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_remainder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(486);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__65) | (1L << T__64) | (1L << T__63) | (1L << T__62) | (1L << T__61) | (1L << T__60) | (1L << T__59) | (1L << T__58) | (1L << T__57) | (1L << T__56) | (1L << T__55) | (1L << T__54) | (1L << T__53) | (1L << T__52) | (1L << T__51) | (1L << T__50) | (1L << T__49) | (1L << T__48) | (1L << T__47) | (1L << T__46) | (1L << T__45) | (1L << T__44) | (1L << T__43) | (1L << T__42) | (1L << T__41) | (1L << T__40) | (1L << T__39) | (1L << T__38) | (1L << T__37) | (1L << T__36) | (1L << T__35) | (1L << T__34) | (1L << T__33) | (1L << T__32) | (1L << T__31) | (1L << T__30) | (1L << T__29) | (1L << T__28) | (1L << T__27) | (1L << T__26) | (1L << T__25) | (1L << T__24) | (1L << T__23) | (1L << T__22) | (1L << T__21) | (1L << T__20) | (1L << T__19) | (1L << T__18) | (1L << T__17) | (1L << T__16) | (1L << T__15) | (1L << T__14) | (1L << T__13) | (1L << T__12) | (1L << T__11) | (1L << T__10) | (1L << T__9) | (1L << T__8) | (1L << T__7) | (1L << T__6) | (1L << T__5) | (1L << T__4) | (1L << T__3))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (T__2 - 64)) | (1L << (T__1 - 64)) | (1L << (T__0 - 64)) | (1L << (STRING - 64)) | (1L << (STAR - 64)) | (1L << (COUNT - 64)) | (1L << (CONCAT - 64)) | (1L << (NEGATIVE - 64)) | (1L << (EQUALS - 64)) | (1L << (OPERATOR - 64)) | (1L << (NULLOPERATOR - 64)) | (1L << (AND - 64)) | (1L << (OR - 64)) | (1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (COLUMN - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (COLORDER - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (JAVACLASSNAMEIDENTIFIER - 64)) | (1L << (FILEPATHIDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0)) {
				{
				{
				setState(483);
				matchWildcard();
				}
				}
				setState(488);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(489); match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IndexDefContext extends ParserRuleContext {
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TerminalNode DIRECTION() { return getToken(SelectParser.DIRECTION, 0); }
		public IndexDefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_indexDef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterIndexDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitIndexDef(this);
		}
	}

	public final IndexDefContext indexDef() throws RecognitionException {
		IndexDefContext _localctx = new IndexDefContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_indexDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(491); columnName();
			setState(493);
			_la = _input.LA(1);
			if (_la==DIRECTION) {
				{
				setState(492); match(DIRECTION);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColDefContext extends ParserRuleContext {
		public NotNullContext notNull() {
			return getRuleContext(NotNullContext.class,0);
		}
		public PrimaryContext primary() {
			return getRuleContext(PrimaryContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public ColDefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colDef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterColDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitColDef(this);
		}
	}

	public final ColDefContext colDef() throws RecognitionException {
		ColDefContext _localctx = new ColDefContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_colDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(495); columnName();
			setState(496); dataType();
			setState(498);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(497); notNull();
				}
			}

			setState(501);
			_la = _input.LA(1);
			if (_la==T__52) {
				{
				setState(500); primary();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PrimaryKeyContext extends ParserRuleContext {
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public PrimaryKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterPrimaryKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitPrimaryKey(this);
		}
	}

	public final PrimaryKeyContext primaryKey() throws RecognitionException {
		PrimaryKeyContext _localctx = new PrimaryKeyContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_primaryKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(503); match(T__52);
			setState(504); match(T__19);
			setState(505); match(T__54);
			setState(506); columnName();
			setState(511);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(507); match(T__53);
				setState(508); columnName();
				}
				}
				setState(513);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(514); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NotNullContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(SelectParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SelectParser.NULL, 0); }
		public NotNullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_notNull; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNotNull(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNotNull(this);
		}
	}

	public final NotNullContext notNull() throws RecognitionException {
		NotNullContext _localctx = new NotNullContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_notNull);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(516); match(NOT);
			setState(517); match(NULL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PrimaryContext extends ParserRuleContext {
		public PrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primary; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterPrimary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitPrimary(this);
		}
	}

	public final PrimaryContext primary() throws RecognitionException {
		PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_primary);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(519); match(T__52);
			setState(520); match(T__19);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public Char2Context char2() {
			return getRuleContext(Char2Context.class,0);
		}
		public Int2Context int2() {
			return getRuleContext(Int2Context.class,0);
		}
		public Long2Context long2() {
			return getRuleContext(Long2Context.class,0);
		}
		public Float2Context float2() {
			return getRuleContext(Float2Context.class,0);
		}
		public Date2Context date2() {
			return getRuleContext(Date2Context.class,0);
		}
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDataType(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_dataType);
		try {
			setState(527);
			switch (_input.LA(1)) {
			case T__48:
			case T__11:
				enterOuterAlt(_localctx, 1);
				{
				setState(522); char2();
				}
				break;
			case T__64:
				enterOuterAlt(_localctx, 2);
				{
				setState(523); int2();
				}
				break;
			case T__45:
				enterOuterAlt(_localctx, 3);
				{
				setState(524); long2();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 4);
				{
				setState(525); date2();
				}
				break;
			case T__65:
			case T__2:
				enterOuterAlt(_localctx, 5);
				{
				setState(526); float2();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Char2Context extends ParserRuleContext {
		public TerminalNode INTEGER() { return getToken(SelectParser.INTEGER, 0); }
		public Char2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_char2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterChar2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitChar2(this);
		}
	}

	public final Char2Context char2() throws RecognitionException {
		Char2Context _localctx = new Char2Context(_ctx, getState());
		enterRule(_localctx, 80, RULE_char2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(529);
			_la = _input.LA(1);
			if ( !(_la==T__48 || _la==T__11) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(530); match(T__54);
			setState(531); match(INTEGER);
			setState(532); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Int2Context extends ParserRuleContext {
		public Int2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_int2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterInt2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitInt2(this);
		}
	}

	public final Int2Context int2() throws RecognitionException {
		Int2Context _localctx = new Int2Context(_ctx, getState());
		enterRule(_localctx, 82, RULE_int2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(534); match(T__64);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Long2Context extends ParserRuleContext {
		public Long2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_long2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterLong2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitLong2(this);
		}
	}

	public final Long2Context long2() throws RecognitionException {
		Long2Context _localctx = new Long2Context(_ctx, getState());
		enterRule(_localctx, 84, RULE_long2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(536); match(T__45);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Date2Context extends ParserRuleContext {
		public TerminalNode DATE() { return getToken(SelectParser.DATE, 0); }
		public Date2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_date2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterDate2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitDate2(this);
		}
	}

	public final Date2Context date2() throws RecognitionException {
		Date2Context _localctx = new Date2Context(_ctx, getState());
		enterRule(_localctx, 86, RULE_date2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(538); match(DATE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Float2Context extends ParserRuleContext {
		public Float2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_float2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterFloat2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitFloat2(this);
		}
	}

	public final Float2Context float2() throws RecognitionException {
		Float2Context _localctx = new Float2Context(_ctx, getState());
		enterRule(_localctx, 88, RULE_float2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(540);
			_la = _input.LA(1);
			if ( !(_la==T__65 || _la==T__2) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColListContext extends ParserRuleContext {
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public ColListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterColList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitColList(this);
		}
	}

	public final ColListContext colList() throws RecognitionException {
		ColListContext _localctx = new ColListContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_colList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(542); match(T__54);
			setState(543); columnName();
			setState(548);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(544); match(T__53);
				setState(545); columnName();
				}
				}
				setState(550);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(551); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommonTableExpressionContext extends ParserRuleContext {
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public CommonTableExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commonTableExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCommonTableExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCommonTableExpression(this);
		}
	}

	public final CommonTableExpressionContext commonTableExpression() throws RecognitionException {
		CommonTableExpressionContext _localctx = new CommonTableExpressionContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_commonTableExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(553); match(IDENTIFIER);
			setState(565);
			_la = _input.LA(1);
			if (_la==T__54) {
				{
				setState(554); match(T__54);
				setState(555); columnName();
				setState(560);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__53) {
					{
					{
					setState(556); match(T__53);
					setState(557); columnName();
					}
					}
					setState(562);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(563); match(T__1);
				}
			}

			setState(567); match(T__32);
			setState(568); match(T__54);
			setState(569); fullSelect();
			setState(570); match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FullSelectContext extends ParserRuleContext {
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
		public OrderByContext orderBy() {
			return getRuleContext(OrderByContext.class,0);
		}
		public ConnectedSelectContext connectedSelect(int i) {
			return getRuleContext(ConnectedSelectContext.class,i);
		}
		public FetchFirstContext fetchFirst() {
			return getRuleContext(FetchFirstContext.class,0);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public List<ConnectedSelectContext> connectedSelect() {
			return getRuleContexts(ConnectedSelectContext.class);
		}
		public FullSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullSelect; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterFullSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitFullSelect(this);
		}
	}

	public final FullSelectContext fullSelect() throws RecognitionException {
		FullSelectContext _localctx = new FullSelectContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_fullSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(577);
			switch (_input.LA(1)) {
			case T__17:
				{
				setState(572); subSelect();
				}
				break;
			case T__54:
				{
				setState(573); match(T__54);
				setState(574); fullSelect();
				setState(575); match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(582);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==TABLECOMBINATION) {
				{
				{
				setState(579); connectedSelect();
				}
				}
				setState(584);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(586);
			_la = _input.LA(1);
			if (_la==T__18) {
				{
				setState(585); orderBy();
				}
			}

			setState(589);
			_la = _input.LA(1);
			if (_la==T__43) {
				{
				setState(588); fetchFirst();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConnectedSelectContext extends ParserRuleContext {
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
		public TerminalNode TABLECOMBINATION() { return getToken(SelectParser.TABLECOMBINATION, 0); }
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public ConnectedSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_connectedSelect; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterConnectedSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitConnectedSelect(this);
		}
	}

	public final ConnectedSelectContext connectedSelect() throws RecognitionException {
		ConnectedSelectContext _localctx = new ConnectedSelectContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_connectedSelect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(591); match(TABLECOMBINATION);
			setState(597);
			switch (_input.LA(1)) {
			case T__17:
				{
				setState(592); subSelect();
				}
				break;
			case T__54:
				{
				setState(593); match(T__54);
				setState(594); fullSelect();
				setState(595); match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubSelectContext extends ParserRuleContext {
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public OrderByContext orderBy() {
			return getRuleContext(OrderByContext.class,0);
		}
		public FetchFirstContext fetchFirst() {
			return getRuleContext(FetchFirstContext.class,0);
		}
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public SubSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subSelect; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSubSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSubSelect(this);
		}
	}

	public final SubSelectContext subSelect() throws RecognitionException {
		SubSelectContext _localctx = new SubSelectContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_subSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(599); selectClause();
			setState(600); fromClause();
			setState(602);
			_la = _input.LA(1);
			if (_la==T__29) {
				{
				setState(601); whereClause();
				}
			}

			setState(605);
			_la = _input.LA(1);
			if (_la==T__56) {
				{
				setState(604); groupBy();
				}
			}

			setState(608);
			_la = _input.LA(1);
			if (_la==T__42) {
				{
				setState(607); havingClause();
				}
			}

			setState(611);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				{
				setState(610); orderBy();
				}
				break;
			}
			setState(614);
			switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
			case 1:
				{
				setState(613); fetchFirst();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectClauseContext extends ParserRuleContext {
		public SelecthowContext selecthow() {
			return getRuleContext(SelecthowContext.class,0);
		}
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
		public SelectListEntryContext selectListEntry(int i) {
			return getRuleContext(SelectListEntryContext.class,i);
		}
		public List<SelectListEntryContext> selectListEntry() {
			return getRuleContexts(SelectListEntryContext.class);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSelectClause(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(616); match(T__17);
			setState(618);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==ALL) {
				{
				setState(617); selecthow();
				}
			}

			setState(629);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(620); match(STAR);
				}
				break;
			case T__55:
			case T__54:
			case STRING:
			case COUNT:
			case NEGATIVE:
			case NULL:
			case INTEGER:
			case DATE:
			case IDENTIFIER:
				{
				{
				setState(621); selectListEntry();
				setState(626);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__53) {
					{
					{
					setState(622); match(T__53);
					setState(623); selectListEntry();
					}
					}
					setState(628);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelecthowContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SelectParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SelectParser.ALL, 0); }
		public SelecthowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selecthow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSelecthow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSelecthow(this);
		}
	}

	public final SelecthowContext selecthow() throws RecognitionException {
		SelecthowContext _localctx = new SelecthowContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_selecthow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(631);
			_la = _input.LA(1);
			if ( !(_la==DISTINCT || _la==ALL) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectListEntryContext extends ParserRuleContext {
		public SelectListEntryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectListEntry; }
	 
		public SelectListEntryContext() { }
		public void copyFrom(SelectListEntryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SelectColumnContext extends SelectListEntryContext {
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public SelectColumnContext(SelectListEntryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSelectColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSelectColumn(this);
		}
	}
	public static class SelectExpressionContext extends SelectListEntryContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public SelectExpressionContext(SelectListEntryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSelectExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSelectExpression(this);
		}
	}

	public final SelectListEntryContext selectListEntry() throws RecognitionException {
		SelectListEntryContext _localctx = new SelectListEntryContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_selectListEntry);
		int _la;
		try {
			setState(647);
			switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
			case 1:
				_localctx = new SelectColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(633); columnName();
				setState(635);
				_la = _input.LA(1);
				if (_la==T__32) {
					{
					setState(634); match(T__32);
					}
				}

				setState(638);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(637); match(IDENTIFIER);
					}
				}

				}
				break;
			case 2:
				_localctx = new SelectExpressionContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(640); expression(0);
				setState(642);
				_la = _input.LA(1);
				if (_la==T__32) {
					{
					setState(641); match(T__32);
					}
				}

				setState(645);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(644); match(IDENTIFIER);
					}
				}

				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitFromClause(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(649); match(T__62);
			setState(650); tableReference(0);
			setState(655);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(651); match(T__53);
				setState(652); tableReference(0);
				}
				}
				setState(657);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableReferenceContext extends ParserRuleContext {
		public TableReferenceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableReference; }
	 
		public TableReferenceContext() { }
		public void copyFrom(TableReferenceContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class JoinPContext extends TableReferenceContext {
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public TerminalNode JOINTYPE() { return getToken(SelectParser.JOINTYPE, 0); }
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public JoinPContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterJoinP(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitJoinP(this);
		}
	}
	public static class NestedTableContext extends TableReferenceContext {
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public NestedTableContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNestedTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNestedTable(this);
		}
	}
	public static class CrossJoinPContext extends TableReferenceContext {
		public TerminalNode CROSSJOIN() { return getToken(SelectParser.CROSSJOIN, 0); }
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public CrossJoinPContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCrossJoinP(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCrossJoinP(this);
		}
	}
	public static class IsSingleTableContext extends TableReferenceContext {
		public SingleTableContext singleTable() {
			return getRuleContext(SingleTableContext.class,0);
		}
		public IsSingleTableContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterIsSingleTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitIsSingleTable(this);
		}
	}
	public static class JoinContext extends TableReferenceContext {
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public TerminalNode JOINTYPE() { return getToken(SelectParser.JOINTYPE, 0); }
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public JoinContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitJoin(this);
		}
	}
	public static class CrossJoinContext extends TableReferenceContext {
		public TerminalNode CROSSJOIN() { return getToken(SelectParser.CROSSJOIN, 0); }
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public CrossJoinContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCrossJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCrossJoin(this);
		}
	}

	public final TableReferenceContext tableReference() throws RecognitionException {
		return tableReference(0);
	}

	private TableReferenceContext tableReference(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TableReferenceContext _localctx = new TableReferenceContext(_ctx, _parentState);
		TableReferenceContext _prevctx = _localctx;
		int _startState = 108;
		enterRecursionRule(_localctx, 108, RULE_tableReference, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(687);
			switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
			case 1:
				{
				_localctx = new JoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(659); match(T__54);
				setState(660); tableReference(0);
				setState(662);
				_la = _input.LA(1);
				if (_la==JOINTYPE) {
					{
					setState(661); match(JOINTYPE);
					}
				}

				setState(664); match(T__25);
				setState(665); tableReference(0);
				setState(666); match(T__26);
				setState(667); searchCondition();
				setState(668); match(T__1);
				setState(670);
				switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
				case 1:
					{
					setState(669); correlationClause();
					}
					break;
				}
				}
				break;
			case 2:
				{
				_localctx = new CrossJoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(672); match(T__54);
				setState(673); tableReference(0);
				setState(674); match(CROSSJOIN);
				setState(675); tableReference(0);
				setState(676); match(T__1);
				setState(678);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
				case 1:
					{
					setState(677); correlationClause();
					}
					break;
				}
				}
				break;
			case 3:
				{
				_localctx = new NestedTableContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(680); match(T__54);
				setState(681); fullSelect();
				setState(682); match(T__1);
				setState(684);
				switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
				case 1:
					{
					setState(683); correlationClause();
					}
					break;
				}
				}
				break;
			case 4:
				{
				_localctx = new IsSingleTableContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(686); singleTable();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(708);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(706);
					switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
					case 1:
						{
						_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(689);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(691);
						_la = _input.LA(1);
						if (_la==JOINTYPE) {
							{
							setState(690); match(JOINTYPE);
							}
						}

						setState(693); match(T__25);
						setState(694); tableReference(0);
						setState(695); match(T__26);
						setState(696); searchCondition();
						setState(698);
						switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
						case 1:
							{
							setState(697); correlationClause();
							}
							break;
						}
						}
						break;
					case 2:
						{
						_localctx = new CrossJoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(700);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(701); match(CROSSJOIN);
						setState(702); tableReference(0);
						setState(704);
						switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
						case 1:
							{
							setState(703); correlationClause();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(710);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class SingleTableContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public SingleTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSingleTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSingleTable(this);
		}
	}

	public final SingleTableContext singleTable() throws RecognitionException {
		SingleTableContext _localctx = new SingleTableContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_singleTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(711); tableName();
			setState(713);
			switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
			case 1:
				{
				setState(712); correlationClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhereClauseContext extends ParserRuleContext {
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitWhereClause(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(715); match(T__29);
			setState(716); searchCondition();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupByContext extends ParserRuleContext {
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public GroupByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterGroupBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitGroupBy(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_groupBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(718); match(T__56);
			setState(719); match(T__31);
			setState(720); columnName();
			setState(725);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(721); match(T__53);
				setState(722); columnName();
				}
				}
				setState(727);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HavingClauseContext extends ParserRuleContext {
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterHavingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitHavingClause(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(728); match(T__42);
			setState(729); searchCondition();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderByContext extends ParserRuleContext {
		public SortKeyContext sortKey(int i) {
			return getRuleContext(SortKeyContext.class,i);
		}
		public List<SortKeyContext> sortKey() {
			return getRuleContexts(SortKeyContext.class);
		}
		public OrderByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterOrderBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitOrderBy(this);
		}
	}

	public final OrderByContext orderBy() throws RecognitionException {
		OrderByContext _localctx = new OrderByContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_orderBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(731); match(T__18);
			setState(732); match(T__31);
			setState(733); sortKey();
			setState(738);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__53) {
				{
				{
				setState(734); match(T__53);
				setState(735); sortKey();
				}
				}
				setState(740);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortKeyContext extends ParserRuleContext {
		public SortKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortKey; }
	 
		public SortKeyContext() { }
		public void copyFrom(SortKeyContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SortKeyColContext extends SortKeyContext {
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TerminalNode DIRECTION() { return getToken(SelectParser.DIRECTION, 0); }
		public SortKeyColContext(SortKeyContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSortKeyCol(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSortKeyCol(this);
		}
	}
	public static class SortKeyIntContext extends SortKeyContext {
		public TerminalNode INTEGER() { return getToken(SelectParser.INTEGER, 0); }
		public TerminalNode DIRECTION() { return getToken(SelectParser.DIRECTION, 0); }
		public SortKeyIntContext(SortKeyContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSortKeyInt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSortKeyInt(this);
		}
	}

	public final SortKeyContext sortKey() throws RecognitionException {
		SortKeyContext _localctx = new SortKeyContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_sortKey);
		int _la;
		try {
			setState(749);
			switch (_input.LA(1)) {
			case INTEGER:
				_localctx = new SortKeyIntContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(741); match(INTEGER);
				setState(743);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(742); match(DIRECTION);
					}
				}

				}
				break;
			case IDENTIFIER:
				_localctx = new SortKeyColContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(745); columnName();
				setState(747);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(746); match(DIRECTION);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CorrelationClauseContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public CorrelationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_correlationClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCorrelationClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCorrelationClause(this);
		}
	}

	public final CorrelationClauseContext correlationClause() throws RecognitionException {
		CorrelationClauseContext _localctx = new CorrelationClauseContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_correlationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(752);
			_la = _input.LA(1);
			if (_la==T__32) {
				{
				setState(751); match(T__32);
				}
			}

			setState(754); match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FetchFirstContext extends ParserRuleContext {
		public TerminalNode INTEGER() { return getToken(SelectParser.INTEGER, 0); }
		public FetchFirstContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fetchFirst; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterFetchFirst(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitFetchFirst(this);
		}
	}

	public final FetchFirstContext fetchFirst() throws RecognitionException {
		FetchFirstContext _localctx = new FetchFirstContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_fetchFirst);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(756); match(T__43);
			setState(757); match(T__44);
			setState(759);
			_la = _input.LA(1);
			if (_la==INTEGER) {
				{
				setState(758); match(INTEGER);
				}
			}

			setState(761);
			_la = _input.LA(1);
			if ( !(_la==T__12 || _la==T__6) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(762); match(T__34);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableNameContext extends ParserRuleContext {
		public TableNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableName; }
	 
		public TableNameContext() { }
		public void copyFrom(TableNameContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Table1PartContext extends TableNameContext {
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public Table1PartContext(TableNameContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterTable1Part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitTable1Part(this);
		}
	}
	public static class Table2PartContext extends TableNameContext {
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SelectParser.IDENTIFIER, i);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(SelectParser.IDENTIFIER); }
		public Table2PartContext(TableNameContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterTable2Part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitTable2Part(this);
		}
	}

	public final TableNameContext tableName() throws RecognitionException {
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_tableName);
		try {
			setState(768);
			switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
			case 1:
				_localctx = new Table1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(764); match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new Table2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(765); match(IDENTIFIER);
				setState(766); match(T__14);
				setState(767); match(IDENTIFIER);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameContext extends ParserRuleContext {
		public ColumnNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnName; }
	 
		public ColumnNameContext() { }
		public void copyFrom(ColumnNameContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Col2PartContext extends ColumnNameContext {
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SelectParser.IDENTIFIER, i);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(SelectParser.IDENTIFIER); }
		public Col2PartContext(ColumnNameContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCol2Part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCol2Part(this);
		}
	}
	public static class Col1PartContext extends ColumnNameContext {
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public Col1PartContext(ColumnNameContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCol1Part(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCol1Part(this);
		}
	}

	public final ColumnNameContext columnName() throws RecognitionException {
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_columnName);
		try {
			setState(774);
			switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
			case 1:
				_localctx = new Col1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(770); match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new Col2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(771); match(IDENTIFIER);
				setState(772); match(T__14);
				setState(773); match(IDENTIFIER);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SearchConditionContext extends ParserRuleContext {
		public SearchClauseContext searchClause() {
			return getRuleContext(SearchClauseContext.class,0);
		}
		public List<ConnectedSearchClauseContext> connectedSearchClause() {
			return getRuleContexts(ConnectedSearchClauseContext.class);
		}
		public ConnectedSearchClauseContext connectedSearchClause(int i) {
			return getRuleContext(ConnectedSearchClauseContext.class,i);
		}
		public SearchConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_searchCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSearchCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSearchCondition(this);
		}
	}

	public final SearchConditionContext searchCondition() throws RecognitionException {
		SearchConditionContext _localctx = new SearchConditionContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_searchCondition);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(776); searchClause();
			setState(780);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,86,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(777); connectedSearchClause();
					}
					} 
				}
				setState(782);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,86,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConnectedSearchClauseContext extends ParserRuleContext {
		public SearchClauseContext searchClause() {
			return getRuleContext(SearchClauseContext.class,0);
		}
		public TerminalNode AND() { return getToken(SelectParser.AND, 0); }
		public TerminalNode OR() { return getToken(SelectParser.OR, 0); }
		public ConnectedSearchClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_connectedSearchClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterConnectedSearchClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitConnectedSearchClause(this);
		}
	}

	public final ConnectedSearchClauseContext connectedSearchClause() throws RecognitionException {
		ConnectedSearchClauseContext _localctx = new ConnectedSearchClauseContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_connectedSearchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(783);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OR) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(784); searchClause();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SearchClauseContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(SelectParser.NOT, 0); }
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public SearchClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_searchClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterSearchClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitSearchClause(this);
		}
	}

	public final SearchClauseContext searchClause() throws RecognitionException {
		SearchClauseContext _localctx = new SearchClauseContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_searchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(787);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(786); match(NOT);
				}
			}

			setState(794);
			switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
			case 1:
				{
				setState(789); predicate();
				}
				break;
			case 2:
				{
				{
				setState(790); match(T__54);
				setState(791); searchCondition();
				setState(792); match(T__1);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExistsPredicateContext extends PredicateContext {
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
		public ExistsPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterExistsPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitExistsPredicate(this);
		}
	}
	public static class NullPredicateContext extends PredicateContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLOPERATOR() { return getToken(SelectParser.NULLOPERATOR, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNullPredicate(this);
		}
	}
	public static class NormalPredicateContext extends PredicateContext {
		public OperatorContext operator() {
			return getRuleContext(OperatorContext.class,0);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public NormalPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNormalPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNormalPredicate(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_predicate);
		try {
			setState(808);
			switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
			case 1:
				_localctx = new NormalPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(796); expression(0);
				setState(797); operator();
				setState(798); expression(0);
				}
				}
				break;
			case 2:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(800); expression(0);
				setState(801); match(NULLOPERATOR);
				}
				}
				break;
			case 3:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(803); match(T__60);
				setState(804); match(T__54);
				setState(805); subSelect();
				setState(806); match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OperatorContext extends ParserRuleContext {
		public TerminalNode EQUALS() { return getToken(SelectParser.EQUALS, 0); }
		public TerminalNode OPERATOR() { return getToken(SelectParser.OPERATOR, 0); }
		public OperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitOperator(this);
		}
	}

	public final OperatorContext operator() throws RecognitionException {
		OperatorContext _localctx = new OperatorContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(810);
			_la = _input.LA(1);
			if ( !(_la==EQUALS || _la==OPERATOR) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	 
		public ExpressionContext() { }
		public void copyFrom(ExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class CountStarContext extends ExpressionContext {
		public TerminalNode COUNT() { return getToken(SelectParser.COUNT, 0); }
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
		public CountStarContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCountStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCountStar(this);
		}
	}
	public static class ExpSelectContext extends ExpressionContext {
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
		public ExpSelectContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterExpSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitExpSelect(this);
		}
	}
	public static class MulDivContext extends ExpressionContext {
		public Token op;
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public MulDivContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterMulDiv(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitMulDiv(this);
		}
	}
	public static class AddSubContext extends ExpressionContext {
		public Token op;
		public TerminalNode NEGATIVE() { return getToken(SelectParser.NEGATIVE, 0); }
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public AddSubContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterAddSub(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitAddSub(this);
		}
	}
	public static class IsLiteralContext extends ExpressionContext {
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public IsLiteralContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterIsLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitIsLiteral(this);
		}
	}
	public static class CaseExpContext extends ExpressionContext {
		public List<CaseCaseContext> caseCase() {
			return getRuleContexts(CaseCaseContext.class);
		}
		public CaseCaseContext caseCase(int i) {
			return getRuleContext(CaseCaseContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CaseExpContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCaseExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCaseExp(this);
		}
	}
	public static class NullExpContext extends ExpressionContext {
		public TerminalNode NULL() { return getToken(SelectParser.NULL, 0); }
		public NullExpContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNullExp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNullExp(this);
		}
	}
	public static class PExpressionContext extends ExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PExpressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterPExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitPExpression(this);
		}
	}
	public static class ConcatContext extends ExpressionContext {
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode CONCAT() { return getToken(SelectParser.CONCAT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ConcatContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterConcat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitConcat(this);
		}
	}
	public static class FunctionContext extends ExpressionContext {
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public FunctionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitFunction(this);
		}
	}
	public static class ColLiteralContext extends ExpressionContext {
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public ColLiteralContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterColLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitColLiteral(this);
		}
	}
	public static class ListContext extends ExpressionContext {
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ListContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitList(this);
		}
	}
	public static class CountDistinctContext extends ExpressionContext {
		public TerminalNode COUNT() { return getToken(SelectParser.COUNT, 0); }
		public TerminalNode DISTINCT() { return getToken(SelectParser.DISTINCT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CountDistinctContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCountDistinct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCountDistinct(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 140;
		enterRecursionRule(_localctx, 140, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(870);
			switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
			case 1:
				{
				_localctx = new FunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(813); identifier();
				setState(814); match(T__54);
				setState(815); expression(0);
				setState(820);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__53) {
					{
					{
					setState(816); match(T__53);
					setState(817); expression(0);
					}
					}
					setState(822);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(823); match(T__1);
				}
				break;
			case 2:
				{
				_localctx = new CountDistinctContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(825); match(COUNT);
				setState(826); match(T__54);
				setState(827); match(DISTINCT);
				setState(828); expression(0);
				setState(829); match(T__1);
				}
				break;
			case 3:
				{
				_localctx = new ListContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(831); match(T__54);
				setState(832); expression(0);
				setState(835); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(833); match(T__53);
					setState(834); expression(0);
					}
					}
					setState(837); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__53 );
				setState(839); match(T__1);
				}
				break;
			case 4:
				{
				_localctx = new CountStarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(841); match(COUNT);
				setState(842); match(T__54);
				setState(843); match(STAR);
				setState(844); match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new IsLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(845); literal();
				}
				break;
			case 6:
				{
				_localctx = new ColLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(846); columnName();
				}
				break;
			case 7:
				{
				_localctx = new ExpSelectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(847); match(T__54);
				setState(848); subSelect();
				setState(849); match(T__1);
				}
				break;
			case 8:
				{
				_localctx = new PExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(851); match(T__54);
				setState(852); expression(0);
				setState(853); match(T__1);
				}
				break;
			case 9:
				{
				_localctx = new NullExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(855); match(NULL);
				}
				break;
			case 10:
				{
				_localctx = new CaseExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(856); match(T__55);
				setState(857); caseCase();
				setState(861);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__13) {
					{
					{
					setState(858); caseCase();
					}
					}
					setState(863);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(864); match(T__30);
				setState(865); expression(0);
				setState(866); match(T__27);
				setState(868);
				switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
				case 1:
					{
					setState(867); match(T__55);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(883);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(881);
					switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
					case 1:
						{
						_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(872);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(873);
						((MulDivContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__35 || _la==STAR) ) {
							((MulDivContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(874); expression(14);
						}
						break;
					case 2:
						{
						_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(875);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(876);
						((AddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__38 || _la==NEGATIVE) ) {
							((AddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(877); expression(13);
						}
						break;
					case 3:
						{
						_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(878);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(879); match(CONCAT);
						setState(880); expression(12);
						}
						break;
					}
					} 
				}
				setState(885);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class CaseCaseContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public CaseCaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseCase; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterCaseCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitCaseCase(this);
		}
	}

	public final CaseCaseContext caseCase() throws RecognitionException {
		CaseCaseContext _localctx = new CaseCaseContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_caseCase);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(886); match(T__13);
			setState(887); searchCondition();
			setState(888); match(T__22);
			setState(889); expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(SelectParser.COUNT, 0); }
		public TerminalNode DATE() { return getToken(SelectParser.DATE, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitIdentifier(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(891);
			_la = _input.LA(1);
			if ( !(((((_la - 69)) & ~0x3f) == 0 && ((1L << (_la - 69)) & ((1L << (COUNT - 69)) | (1L << (DATE - 69)) | (1L << (IDENTIFIER - 69)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralContext extends ParserRuleContext {
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
	 
		public LiteralContext() { }
		public void copyFrom(LiteralContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NumericLiteralContext extends LiteralContext {
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode NEGATIVE() { return getToken(SelectParser.NEGATIVE, 0); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public NumericLiteralContext(LiteralContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitNumericLiteral(this);
		}
	}
	public static class StringLiteralContext extends LiteralContext {
		public TerminalNode STRING() { return getToken(SelectParser.STRING, 0); }
		public StringLiteralContext(LiteralContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitStringLiteral(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_literal);
		int _la;
		try {
			setState(902);
			switch (_input.LA(1)) {
			case NEGATIVE:
			case INTEGER:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(894);
				_la = _input.LA(1);
				if (_la==NEGATIVE) {
					{
					setState(893); match(NEGATIVE);
					}
				}

				setState(896); match(INTEGER);
				setState(899);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(897); match(T__14);
					setState(898); match(INTEGER);
					}
					break;
				}
				}
				break;
			case STRING:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(901); match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 54: return tableReference_sempred((TableReferenceContext)_localctx, predIndex);
		case 70: return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2: return precpred(_ctx, 13);
		case 3: return precpred(_ctx, 12);
		case 4: return precpred(_ctx, 11);
		}
		return true;
	}
	private boolean tableReference_sempred(TableReferenceContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0: return precpred(_ctx, 6);
		case 1: return precpred(_ctx, 5);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3f\u038b\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3"+
		"\2\3\2\3\2\7\2\u00a7\n\2\f\2\16\2\u00aa\13\2\5\2\u00ac\n\2\3\2\5\2\u00af"+
		"\n\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4\u00b9\n\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\7\4\u00c1\n\4\f\4\16\4\u00c4\13\4\3\4\3\4\5\4\u00c8\n\4\3\5\3\5\3"+
		"\5\3\5\3\5\5\5\u00cf\n\5\3\5\3\5\3\5\5\5\u00d4\n\5\3\6\3\6\3\6\3\6\5\6"+
		"\u00da\n\6\3\7\3\7\5\7\u00de\n\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7\u00e6\n\7"+
		"\f\7\16\7\u00e9\13\7\3\7\3\7\5\7\u00ed\n\7\3\7\3\7\5\7\u00f1\n\7\3\7\5"+
		"\7\u00f4\n\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\7\b\u0101\n\b"+
		"\f\b\16\b\u0104\13\b\3\b\3\b\3\b\5\b\u0109\n\b\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\13\3\13\3\13\7\13\u0125\n\13\f\13\16\13\u0128\13\13\3\13\3\13\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\3\f\3\f\7\f\u0134\n\f\f\f\16\f\u0137\13\f\3\r\3\r\3\16"+
		"\3\16\3\17\3\17\3\17\3\17\3\17\7\17\u0142\n\17\f\17\16\17\u0145\13\17"+
		"\3\17\3\17\3\20\3\20\5\20\u014b\n\20\3\21\3\21\3\21\3\21\7\21\u0151\n"+
		"\21\f\21\16\21\u0154\13\21\3\21\3\21\3\21\3\21\5\21\u015a\n\21\5\21\u015c"+
		"\n\21\3\22\3\22\3\22\3\22\7\22\u0162\n\22\f\22\16\22\u0165\13\22\3\22"+
		"\3\22\3\23\7\23\u016a\n\23\f\23\16\23\u016d\13\23\3\24\3\24\5\24\u0171"+
		"\n\24\3\25\3\25\5\25\u0175\n\25\3\25\3\25\3\25\5\25\u017a\n\25\5\25\u017c"+
		"\n\25\3\26\3\26\3\26\3\26\7\26\u0182\n\26\f\26\16\26\u0185\13\26\3\26"+
		"\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\7\30\u0191\n\30\f\30\16"+
		"\30\u0194\13\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32"+
		"\3\32\7\32\u01a2\n\32\f\32\16\32\u01a5\13\32\3\32\3\32\3\33\3\33\5\33"+
		"\u01ab\n\33\3\33\3\33\3\33\5\33\u01b0\n\33\5\33\u01b2\n\33\3\34\3\34\3"+
		"\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3\37\3\37\5"+
		"\37\u01c4\n\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u01ce\n\37"+
		"\f\37\16\37\u01d1\13\37\3\37\3\37\3 \3 \3 \3 \3!\3!\3!\3!\3!\3!\5!\u01df"+
		"\n!\3!\3!\3!\3\"\3\"\3#\7#\u01e7\n#\f#\16#\u01ea\13#\3#\3#\3$\3$\5$\u01f0"+
		"\n$\3%\3%\3%\5%\u01f5\n%\3%\5%\u01f8\n%\3&\3&\3&\3&\3&\3&\7&\u0200\n&"+
		"\f&\16&\u0203\13&\3&\3&\3\'\3\'\3\'\3(\3(\3(\3)\3)\3)\3)\3)\5)\u0212\n"+
		")\3*\3*\3*\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/\3/\3/\7/\u0225\n/\f/\16"+
		"/\u0228\13/\3/\3/\3\60\3\60\3\60\3\60\3\60\7\60\u0231\n\60\f\60\16\60"+
		"\u0234\13\60\3\60\3\60\5\60\u0238\n\60\3\60\3\60\3\60\3\60\3\60\3\61\3"+
		"\61\3\61\3\61\3\61\5\61\u0244\n\61\3\61\7\61\u0247\n\61\f\61\16\61\u024a"+
		"\13\61\3\61\5\61\u024d\n\61\3\61\5\61\u0250\n\61\3\62\3\62\3\62\3\62\3"+
		"\62\3\62\5\62\u0258\n\62\3\63\3\63\3\63\5\63\u025d\n\63\3\63\5\63\u0260"+
		"\n\63\3\63\5\63\u0263\n\63\3\63\5\63\u0266\n\63\3\63\5\63\u0269\n\63\3"+
		"\64\3\64\5\64\u026d\n\64\3\64\3\64\3\64\3\64\7\64\u0273\n\64\f\64\16\64"+
		"\u0276\13\64\5\64\u0278\n\64\3\65\3\65\3\66\3\66\5\66\u027e\n\66\3\66"+
		"\5\66\u0281\n\66\3\66\3\66\5\66\u0285\n\66\3\66\5\66\u0288\n\66\5\66\u028a"+
		"\n\66\3\67\3\67\3\67\3\67\7\67\u0290\n\67\f\67\16\67\u0293\13\67\38\3"+
		"8\38\38\58\u0299\n8\38\38\38\38\38\38\58\u02a1\n8\38\38\38\38\38\38\5"+
		"8\u02a9\n8\38\38\38\38\58\u02af\n8\38\58\u02b2\n8\38\38\58\u02b6\n8\3"+
		"8\38\38\38\38\58\u02bd\n8\38\38\38\38\58\u02c3\n8\78\u02c5\n8\f8\168\u02c8"+
		"\138\39\39\59\u02cc\n9\3:\3:\3:\3;\3;\3;\3;\3;\7;\u02d6\n;\f;\16;\u02d9"+
		"\13;\3<\3<\3<\3=\3=\3=\3=\3=\7=\u02e3\n=\f=\16=\u02e6\13=\3>\3>\5>\u02ea"+
		"\n>\3>\3>\5>\u02ee\n>\5>\u02f0\n>\3?\5?\u02f3\n?\3?\3?\3@\3@\3@\5@\u02fa"+
		"\n@\3@\3@\3@\3A\3A\3A\3A\5A\u0303\nA\3B\3B\3B\3B\5B\u0309\nB\3C\3C\7C"+
		"\u030d\nC\fC\16C\u0310\13C\3D\3D\3D\3E\5E\u0316\nE\3E\3E\3E\3E\3E\5E\u031d"+
		"\nE\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\3F\5F\u032b\nF\3G\3G\3H\3H\3H\3H"+
		"\3H\3H\7H\u0335\nH\fH\16H\u0338\13H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3"+
		"H\6H\u0346\nH\rH\16H\u0347\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3H\3"+
		"H\3H\3H\3H\3H\3H\7H\u035e\nH\fH\16H\u0361\13H\3H\3H\3H\3H\5H\u0367\nH"+
		"\5H\u0369\nH\3H\3H\3H\3H\3H\3H\3H\3H\3H\7H\u0374\nH\fH\16H\u0377\13H\3"+
		"I\3I\3I\3I\3I\3J\3J\3K\5K\u0381\nK\3K\3K\3K\5K\u0386\nK\3K\5K\u0389\n"+
		"K\3K\3\u016b\4n\u008eL\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,"+
		".\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086"+
		"\u0088\u008a\u008c\u008e\u0090\u0092\u0094\2\r\5\2  \60\60<<\3\2Z[\4\2"+
		"\24\2499\4\2\3\3BB\4\2VV]]\4\288>>\3\2MN\3\2JK\4\2!!FF\4\2\36\36II\5\2"+
		"GGaacc\u03be\2\u00ae\3\2\2\2\4\u00b0\3\2\2\2\6\u00b4\3\2\2\2\b\u00c9\3"+
		"\2\2\2\n\u00d5\3\2\2\2\f\u00db\3\2\2\2\16\u00f8\3\2\2\2\20\u010a\3\2\2"+
		"\2\22\u0119\3\2\2\2\24\u0121\3\2\2\2\26\u012b\3\2\2\2\30\u0138\3\2\2\2"+
		"\32\u013a\3\2\2\2\34\u013c\3\2\2\2\36\u014a\3\2\2\2 \u014c\3\2\2\2\"\u015d"+
		"\3\2\2\2$\u016b\3\2\2\2&\u0170\3\2\2\2(\u0174\3\2\2\2*\u017d\3\2\2\2,"+
		"\u0188\3\2\2\2.\u018c\3\2\2\2\60\u0197\3\2\2\2\62\u019d\3\2\2\2\64\u01aa"+
		"\3\2\2\2\66\u01b3\3\2\2\28\u01b7\3\2\2\2:\u01bd\3\2\2\2<\u01c1\3\2\2\2"+
		">\u01d4\3\2\2\2@\u01d8\3\2\2\2B\u01e3\3\2\2\2D\u01e8\3\2\2\2F\u01ed\3"+
		"\2\2\2H\u01f1\3\2\2\2J\u01f9\3\2\2\2L\u0206\3\2\2\2N\u0209\3\2\2\2P\u0211"+
		"\3\2\2\2R\u0213\3\2\2\2T\u0218\3\2\2\2V\u021a\3\2\2\2X\u021c\3\2\2\2Z"+
		"\u021e\3\2\2\2\\\u0220\3\2\2\2^\u022b\3\2\2\2`\u0243\3\2\2\2b\u0251\3"+
		"\2\2\2d\u0259\3\2\2\2f\u026a\3\2\2\2h\u0279\3\2\2\2j\u0289\3\2\2\2l\u028b"+
		"\3\2\2\2n\u02b1\3\2\2\2p\u02c9\3\2\2\2r\u02cd\3\2\2\2t\u02d0\3\2\2\2v"+
		"\u02da\3\2\2\2x\u02dd\3\2\2\2z\u02ef\3\2\2\2|\u02f2\3\2\2\2~\u02f6\3\2"+
		"\2\2\u0080\u0302\3\2\2\2\u0082\u0308\3\2\2\2\u0084\u030a\3\2\2\2\u0086"+
		"\u0311\3\2\2\2\u0088\u0315\3\2\2\2\u008a\u032a\3\2\2\2\u008c\u032c\3\2"+
		"\2\2\u008e\u0368\3\2\2\2\u0090\u0378\3\2\2\2\u0092\u037d\3\2\2\2\u0094"+
		"\u0388\3\2\2\2\u0096\u00af\5\6\4\2\u0097\u00af\5\b\5\2\u0098\u00af\5\n"+
		"\6\2\u0099\u00af\5\f\7\2\u009a\u00af\5\16\b\2\u009b\u00af\5<\37\2\u009c"+
		"\u00af\58\35\2\u009d\u00af\5\66\34\2\u009e\u00af\5> \2\u009f\u00af\5:"+
		"\36\2\u00a0\u00af\5@!\2\u00a1\u00af\5\4\3\2\u00a2\u00a3\7\64\2\2\u00a3"+
		"\u00a8\5^\60\2\u00a4\u00a5\7\17\2\2\u00a5\u00a7\5^\60\2\u00a6\u00a4\3"+
		"\2\2\2\u00a7\u00aa\3\2\2\2\u00a8\u00a6\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9"+
		"\u00ac\3\2\2\2\u00aa\u00a8\3\2\2\2\u00ab\u00a2\3\2\2\2\u00ab\u00ac\3\2"+
		"\2\2\u00ac\u00ad\3\2\2\2\u00ad\u00af\5`\61\2\u00ae\u0096\3\2\2\2\u00ae"+
		"\u0097\3\2\2\2\u00ae\u0098\3\2\2\2\u00ae\u0099\3\2\2\2\u00ae\u009a\3\2"+
		"\2\2\u00ae\u009b\3\2\2\2\u00ae\u009c\3\2\2\2\u00ae\u009d\3\2\2\2\u00ae"+
		"\u009e\3\2\2\2\u00ae\u009f\3\2\2\2\u00ae\u00a0\3\2\2\2\u00ae\u00a1\3\2"+
		"\2\2\u00ae\u00ab\3\2\2\2\u00af\3\3\2\2\2\u00b0\u00b1\7@\2\2\u00b1\u00b2"+
		"\7*\2\2\u00b2\u00b3\5\u0080A\2\u00b3\5\3\2\2\2\u00b4\u00b5\7\34\2\2\u00b5"+
		"\u00b6\7(\2\2\u00b6\u00c7\5\u0080A\2\u00b7\u00b9\7\6\2\2\u00b8\u00b7\3"+
		"\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00c8\5`\61\2\u00bb"+
		"\u00bc\7\23\2\2\u00bc\u00bd\7\16\2\2\u00bd\u00c2\5\u008eH\2\u00be\u00bf"+
		"\7\17\2\2\u00bf\u00c1\5\u008eH\2\u00c0\u00be\3\2\2\2\u00c1\u00c4\3\2\2"+
		"\2\u00c2\u00c0\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\u00c5\3\2\2\2\u00c4\u00c2"+
		"\3\2\2\2\u00c5\u00c6\7C\2\2\u00c6\u00c8\3\2\2\2\u00c7\u00b8\3\2\2\2\u00c7"+
		"\u00bb\3\2\2\2\u00c8\7\3\2\2\2\u00c9\u00ca\7\25\2\2\u00ca\u00cb\5\u0080"+
		"A\2\u00cb\u00ce\7D\2\2\u00cc\u00cf\5\u0082B\2\u00cd\u00cf\5\\/\2\u00ce"+
		"\u00cc\3\2\2\2\u00ce\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d1\7J"+
		"\2\2\u00d1\u00d3\5\u008eH\2\u00d2\u00d4\5r:\2\u00d3\u00d2\3\2\2\2\u00d3"+
		"\u00d4\3\2\2\2\u00d4\t\3\2\2\2\u00d5\u00d6\7\26\2\2\u00d6\u00d7\7\6\2"+
		"\2\u00d7\u00d9\5\u0080A\2\u00d8\u00da\5r:\2\u00d9\u00d8\3\2\2\2\u00d9"+
		"\u00da\3\2\2\2\u00da\13\3\2\2\2\u00db\u00dd\7\37\2\2\u00dc\u00de\7U\2"+
		"\2\u00dd\u00dc\3\2\2\2\u00dd\u00de\3\2\2\2\u00de\u00df\3\2\2\2\u00df\u00e0"+
		"\7#\2\2\u00e0\u00e1\5\u0080A\2\u00e1\u00e2\7\16\2\2\u00e2\u00e7\5H%\2"+
		"\u00e3\u00e4\7\17\2\2\u00e4\u00e6\5H%\2\u00e5\u00e3\3\2\2\2\u00e6\u00e9"+
		"\3\2\2\2\u00e7\u00e5\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8\u00ec\3\2\2\2\u00e9"+
		"\u00e7\3\2\2\2\u00ea\u00eb\7\17\2\2\u00eb\u00ed\5J&\2\u00ec\u00ea\3\2"+
		"\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\3\2\2\2\u00ee\u00f0\7C\2\2\u00ef"+
		"\u00f1\5\34\17\2\u00f0\u00ef\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f3\3"+
		"\2\2\2\u00f2\u00f4\5\36\20\2\u00f3\u00f2\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4"+
		"\u00f5\3\2\2\2\u00f5\u00f6\5&\24\2\u00f6\u00f7\5\64\33\2\u00f7\r\3\2\2"+
		"\2\u00f8\u00f9\7\37\2\2\u00f9\u00fa\7?\2\2\u00fa\u00fb\7#\2\2\u00fb\u00fc"+
		"\5\u0080A\2\u00fc\u00fd\7\16\2\2\u00fd\u0102\5H%\2\u00fe\u00ff\7\17\2"+
		"\2\u00ff\u0101\5H%\2\u0100\u00fe\3\2\2\2\u0101\u0104\3\2\2\2\u0102\u0100"+
		"\3\2\2\2\u0102\u0103\3\2\2\2\u0103\u0105\3\2\2\2\u0104\u0102\3\2\2\2\u0105"+
		"\u0108\7C\2\2\u0106\u0109\5\20\t\2\u0107\u0109\5\22\n\2\u0108\u0106\3"+
		"\2\2\2\u0108\u0107\3\2\2\2\u0109\17\3\2\2\2\u010a\u010b\7\5\2\2\u010b"+
		"\u010c\7\6\2\2\u010c\u010d\5\32\16\2\u010d\u010e\7\35\2\2\u010e\u010f"+
		"\7A\2\2\u010f\u0110\7%\2\2\u0110\u0111\5\30\r\2\u0111\u0112\7>\2\2\u0112"+
		"\u0113\7A\2\2\u0113\u0114\7%\2\2\u0114\u0115\5\30\r\2\u0115\u0116\7\13"+
		"\2\2\u0116\u0117\7\33\2\2\u0117\u0118\7e\2\2\u0118\21\3\2\2\2\u0119\u011a"+
		"\7\7\2\2\u011a\u011b\5\24\13\2\u011b\u011c\7\64\2\2\u011c\u011d\7\n\2"+
		"\2\u011d\u011e\7\16\2\2\u011e\u011f\5\26\f\2\u011f\u0120\7C\2\2\u0120"+
		"\23\3\2\2\2\u0121\u0126\7d\2\2\u0122\u0123\7\66\2\2\u0123\u0125\7d\2\2"+
		"\u0124\u0122\3\2\2\2\u0125\u0128\3\2\2\2\u0126\u0124\3\2\2\2\u0126\u0127"+
		"\3\2\2\2\u0127\u0129\3\2\2\2\u0128\u0126\3\2\2\2\u0129\u012a\7/\2\2\u012a"+
		"\25\3\2\2\2\u012b\u012c\5\30\r\2\u012c\u012d\7:\2\2\u012d\u0135\5\30\r"+
		"\2\u012e\u012f\7\17\2\2\u012f\u0130\5\30\r\2\u0130\u0131\7:\2\2\u0131"+
		"\u0132\5\30\r\2\u0132\u0134\3\2\2\2\u0133\u012e\3\2\2\2\u0134\u0137\3"+
		"\2\2\2\u0135\u0133\3\2\2\2\u0135\u0136\3\2\2\2\u0136\27\3\2\2\2\u0137"+
		"\u0135\3\2\2\2\u0138\u0139\13\2\2\2\u0139\31\3\2\2\2\u013a\u013b\t\2\2"+
		"\2\u013b\33\3\2\2\2\u013c\u013d\7b\2\2\u013d\u013e\7\16\2\2\u013e\u0143"+
		"\7W\2\2\u013f\u0140\7\17\2\2\u0140\u0142\7W\2\2\u0141\u013f\3\2\2\2\u0142"+
		"\u0145\3\2\2\2\u0143\u0141\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0146\3\2"+
		"\2\2\u0145\u0143\3\2\2\2\u0146\u0147\7C\2\2\u0147\35\3\2\2\2\u0148\u014b"+
		"\7\\\2\2\u0149\u014b\5 \21\2\u014a\u0148\3\2\2\2\u014a\u0149\3\2\2\2\u014b"+
		"\37\3\2\2\2\u014c\u014d\7\t\2\2\u014d\u0152\5\"\22\2\u014e\u014f\7=\2"+
		"\2\u014f\u0151\5\"\22\2\u0150\u014e\3\2\2\2\u0151\u0154\3\2\2\2\u0152"+
		"\u0150\3\2\2\2\u0152\u0153\3\2\2\2\u0153\u0155\3\2\2\2\u0154\u0152\3\2"+
		"\2\2\u0155\u015b\7,\2\2\u0156\u0159\7\17\2\2\u0157\u015a\5,\27\2\u0158"+
		"\u015a\5\60\31\2\u0159\u0157\3\2\2\2\u0159\u0158\3\2\2\2\u015a\u015c\3"+
		"\2\2\2\u015b\u0156\3\2\2\2\u015b\u015c\3\2\2\2\u015c!\3\2\2\2\u015d\u015e"+
		"\7\t\2\2\u015e\u0163\7W\2\2\u015f\u0160\7=\2\2\u0160\u0162\7W\2\2\u0161"+
		"\u015f\3\2\2\2\u0162\u0165\3\2\2\2\u0163\u0161\3\2\2\2\u0163\u0164\3\2"+
		"\2\2\u0164\u0166\3\2\2\2\u0165\u0163\3\2\2\2\u0166\u0167\7,\2\2\u0167"+
		"#\3\2\2\2\u0168\u016a\13\2\2\2\u0169\u0168\3\2\2\2\u016a\u016d\3\2\2\2"+
		"\u016b\u016c\3\2\2\2\u016b\u0169\3\2\2\2\u016c%\3\2\2\2\u016d\u016b\3"+
		"\2\2\2\u016e\u0171\7^\2\2\u016f\u0171\5(\25\2\u0170\u016e\3\2\2\2\u0170"+
		"\u016f\3\2\2\2\u0171\'\3\2\2\2\u0172\u0175\7]\2\2\u0173\u0175\5*\26\2"+
		"\u0174\u0172\3\2\2\2\u0174\u0173\3\2\2\2\u0175\u017b\3\2\2\2\u0176\u0179"+
		"\7\17\2\2\u0177\u017a\5,\27\2\u0178\u017a\5\60\31\2\u0179\u0177\3\2\2"+
		"\2\u0179\u0178\3\2\2\2\u017a\u017c\3\2\2\2\u017b\u0176\3\2\2\2\u017b\u017c"+
		"\3\2\2\2\u017c)\3\2\2\2\u017d\u017e\7\t\2\2\u017e\u0183\7W\2\2\u017f\u0180"+
		"\7=\2\2\u0180\u0182\7W\2\2\u0181\u017f\3\2\2\2\u0182\u0185\3\2\2\2\u0183"+
		"\u0181\3\2\2\2\u0183\u0184\3\2\2\2\u0184\u0186\3\2\2\2\u0185\u0183\3\2"+
		"\2\2\u0186\u0187\7,\2\2\u0187+\3\2\2\2\u0188\u0189\7_\2\2\u0189\u018a"+
		"\7\17\2\2\u018a\u018b\5.\30\2\u018b-\3\2\2\2\u018c\u018d\7\t\2\2\u018d"+
		"\u0192\5\u0082B\2\u018e\u018f\7=\2\2\u018f\u0191\5\u0082B\2\u0190\u018e"+
		"\3\2\2\2\u0191\u0194\3\2\2\2\u0192\u0190\3\2\2\2\u0192\u0193\3\2\2\2\u0193"+
		"\u0195\3\2\2\2\u0194\u0192\3\2\2\2\u0195\u0196\7,\2\2\u0196/\3\2\2\2\u0197"+
		"\u0198\7`\2\2\u0198\u0199\7\17\2\2\u0199\u019a\5\u0082B\2\u019a\u019b"+
		"\7\17\2\2\u019b\u019c\5\62\32\2\u019c\61\3\2\2\2\u019d\u019e\7\t\2\2\u019e"+
		"\u01a3\5$\23\2\u019f\u01a0\7=\2\2\u01a0\u01a2\5$\23\2\u01a1\u019f\3\2"+
		"\2\2\u01a2\u01a5\3\2\2\2\u01a3\u01a1\3\2\2\2\u01a3\u01a4\3\2\2\2\u01a4"+
		"\u01a6\3\2\2\2\u01a5\u01a3\3\2\2\2\u01a6\u01a7\7,\2\2\u01a7\63\3\2\2\2"+
		"\u01a8\u01ab\7]\2\2\u01a9\u01ab\5*\26\2\u01aa\u01a8\3\2\2\2\u01aa\u01a9"+
		"\3\2\2\2\u01ab\u01b1\3\2\2\2\u01ac\u01af\7\17\2\2\u01ad\u01b0\5,\27\2"+
		"\u01ae\u01b0\5$\23\2\u01af\u01ad\3\2\2\2\u01af\u01ae\3\2\2\2\u01b0\u01b2"+
		"\3\2\2\2\u01b1\u01ac\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2\65\3\2\2\2\u01b3"+
		"\u01b4\7\65\2\2\u01b4\u01b5\7#\2\2\u01b5\u01b6\5\u0080A\2\u01b6\67\3\2"+
		"\2\2\u01b7\u01b8\7\37\2\2\u01b8\u01b9\7-\2\2\u01b9\u01ba\5\u0080A\2\u01ba"+
		"\u01bb\7$\2\2\u01bb\u01bc\5`\61\2\u01bc9\3\2\2\2\u01bd\u01be\7\65\2\2"+
		"\u01be\u01bf\7-\2\2\u01bf\u01c0\5\u0080A\2\u01c0;\3\2\2\2\u01c1\u01c3"+
		"\7\37\2\2\u01c2\u01c4\7Y\2\2\u01c3\u01c2\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4"+
		"\u01c5\3\2\2\2\u01c5\u01c6\7;\2\2\u01c6\u01c7\5\u0080A\2\u01c7\u01c8\7"+
		"*\2\2\u01c8\u01c9\5\u0080A\2\u01c9\u01ca\7\16\2\2\u01ca\u01cf\5F$\2\u01cb"+
		"\u01cc\7\17\2\2\u01cc\u01ce\5F$\2\u01cd\u01cb\3\2\2\2\u01ce\u01d1\3\2"+
		"\2\2\u01cf\u01cd\3\2\2\2\u01cf\u01d0\3\2\2\2\u01d0\u01d2\3\2\2\2\u01d1"+
		"\u01cf\3\2\2\2\u01d2\u01d3\7C\2\2\u01d3=\3\2\2\2\u01d4\u01d5\7\65\2\2"+
		"\u01d5\u01d6\7;\2\2\u01d6\u01d7\5\u0080A\2\u01d7?\3\2\2\2\u01d8\u01d9"+
		"\7\22\2\2\u01d9\u01da\t\3\2\2\u01da\u01db\7(\2\2\u01db\u01de\5\u0080A"+
		"\2\u01dc\u01dd\7\21\2\2\u01dd\u01df\5B\"\2\u01de\u01dc\3\2\2\2\u01de\u01df"+
		"\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0\u01e1\7\6\2\2\u01e1\u01e2\5D#\2\u01e2"+
		"A\3\2\2\2\u01e3\u01e4\13\2\2\2\u01e4C\3\2\2\2\u01e5\u01e7\13\2\2\2\u01e6"+
		"\u01e5\3\2\2\2\u01e7\u01ea\3\2\2\2\u01e8\u01e6\3\2\2\2\u01e8\u01e9\3\2"+
		"\2\2\u01e9\u01eb\3\2\2\2\u01ea\u01e8\3\2\2\2\u01eb\u01ec\7\2\2\3\u01ec"+
		"E\3\2\2\2\u01ed\u01ef\5\u0082B\2\u01ee\u01f0\7Q\2\2\u01ef\u01ee\3\2\2"+
		"\2\u01ef\u01f0\3\2\2\2\u01f0G\3\2\2\2\u01f1\u01f2\5\u0082B\2\u01f2\u01f4"+
		"\5P)\2\u01f3\u01f5\5L\'\2\u01f4\u01f3\3\2\2\2\u01f4\u01f5\3\2\2\2\u01f5"+
		"\u01f7\3\2\2\2\u01f6\u01f8\5N(\2\u01f7\u01f6\3\2\2\2\u01f7\u01f8\3\2\2"+
		"\2\u01f8I\3\2\2\2\u01f9\u01fa\7\20\2\2\u01fa\u01fb\7\61\2\2\u01fb\u01fc"+
		"\7\16\2\2\u01fc\u0201\5\u0082B\2\u01fd\u01fe\7\17\2\2\u01fe\u0200\5\u0082"+
		"B\2\u01ff\u01fd\3\2\2\2\u0200\u0203\3\2\2\2\u0201\u01ff\3\2\2\2\u0201"+
		"\u0202\3\2\2\2\u0202\u0204\3\2\2\2\u0203\u0201\3\2\2\2\u0204\u0205\7C"+
		"\2\2\u0205K\3\2\2\2\u0206\u0207\7O\2\2\u0207\u0208\7P\2\2\u0208M\3\2\2"+
		"\2\u0209\u020a\7\20\2\2\u020a\u020b\7\61\2\2\u020bO\3\2\2\2\u020c\u0212"+
		"\5R*\2\u020d\u0212\5T+\2\u020e\u0212\5V,\2\u020f\u0212\5X-\2\u0210\u0212"+
		"\5Z.\2\u0211\u020c\3\2\2\2\u0211\u020d\3\2\2\2\u0211\u020e\3\2\2\2\u0211"+
		"\u020f\3\2\2\2\u0211\u0210\3\2\2\2\u0212Q\3\2\2\2\u0213\u0214\t\4\2\2"+
		"\u0214\u0215\7\16\2\2\u0215\u0216\7W\2\2\u0216\u0217\7C\2\2\u0217S\3\2"+
		"\2\2\u0218\u0219\7\4\2\2\u0219U\3\2\2\2\u021a\u021b\7\27\2\2\u021bW\3"+
		"\2\2\2\u021c\u021d\7a\2\2\u021dY\3\2\2\2\u021e\u021f\t\5\2\2\u021f[\3"+
		"\2\2\2\u0220\u0221\7\16\2\2\u0221\u0226\5\u0082B\2\u0222\u0223\7\17\2"+
		"\2\u0223\u0225\5\u0082B\2\u0224\u0222\3\2\2\2\u0225\u0228\3\2\2\2\u0226"+
		"\u0224\3\2\2\2\u0226\u0227\3\2\2\2\u0227\u0229\3\2\2\2\u0228\u0226\3\2"+
		"\2\2\u0229\u022a\7C\2\2\u022a]\3\2\2\2\u022b\u0237\7c\2\2\u022c\u022d"+
		"\7\16\2\2\u022d\u0232\5\u0082B\2\u022e\u022f\7\17\2\2\u022f\u0231\5\u0082"+
		"B\2\u0230\u022e\3\2\2\2\u0231\u0234\3\2\2\2\u0232\u0230\3\2\2\2\u0232"+
		"\u0233\3\2\2\2\u0233\u0235\3\2\2\2\u0234\u0232\3\2\2\2\u0235\u0236\7C"+
		"\2\2\u0236\u0238\3\2\2\2\u0237\u022c\3\2\2\2\u0237\u0238\3\2\2\2\u0238"+
		"\u0239\3\2\2\2\u0239\u023a\7$\2\2\u023a\u023b\7\16\2\2\u023b\u023c\5`"+
		"\61\2\u023c\u023d\7C\2\2\u023d_\3\2\2\2\u023e\u0244\5d\63\2\u023f\u0240"+
		"\7\16\2\2\u0240\u0241\5`\61\2\u0241\u0242\7C\2\2\u0242\u0244\3\2\2\2\u0243"+
		"\u023e\3\2\2\2\u0243\u023f\3\2\2\2\u0244\u0248\3\2\2\2\u0245\u0247\5b"+
		"\62\2\u0246\u0245\3\2\2\2\u0247\u024a\3\2\2\2\u0248\u0246\3\2\2\2\u0248"+
		"\u0249\3\2\2\2\u0249\u024c\3\2\2\2\u024a\u0248\3\2\2\2\u024b\u024d\5x"+
		"=\2\u024c\u024b\3\2\2\2\u024c\u024d\3\2\2\2\u024d\u024f\3\2\2\2\u024e"+
		"\u0250\5~@\2\u024f\u024e\3\2\2\2\u024f\u0250\3\2\2\2\u0250a\3\2\2\2\u0251"+
		"\u0257\7T\2\2\u0252\u0258\5d\63\2\u0253\u0254\7\16\2\2\u0254\u0255\5`"+
		"\61\2\u0255\u0256\7C\2\2\u0256\u0258\3\2\2\2\u0257\u0252\3\2\2\2\u0257"+
		"\u0253\3\2\2\2\u0258c\3\2\2\2\u0259\u025a\5f\64\2\u025a\u025c\5l\67\2"+
		"\u025b\u025d\5r:\2\u025c\u025b\3\2\2\2\u025c\u025d\3\2\2\2\u025d\u025f"+
		"\3\2\2\2\u025e\u0260\5t;\2\u025f\u025e\3\2\2\2\u025f\u0260\3\2\2\2\u0260"+
		"\u0262\3\2\2\2\u0261\u0263\5v<\2\u0262\u0261\3\2\2\2\u0262\u0263\3\2\2"+
		"\2\u0263\u0265\3\2\2\2\u0264\u0266\5x=\2\u0265\u0264\3\2\2\2\u0265\u0266"+
		"\3\2\2\2\u0266\u0268\3\2\2\2\u0267\u0269\5~@\2\u0268\u0267\3\2\2\2\u0268"+
		"\u0269\3\2\2\2\u0269e\3\2\2\2\u026a\u026c\7\63\2\2\u026b\u026d\5h\65\2"+
		"\u026c\u026b\3\2\2\2\u026c\u026d\3\2\2\2\u026d\u0277\3\2\2\2\u026e\u0278"+
		"\7F\2\2\u026f\u0274\5j\66\2\u0270\u0271\7\17\2\2\u0271\u0273\5j\66\2\u0272"+
		"\u0270\3\2\2\2\u0273\u0276\3\2\2\2\u0274\u0272\3\2\2\2\u0274\u0275\3\2"+
		"\2\2\u0275\u0278\3\2\2\2\u0276\u0274\3\2\2\2\u0277\u026e\3\2\2\2\u0277"+
		"\u026f\3\2\2\2\u0278g\3\2\2\2\u0279\u027a\t\6\2\2\u027ai\3\2\2\2\u027b"+
		"\u027d\5\u0082B\2\u027c\u027e\7$\2\2\u027d\u027c\3\2\2\2\u027d\u027e\3"+
		"\2\2\2\u027e\u0280\3\2\2\2\u027f\u0281\7c\2\2\u0280\u027f\3\2\2\2\u0280"+
		"\u0281\3\2\2\2\u0281\u028a\3\2\2\2\u0282\u0284\5\u008eH\2\u0283\u0285"+
		"\7$\2\2\u0284\u0283\3\2\2\2\u0284\u0285\3\2\2\2\u0285\u0287\3\2\2\2\u0286"+
		"\u0288\7c\2\2\u0287\u0286\3\2\2\2\u0287\u0288\3\2\2\2\u0288\u028a\3\2"+
		"\2\2\u0289\u027b\3\2\2\2\u0289\u0282\3\2\2\2\u028ak\3\2\2\2\u028b\u028c"+
		"\7\6\2\2\u028c\u0291\5n8\2\u028d\u028e\7\17\2\2\u028e\u0290\5n8\2\u028f"+
		"\u028d\3\2\2\2\u0290\u0293\3\2\2\2\u0291\u028f\3\2\2\2\u0291\u0292\3\2"+
		"\2\2\u0292m\3\2\2\2\u0293\u0291\3\2\2\2\u0294\u0295\b8\1\2\u0295\u0296"+
		"\7\16\2\2\u0296\u0298\5n8\2\u0297\u0299\7R\2\2\u0298\u0297\3\2\2\2\u0298"+
		"\u0299\3\2\2\2\u0299\u029a\3\2\2\2\u029a\u029b\7+\2\2\u029b\u029c\5n8"+
		"\2\u029c\u029d\7*\2\2\u029d\u029e\5\u0084C\2\u029e\u02a0\7C\2\2\u029f"+
		"\u02a1\5|?\2\u02a0\u029f\3\2\2\2\u02a0\u02a1\3\2\2\2\u02a1\u02b2\3\2\2"+
		"\2\u02a2\u02a3\7\16\2\2\u02a3\u02a4\5n8\2\u02a4\u02a5\7S\2\2\u02a5\u02a6"+
		"\5n8\2\u02a6\u02a8\7C\2\2\u02a7\u02a9\5|?\2\u02a8\u02a7\3\2\2\2\u02a8"+
		"\u02a9\3\2\2\2\u02a9\u02b2\3\2\2\2\u02aa\u02ab\7\16\2\2\u02ab\u02ac\5"+
		"`\61\2\u02ac\u02ae\7C\2\2\u02ad\u02af\5|?\2\u02ae\u02ad\3\2\2\2\u02ae"+
		"\u02af\3\2\2\2\u02af\u02b2\3\2\2\2\u02b0\u02b2\5p9\2\u02b1\u0294\3\2\2"+
		"\2\u02b1\u02a2\3\2\2\2\u02b1\u02aa\3\2\2\2\u02b1\u02b0\3\2\2\2\u02b2\u02c6"+
		"\3\2\2\2\u02b3\u02b5\f\b\2\2\u02b4\u02b6\7R\2\2\u02b5\u02b4\3\2\2\2\u02b5"+
		"\u02b6\3\2\2\2\u02b6\u02b7\3\2\2\2\u02b7\u02b8\7+\2\2\u02b8\u02b9\5n8"+
		"\2\u02b9\u02ba\7*\2\2\u02ba\u02bc\5\u0084C\2\u02bb\u02bd\5|?\2\u02bc\u02bb"+
		"\3\2\2\2\u02bc\u02bd\3\2\2\2\u02bd\u02c5\3\2\2\2\u02be\u02bf\f\7\2\2\u02bf"+
		"\u02c0\7S\2\2\u02c0\u02c2\5n8\2\u02c1\u02c3\5|?\2\u02c2\u02c1\3\2\2\2"+
		"\u02c2\u02c3\3\2\2\2\u02c3\u02c5\3\2\2\2\u02c4\u02b3\3\2\2\2\u02c4\u02be"+
		"\3\2\2\2\u02c5\u02c8\3\2\2\2\u02c6\u02c4\3\2\2\2\u02c6\u02c7\3\2\2\2\u02c7"+
		"o\3\2\2\2\u02c8\u02c6\3\2\2\2\u02c9\u02cb\5\u0080A\2\u02ca\u02cc\5|?\2"+
		"\u02cb\u02ca\3\2\2\2\u02cb\u02cc\3\2\2\2\u02ccq\3\2\2\2\u02cd\u02ce\7"+
		"\'\2\2\u02ce\u02cf\5\u0084C\2\u02cfs\3\2\2\2\u02d0\u02d1\7\f\2\2\u02d1"+
		"\u02d2\7%\2\2\u02d2\u02d7\5\u0082B\2\u02d3\u02d4\7\17\2\2\u02d4\u02d6"+
		"\5\u0082B\2\u02d5\u02d3\3\2\2\2\u02d6\u02d9\3\2\2\2\u02d7\u02d5\3\2\2"+
		"\2\u02d7\u02d8\3\2\2\2\u02d8u\3\2\2\2\u02d9\u02d7\3\2\2\2\u02da\u02db"+
		"\7\32\2\2\u02db\u02dc\5\u0084C\2\u02dcw\3\2\2\2\u02dd\u02de\7\62\2\2\u02de"+
		"\u02df\7%\2\2\u02df\u02e4\5z>\2\u02e0\u02e1\7\17\2\2\u02e1\u02e3\5z>\2"+
		"\u02e2\u02e0\3\2\2\2\u02e3\u02e6\3\2\2\2\u02e4\u02e2\3\2\2\2\u02e4\u02e5"+
		"\3\2\2\2\u02e5y\3\2\2\2\u02e6\u02e4\3\2\2\2\u02e7\u02e9\7W\2\2\u02e8\u02ea"+
		"\7Q\2\2\u02e9\u02e8\3\2\2\2\u02e9\u02ea\3\2\2\2\u02ea\u02f0\3\2\2\2\u02eb"+
		"\u02ed\5\u0082B\2\u02ec\u02ee\7Q\2\2\u02ed\u02ec\3\2\2\2\u02ed\u02ee\3"+
		"\2\2\2\u02ee\u02f0\3\2\2\2\u02ef\u02e7\3\2\2\2\u02ef\u02eb\3\2\2\2\u02f0"+
		"{\3\2\2\2\u02f1\u02f3\7$\2\2\u02f2\u02f1\3\2\2\2\u02f2\u02f3\3\2\2\2\u02f3"+
		"\u02f4\3\2\2\2\u02f4\u02f5\7c\2\2\u02f5}\3\2\2\2\u02f6\u02f7\7\31\2\2"+
		"\u02f7\u02f9\7\30\2\2\u02f8\u02fa\7W\2\2\u02f9\u02f8\3\2\2\2\u02f9\u02fa"+
		"\3\2\2\2\u02fa\u02fb\3\2\2\2\u02fb\u02fc\t\7\2\2\u02fc\u02fd\7\"\2\2\u02fd"+
		"\177\3\2\2\2\u02fe\u0303\7c\2\2\u02ff\u0300\7c\2\2\u0300\u0301\7\66\2"+
		"\2\u0301\u0303\7c\2\2\u0302\u02fe\3\2\2\2\u0302\u02ff\3\2\2\2\u0303\u0081"+
		"\3\2\2\2\u0304\u0309\7c\2\2\u0305\u0306\7c\2\2\u0306\u0307\7\66\2\2\u0307"+
		"\u0309\7c\2\2\u0308\u0304\3\2\2\2\u0308\u0305\3\2\2\2\u0309\u0083\3\2"+
		"\2\2\u030a\u030e\5\u0088E\2\u030b\u030d\5\u0086D\2\u030c\u030b\3\2\2\2"+
		"\u030d\u0310\3\2\2\2\u030e\u030c\3\2\2\2\u030e\u030f\3\2\2\2\u030f\u0085"+
		"\3\2\2\2\u0310\u030e\3\2\2\2\u0311\u0312\t\b\2\2\u0312\u0313\5\u0088E"+
		"\2\u0313\u0087\3\2\2\2\u0314\u0316\7O\2\2\u0315\u0314\3\2\2\2\u0315\u0316"+
		"\3\2\2\2\u0316\u031c\3\2\2\2\u0317\u031d\5\u008aF\2\u0318\u0319\7\16\2"+
		"\2\u0319\u031a\5\u0084C\2\u031a\u031b\7C\2\2\u031b\u031d\3\2\2\2\u031c"+
		"\u0317\3\2\2\2\u031c\u0318\3\2\2\2\u031d\u0089\3\2\2\2\u031e\u031f\5\u008e"+
		"H\2\u031f\u0320\5\u008cG\2\u0320\u0321\5\u008eH\2\u0321\u032b\3\2\2\2"+
		"\u0322\u0323\5\u008eH\2\u0323\u0324\7L\2\2\u0324\u032b\3\2\2\2\u0325\u0326"+
		"\7\b\2\2\u0326\u0327\7\16\2\2\u0327\u0328\5d\63\2\u0328\u0329\7C\2\2\u0329"+
		"\u032b\3\2\2\2\u032a\u031e\3\2\2\2\u032a\u0322\3\2\2\2\u032a\u0325\3\2"+
		"\2\2\u032b\u008b\3\2\2\2\u032c\u032d\t\t\2\2\u032d\u008d\3\2\2\2\u032e"+
		"\u032f\bH\1\2\u032f\u0330\5\u0092J\2\u0330\u0331\7\16\2\2\u0331\u0336"+
		"\5\u008eH\2\u0332\u0333\7\17\2\2\u0333\u0335\5\u008eH\2\u0334\u0332\3"+
		"\2\2\2\u0335\u0338\3\2\2\2\u0336\u0334\3\2\2\2\u0336\u0337\3\2\2\2\u0337"+
		"\u0339\3\2\2\2\u0338\u0336\3\2\2\2\u0339\u033a\7C\2\2\u033a\u0369\3\2"+
		"\2\2\u033b\u033c\7G\2\2\u033c\u033d\7\16\2\2\u033d\u033e\7V\2\2\u033e"+
		"\u033f\5\u008eH\2\u033f\u0340\7C\2\2\u0340\u0369\3\2\2\2\u0341\u0342\7"+
		"\16\2\2\u0342\u0345\5\u008eH\2\u0343\u0344\7\17\2\2\u0344\u0346\5\u008e"+
		"H\2\u0345\u0343\3\2\2\2\u0346\u0347\3\2\2\2\u0347\u0345\3\2\2\2\u0347"+
		"\u0348\3\2\2\2\u0348\u0349\3\2\2\2\u0349\u034a\7C\2\2\u034a\u0369\3\2"+
		"\2\2\u034b\u034c\7G\2\2\u034c\u034d\7\16\2\2\u034d\u034e\7F\2\2\u034e"+
		"\u0369\7C\2\2\u034f\u0369\5\u0094K\2\u0350\u0369\5\u0082B\2\u0351\u0352"+
		"\7\16\2\2\u0352\u0353\5d\63\2\u0353\u0354\7C\2\2\u0354\u0369\3\2\2\2\u0355"+
		"\u0356\7\16\2\2\u0356\u0357\5\u008eH\2\u0357\u0358\7C\2\2\u0358\u0369"+
		"\3\2\2\2\u0359\u0369\7P\2\2\u035a\u035b\7\r\2\2\u035b\u035f\5\u0090I\2"+
		"\u035c\u035e\5\u0090I\2\u035d\u035c\3\2\2\2\u035e\u0361\3\2\2\2\u035f"+
		"\u035d\3\2\2\2\u035f\u0360\3\2\2\2\u0360\u0362\3\2\2\2\u0361\u035f\3\2"+
		"\2\2\u0362\u0363\7&\2\2\u0363\u0364\5\u008eH\2\u0364\u0366\7)\2\2\u0365"+
		"\u0367\7\r\2\2\u0366\u0365\3\2\2\2\u0366\u0367\3\2\2\2\u0367\u0369\3\2"+
		"\2\2\u0368\u032e\3\2\2\2\u0368\u033b\3\2\2\2\u0368\u0341\3\2\2\2\u0368"+
		"\u034b\3\2\2\2\u0368\u034f\3\2\2\2\u0368\u0350\3\2\2\2\u0368\u0351\3\2"+
		"\2\2\u0368\u0355\3\2\2\2\u0368\u0359\3\2\2\2\u0368\u035a\3\2\2\2\u0369"+
		"\u0375\3\2\2\2\u036a\u036b\f\17\2\2\u036b\u036c\t\n\2\2\u036c\u0374\5"+
		"\u008eH\20\u036d\u036e\f\16\2\2\u036e\u036f\t\13\2\2\u036f\u0374\5\u008e"+
		"H\17\u0370\u0371\f\r\2\2\u0371\u0372\7H\2\2\u0372\u0374\5\u008eH\16\u0373"+
		"\u036a\3\2\2\2\u0373\u036d\3\2\2\2\u0373\u0370\3\2\2\2\u0374\u0377\3\2"+
		"\2\2\u0375\u0373\3\2\2\2\u0375\u0376\3\2\2\2\u0376\u008f\3\2\2\2\u0377"+
		"\u0375\3\2\2\2\u0378\u0379\7\67\2\2\u0379\u037a\5\u0084C\2\u037a\u037b"+
		"\7.\2\2\u037b\u037c\5\u008eH\2\u037c\u0091\3\2\2\2\u037d\u037e\t\f\2\2"+
		"\u037e\u0093\3\2\2\2\u037f\u0381\7I\2\2\u0380\u037f\3\2\2\2\u0380\u0381"+
		"\3\2\2\2\u0381\u0382\3\2\2\2\u0382\u0385\7W\2\2\u0383\u0384\7\66\2\2\u0384"+
		"\u0386\7W\2\2\u0385\u0383\3\2\2\2\u0385\u0386\3\2\2\2\u0386\u0389\3\2"+
		"\2\2\u0387\u0389\7E\2\2\u0388\u0380\3\2\2\2\u0388\u0387\3\2\2\2\u0389"+
		"\u0095\3\2\2\2f\u00a8\u00ab\u00ae\u00b8\u00c2\u00c7\u00ce\u00d3\u00d9"+
		"\u00dd\u00e7\u00ec\u00f0\u00f3\u0102\u0108\u0126\u0135\u0143\u014a\u0152"+
		"\u0159\u015b\u0163\u016b\u0170\u0174\u0179\u017b\u0183\u0192\u01a3\u01aa"+
		"\u01af\u01b1\u01c3\u01cf\u01de\u01e8\u01ef\u01f4\u01f7\u0201\u0211\u0226"+
		"\u0232\u0237\u0243\u0248\u024c\u024f\u0257\u025c\u025f\u0262\u0265\u0268"+
		"\u026c\u0274\u0277\u027d\u0280\u0284\u0287\u0289\u0291\u0298\u02a0\u02a8"+
		"\u02ae\u02b1\u02b5\u02bc\u02c2\u02c4\u02c6\u02cb\u02d7\u02e4\u02e9\u02ed"+
		"\u02ef\u02f2\u02f9\u0302\u0308\u030e\u0315\u031c\u032a\u0336\u0347\u035f"+
		"\u0366\u0368\u0373\u0375\u0380\u0385\u0388";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}