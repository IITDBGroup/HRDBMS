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
		DATE=95, COLORDER=96, ORGANIZATION=97, IDENTIFIER=98, JAVACLASSNAMEIDENTIFIER=99, 
		FILEPATHIDENTIFIER=100, ANY=101;
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
		"'ORGANIZATION'", "IDENTIFIER", "JAVACLASSNAMEIDENTIFIER", "FILEPATHIDENTIFIER", 
		"ANY"
	};
	public static final int
		RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_update = 3, 
		RULE_delete = 4, RULE_createTable = 5, RULE_organization = 6, RULE_createExternalTable = 7, 
		RULE_generalExtTableSpec = 8, RULE_javaClassExtTableSpec = 9, RULE_javaClassName = 10, 
		RULE_keyValueList = 11, RULE_anything = 12, RULE_sourceList = 13, RULE_colOrder = 14, 
		RULE_groupExp = 15, RULE_realGroupExp = 16, RULE_groupDef = 17, RULE_rangeExp = 18, 
		RULE_nodeExp = 19, RULE_realNodeExp = 20, RULE_integerSet = 21, RULE_hashExp = 22, 
		RULE_columnSet = 23, RULE_rangeType = 24, RULE_rangeSet = 25, RULE_deviceExp = 26, 
		RULE_dropTable = 27, RULE_createView = 28, RULE_dropView = 29, RULE_createIndex = 30, 
		RULE_dropIndex = 31, RULE_load = 32, RULE_any = 33, RULE_remainder = 34, 
		RULE_indexDef = 35, RULE_colDef = 36, RULE_primaryKey = 37, RULE_notNull = 38, 
		RULE_primary = 39, RULE_dataType = 40, RULE_char2 = 41, RULE_int2 = 42, 
		RULE_long2 = 43, RULE_date2 = 44, RULE_float2 = 45, RULE_colList = 46, 
		RULE_commonTableExpression = 47, RULE_fullSelect = 48, RULE_connectedSelect = 49, 
		RULE_subSelect = 50, RULE_selectClause = 51, RULE_selecthow = 52, RULE_selectListEntry = 53, 
		RULE_fromClause = 54, RULE_tableReference = 55, RULE_singleTable = 56, 
		RULE_whereClause = 57, RULE_groupBy = 58, RULE_havingClause = 59, RULE_orderBy = 60, 
		RULE_sortKey = 61, RULE_correlationClause = 62, RULE_fetchFirst = 63, 
		RULE_tableName = 64, RULE_columnName = 65, RULE_searchCondition = 66, 
		RULE_connectedSearchClause = 67, RULE_searchClause = 68, RULE_predicate = 69, 
		RULE_operator = 70, RULE_expression = 71, RULE_caseCase = 72, RULE_identifier = 73, 
		RULE_literal = 74;
	public static final String[] ruleNames = {
		"select", "runstats", "insert", "update", "delete", "createTable", "organization", 
		"createExternalTable", "generalExtTableSpec", "javaClassExtTableSpec", 
		"javaClassName", "keyValueList", "anything", "sourceList", "colOrder", 
		"groupExp", "realGroupExp", "groupDef", "rangeExp", "nodeExp", "realNodeExp", 
		"integerSet", "hashExp", "columnSet", "rangeType", "rangeSet", "deviceExp", 
		"dropTable", "createView", "dropView", "createIndex", "dropIndex", "load", 
		"any", "remainder", "indexDef", "colDef", "primaryKey", "notNull", "primary", 
		"dataType", "char2", "int2", "long2", "date2", "float2", "colList", "commonTableExpression", 
		"fullSelect", "connectedSelect", "subSelect", "selectClause", "selecthow", 
		"selectListEntry", "fromClause", "tableReference", "singleTable", "whereClause", 
		"groupBy", "havingClause", "orderBy", "sortKey", "correlationClause", 
		"fetchFirst", "tableName", "columnName", "searchCondition", "connectedSearchClause", 
		"searchClause", "predicate", "operator", "expression", "caseCase", "identifier", 
		"literal"
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
		public TerminalNode EOF() { return getToken(SelectParser.EOF, 0); }
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectContext select() throws RecognitionException {
		SelectContext _localctx = new SelectContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_select);
		int _la;
		try {
			setState(198);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(150); insert();
				setState(151); match(EOF);
				}
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(153); update();
				setState(154); match(EOF);
				}
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(156); delete();
				setState(157); match(EOF);
				}
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(159); createTable();
				setState(160); match(EOF);
				}
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(162); createIndex();
				setState(163); match(EOF);
				}
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(165); createView();
				setState(166); match(EOF);
				}
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				{
				setState(168); dropTable();
				setState(169); match(EOF);
				}
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				{
				setState(171); dropIndex();
				setState(172); match(EOF);
				}
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				{
				setState(174); dropView();
				setState(175); match(EOF);
				}
				}
				break;

			case 10:
				enterOuterAlt(_localctx, 10);
				{
				{
				setState(177); load();
				setState(178); match(EOF);
				}
				}
				break;

			case 11:
				enterOuterAlt(_localctx, 11);
				{
				{
				setState(180); runstats();
				setState(181); match(EOF);
				}
				}
				break;

			case 12:
				enterOuterAlt(_localctx, 12);
				{
				{
				{
				setState(192);
				_la = _input.LA(1);
				if (_la==50) {
					{
					setState(183); match(50);
					setState(184); commonTableExpression();
					setState(189);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==13) {
						{
						{
						setState(185); match(13);
						setState(186); commonTableExpression();
						}
						}
						setState(191);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(194); fullSelect();
				}
				setState(196); match(EOF);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRunstats(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RunstatsContext runstats() throws RecognitionException {
		RunstatsContext _localctx = new RunstatsContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_runstats);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(200); match(62);
			setState(201); match(40);
			setState(202); tableName();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitInsert(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertContext insert() throws RecognitionException {
		InsertContext _localctx = new InsertContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_insert);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(204); match(26);
			setState(205); match(38);
			setState(206); tableName();
			setState(223);
			switch (_input.LA(1)) {
			case 4:
			case 12:
			case 49:
				{
				{
				setState(208);
				_la = _input.LA(1);
				if (_la==4) {
					{
					setState(207); match(4);
					}
				}

				setState(210); fullSelect();
				}
				}
				break;
			case 17:
				{
				{
				setState(211); match(17);
				setState(212); match(12);
				setState(213); expression(0);
				setState(218);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==13) {
					{
					{
					setState(214); match(13);
					setState(215); expression(0);
					}
					}
					setState(220);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(221); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitUpdate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UpdateContext update() throws RecognitionException {
		UpdateContext _localctx = new UpdateContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_update);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(225); match(19);
			setState(226); tableName();
			setState(227); match(66);
			setState(230);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(228); columnName();
				}
				break;
			case 12:
				{
				setState(229); colList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(232); match(EQUALS);
			setState(233); expression(0);
			setState(235);
			_la = _input.LA(1);
			if (_la==37) {
				{
				setState(234); whereClause();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDelete(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeleteContext delete() throws RecognitionException {
		DeleteContext _localctx = new DeleteContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_delete);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(237); match(20);
			setState(238); match(4);
			setState(239); tableName();
			setState(241);
			_la = _input.LA(1);
			if (_la==37) {
				{
				setState(240); whereClause();
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
		public OrganizationContext organization() {
			return getRuleContext(OrganizationContext.class,0);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
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
			setState(243); match(29);
			setState(245);
			_la = _input.LA(1);
			if (_la==COLUMN) {
				{
				setState(244); match(COLUMN);
				}
			}

			setState(247); match(33);
			setState(248); tableName();
			setState(249); match(12);
			setState(250); colDef();
			setState(255);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(251); match(13);
					setState(252); colDef();
					}
					} 
				}
				setState(257);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			}
			setState(260);
			_la = _input.LA(1);
			if (_la==13) {
				{
				setState(258); match(13);
				setState(259); primaryKey();
				}
			}

			setState(262); match(65);
			setState(264);
			_la = _input.LA(1);
			if (_la==COLORDER) {
				{
				setState(263); colOrder();
				}
			}

			setState(267);
			_la = _input.LA(1);
			if (_la==ORGANIZATION) {
				{
				setState(266); organization();
				}
			}

			setState(270);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
			case 1:
				{
				setState(269); groupExp();
				}
				break;
			}
			setState(272); nodeExp();
			setState(273); deviceExp();
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

	public static class OrganizationContext extends ParserRuleContext {
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode ORGANIZATION() { return getToken(SelectParser.ORGANIZATION, 0); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public OrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_organization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrganizationContext organization() throws RecognitionException {
		OrganizationContext _localctx = new OrganizationContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_organization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(275); match(ORGANIZATION);
			setState(276); match(12);
			setState(277); match(INTEGER);
			setState(282);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(278); match(13);
				setState(279); match(INTEGER);
				}
				}
				setState(284);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(285); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateExternalTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateExternalTableContext createExternalTable() throws RecognitionException {
		CreateExternalTableContext _localctx = new CreateExternalTableContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_createExternalTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287); match(29);
			setState(288); match(61);
			setState(289); match(33);
			setState(290); tableName();
			setState(291); match(12);
			setState(292); colDef();
			setState(297);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(293); match(13);
				setState(294); colDef();
				}
				}
				setState(299);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(300); match(65);
			setState(303);
			switch (_input.LA(1)) {
			case 3:
				{
				setState(301); generalExtTableSpec();
				}
				break;
			case 5:
				{
				setState(302); javaClassExtTableSpec();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGeneralExtTableSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GeneralExtTableSpecContext generalExtTableSpec() throws RecognitionException {
		GeneralExtTableSpecContext _localctx = new GeneralExtTableSpecContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_generalExtTableSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(305); match(3);
			setState(306); match(4);
			setState(307); sourceList();
			setState(308); match(27);
			setState(309); match(63);
			setState(310); match(35);
			setState(311); anything();
			setState(312); match(60);
			setState(313); match(63);
			setState(314); match(35);
			setState(315); anything();
			setState(316); match(9);
			setState(317); match(25);
			setState(318); match(FILEPATHIDENTIFIER);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitJavaClassExtTableSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JavaClassExtTableSpecContext javaClassExtTableSpec() throws RecognitionException {
		JavaClassExtTableSpecContext _localctx = new JavaClassExtTableSpecContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_javaClassExtTableSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(320); match(5);
			setState(321); javaClassName();
			setState(322); match(50);
			setState(323); match(8);
			setState(324); match(12);
			setState(325); keyValueList();
			setState(326); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitJavaClassName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JavaClassNameContext javaClassName() throws RecognitionException {
		JavaClassNameContext _localctx = new JavaClassNameContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_javaClassName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(328); match(JAVACLASSNAMEIDENTIFIER);
			setState(333);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==52) {
				{
				{
				setState(329); match(52);
				setState(330); match(JAVACLASSNAMEIDENTIFIER);
				}
				}
				setState(335);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(336); match(45);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitKeyValueList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeyValueListContext keyValueList() throws RecognitionException {
		KeyValueListContext _localctx = new KeyValueListContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_keyValueList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(338); anything();
			setState(339); match(56);
			setState(340); anything();
			setState(348);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(341); match(13);
				setState(342); anything();
				setState(343); match(56);
				setState(344); anything();
				}
				}
				setState(350);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitAnything(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnythingContext anything() throws RecognitionException {
		AnythingContext _localctx = new AnythingContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_anything);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(351);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSourceList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SourceListContext sourceList() throws RecognitionException {
		SourceListContext _localctx = new SourceListContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_sourceList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(353);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 30) | (1L << 46) | (1L << 58))) != 0)) ) {
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColOrder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColOrderContext colOrder() throws RecognitionException {
		ColOrderContext _localctx = new ColOrderContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_colOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(355); match(COLORDER);
			setState(356); match(12);
			setState(357); match(INTEGER);
			setState(362);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(358); match(13);
				setState(359); match(INTEGER);
				}
				}
				setState(364);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(365); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGroupExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupExpContext groupExp() throws RecognitionException {
		GroupExpContext _localctx = new GroupExpContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_groupExp);
		try {
			setState(369);
			switch (_input.LA(1)) {
			case NONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(367); match(NONE);
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 2);
				{
				setState(368); realGroupExp();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRealGroupExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RealGroupExpContext realGroupExp() throws RecognitionException {
		RealGroupExpContext _localctx = new RealGroupExpContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_realGroupExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(371); match(7);
			setState(372); groupDef();
			setState(377);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==59) {
				{
				{
				setState(373); match(59);
				setState(374); groupDef();
				}
				}
				setState(379);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(380); match(42);
			setState(386);
			_la = _input.LA(1);
			if (_la==13) {
				{
				setState(381); match(13);
				setState(384);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(382); hashExp();
					}
					break;
				case RANGE:
					{
					setState(383); rangeType();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGroupDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupDefContext groupDef() throws RecognitionException {
		GroupDefContext _localctx = new GroupDefContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_groupDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(388); match(7);
			setState(389); match(INTEGER);
			setState(394);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==59) {
				{
				{
				setState(390); match(59);
				setState(391); match(INTEGER);
				}
				}
				setState(396);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(397); match(42);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRangeExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeExpContext rangeExp() throws RecognitionException {
		RangeExpContext _localctx = new RangeExpContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_rangeExp);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(402);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,26,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(399);
					matchWildcard();
					}
					} 
				}
				setState(404);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,26,_ctx);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNodeExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeExpContext nodeExp() throws RecognitionException {
		NodeExpContext _localctx = new NodeExpContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_nodeExp);
		try {
			setState(407);
			switch (_input.LA(1)) {
			case ANYTEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(405); match(ANYTEXT);
				}
				break;
			case 7:
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(406); realNodeExp();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRealNodeExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RealNodeExpContext realNodeExp() throws RecognitionException {
		RealNodeExpContext _localctx = new RealNodeExpContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_realNodeExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(411);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(409); match(ALL);
				}
				break;
			case 7:
				{
				setState(410); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(418);
			_la = _input.LA(1);
			if (_la==13) {
				{
				setState(413); match(13);
				setState(416);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(414); hashExp();
					}
					break;
				case RANGE:
					{
					setState(415); rangeType();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIntegerSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntegerSetContext integerSet() throws RecognitionException {
		IntegerSetContext _localctx = new IntegerSetContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_integerSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(420); match(7);
			setState(421); match(INTEGER);
			setState(426);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==59) {
				{
				{
				setState(422); match(59);
				setState(423); match(INTEGER);
				}
				}
				setState(428);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(429); match(42);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitHashExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HashExpContext hashExp() throws RecognitionException {
		HashExpContext _localctx = new HashExpContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_hashExp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(431); match(HASH);
			setState(432); match(13);
			setState(433); columnSet();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColumnSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnSetContext columnSet() throws RecognitionException {
		ColumnSetContext _localctx = new ColumnSetContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_columnSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(435); match(7);
			setState(436); columnName();
			setState(441);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==59) {
				{
				{
				setState(437); match(59);
				setState(438); columnName();
				}
				}
				setState(443);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(444); match(42);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRangeType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeTypeContext rangeType() throws RecognitionException {
		RangeTypeContext _localctx = new RangeTypeContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_rangeType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(446); match(RANGE);
			setState(447); match(13);
			setState(448); columnName();
			setState(449); match(13);
			setState(450); rangeSet();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRangeSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeSetContext rangeSet() throws RecognitionException {
		RangeSetContext _localctx = new RangeSetContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_rangeSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(452); match(7);
			setState(453); rangeExp();
			setState(458);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==59) {
				{
				{
				setState(454); match(59);
				setState(455); rangeExp();
				}
				}
				setState(460);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(461); match(42);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDeviceExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeviceExpContext deviceExp() throws RecognitionException {
		DeviceExpContext _localctx = new DeviceExpContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_deviceExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(465);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(463); match(ALL);
				}
				break;
			case 7:
				{
				setState(464); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(472);
			_la = _input.LA(1);
			if (_la==13) {
				{
				setState(467); match(13);
				setState(470);
				switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
				case 1:
					{
					setState(468); hashExp();
					}
					break;

				case 2:
					{
					setState(469); rangeExp();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropTableContext dropTable() throws RecognitionException {
		DropTableContext _localctx = new DropTableContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_dropTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(474); match(51);
			setState(475); match(33);
			setState(476); tableName();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateViewContext createView() throws RecognitionException {
		CreateViewContext _localctx = new CreateViewContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_createView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(478); match(29);
			setState(479); match(43);
			setState(480); tableName();
			setState(481); match(34);
			setState(482); fullSelect();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDropView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropViewContext dropView() throws RecognitionException {
		DropViewContext _localctx = new DropViewContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_dropView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(484); match(51);
			setState(485); match(43);
			setState(486); tableName();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateIndex(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateIndexContext createIndex() throws RecognitionException {
		CreateIndexContext _localctx = new CreateIndexContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_createIndex);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(488); match(29);
			setState(490);
			_la = _input.LA(1);
			if (_la==UNIQUE) {
				{
				setState(489); match(UNIQUE);
				}
			}

			setState(492); match(57);
			setState(493); tableName();
			setState(494); match(40);
			setState(495); tableName();
			setState(496); match(12);
			setState(497); indexDef();
			setState(502);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(498); match(13);
				setState(499); indexDef();
				}
				}
				setState(504);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(505); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDropIndex(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropIndexContext dropIndex() throws RecognitionException {
		DropIndexContext _localctx = new DropIndexContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_dropIndex);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(507); match(51);
			setState(508); match(57);
			setState(509); tableName();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitLoad(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LoadContext load() throws RecognitionException {
		LoadContext _localctx = new LoadContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_load);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(511); match(16);
			setState(512);
			_la = _input.LA(1);
			if ( !(_la==REPLACE || _la==RESUME) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(513); match(38);
			setState(514); tableName();
			setState(517);
			_la = _input.LA(1);
			if (_la==15) {
				{
				setState(515); match(15);
				setState(516); any();
				}
			}

			setState(519); match(4);
			setState(520); remainder();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitAny(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnyContext any() throws RecognitionException {
		AnyContext _localctx = new AnyContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_any);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(522);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRemainder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RemainderContext remainder() throws RecognitionException {
		RemainderContext _localctx = new RemainderContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_remainder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(527);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1) | (1L << 2) | (1L << 3) | (1L << 4) | (1L << 5) | (1L << 6) | (1L << 7) | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 11) | (1L << 12) | (1L << 13) | (1L << 14) | (1L << 15) | (1L << 16) | (1L << 17) | (1L << 18) | (1L << 19) | (1L << 20) | (1L << 21) | (1L << 22) | (1L << 23) | (1L << 24) | (1L << 25) | (1L << 26) | (1L << 27) | (1L << 28) | (1L << 29) | (1L << 30) | (1L << 31) | (1L << 32) | (1L << 33) | (1L << 34) | (1L << 35) | (1L << 36) | (1L << 37) | (1L << 38) | (1L << 39) | (1L << 40) | (1L << 41) | (1L << 42) | (1L << 43) | (1L << 44) | (1L << 45) | (1L << 46) | (1L << 47) | (1L << 48) | (1L << 49) | (1L << 50) | (1L << 51) | (1L << 52) | (1L << 53) | (1L << 54) | (1L << 55) | (1L << 56) | (1L << 57) | (1L << 58) | (1L << 59) | (1L << 60) | (1L << 61) | (1L << 62) | (1L << 63))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (64 - 64)) | (1L << (65 - 64)) | (1L << (66 - 64)) | (1L << (STRING - 64)) | (1L << (STAR - 64)) | (1L << (COUNT - 64)) | (1L << (CONCAT - 64)) | (1L << (NEGATIVE - 64)) | (1L << (EQUALS - 64)) | (1L << (OPERATOR - 64)) | (1L << (NULLOPERATOR - 64)) | (1L << (AND - 64)) | (1L << (OR - 64)) | (1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (COLUMN - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (COLORDER - 64)) | (1L << (ORGANIZATION - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (JAVACLASSNAMEIDENTIFIER - 64)) | (1L << (FILEPATHIDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0)) {
				{
				{
				setState(524);
				matchWildcard();
				}
				}
				setState(529);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(530); match(EOF);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIndexDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IndexDefContext indexDef() throws RecognitionException {
		IndexDefContext _localctx = new IndexDefContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_indexDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(532); columnName();
			setState(534);
			_la = _input.LA(1);
			if (_la==DIRECTION) {
				{
				setState(533); match(DIRECTION);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColDefContext colDef() throws RecognitionException {
		ColDefContext _localctx = new ColDefContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_colDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(536); columnName();
			setState(537); dataType();
			setState(539);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(538); notNull();
				}
			}

			setState(542);
			_la = _input.LA(1);
			if (_la==14) {
				{
				setState(541); primary();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitPrimaryKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryKeyContext primaryKey() throws RecognitionException {
		PrimaryKeyContext _localctx = new PrimaryKeyContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_primaryKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(544); match(14);
			setState(545); match(47);
			setState(546); match(12);
			setState(547); columnName();
			setState(552);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(548); match(13);
				setState(549); columnName();
				}
				}
				setState(554);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(555); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNotNull(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NotNullContext notNull() throws RecognitionException {
		NotNullContext _localctx = new NotNullContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_notNull);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(557); match(NOT);
			setState(558); match(NULL);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitPrimary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryContext primary() throws RecognitionException {
		PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_primary);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(560); match(14);
			setState(561); match(47);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_dataType);
		try {
			setState(568);
			switch (_input.LA(1)) {
			case 18:
			case 55:
				enterOuterAlt(_localctx, 1);
				{
				setState(563); char2();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(564); int2();
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 3);
				{
				setState(565); long2();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 4);
				{
				setState(566); date2();
				}
				break;
			case 1:
			case 64:
				enterOuterAlt(_localctx, 5);
				{
				setState(567); float2();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitChar2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Char2Context char2() throws RecognitionException {
		Char2Context _localctx = new Char2Context(_ctx, getState());
		enterRule(_localctx, 82, RULE_char2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(570);
			_la = _input.LA(1);
			if ( !(_la==18 || _la==55) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(571); match(12);
			setState(572); match(INTEGER);
			setState(573); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitInt2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Int2Context int2() throws RecognitionException {
		Int2Context _localctx = new Int2Context(_ctx, getState());
		enterRule(_localctx, 84, RULE_int2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(575); match(2);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitLong2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Long2Context long2() throws RecognitionException {
		Long2Context _localctx = new Long2Context(_ctx, getState());
		enterRule(_localctx, 86, RULE_long2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(577); match(21);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDate2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Date2Context date2() throws RecognitionException {
		Date2Context _localctx = new Date2Context(_ctx, getState());
		enterRule(_localctx, 88, RULE_date2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(579); match(DATE);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFloat2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Float2Context float2() throws RecognitionException {
		Float2Context _localctx = new Float2Context(_ctx, getState());
		enterRule(_localctx, 90, RULE_float2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(581);
			_la = _input.LA(1);
			if ( !(_la==1 || _la==64) ) {
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColListContext colList() throws RecognitionException {
		ColListContext _localctx = new ColListContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_colList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(583); match(12);
			setState(584); columnName();
			setState(589);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(585); match(13);
				setState(586); columnName();
				}
				}
				setState(591);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(592); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCommonTableExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommonTableExpressionContext commonTableExpression() throws RecognitionException {
		CommonTableExpressionContext _localctx = new CommonTableExpressionContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_commonTableExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(594); match(IDENTIFIER);
			setState(606);
			_la = _input.LA(1);
			if (_la==12) {
				{
				setState(595); match(12);
				setState(596); columnName();
				setState(601);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==13) {
					{
					{
					setState(597); match(13);
					setState(598); columnName();
					}
					}
					setState(603);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(604); match(65);
				}
			}

			setState(608); match(34);
			setState(609); match(12);
			setState(610); fullSelect();
			setState(611); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFullSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FullSelectContext fullSelect() throws RecognitionException {
		FullSelectContext _localctx = new FullSelectContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_fullSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(618);
			switch (_input.LA(1)) {
			case 49:
				{
				setState(613); subSelect();
				}
				break;
			case 12:
				{
				setState(614); match(12);
				setState(615); fullSelect();
				setState(616); match(65);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(623);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==TABLECOMBINATION) {
				{
				{
				setState(620); connectedSelect();
				}
				}
				setState(625);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(627);
			_la = _input.LA(1);
			if (_la==48) {
				{
				setState(626); orderBy();
				}
			}

			setState(630);
			_la = _input.LA(1);
			if (_la==23) {
				{
				setState(629); fetchFirst();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitConnectedSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConnectedSelectContext connectedSelect() throws RecognitionException {
		ConnectedSelectContext _localctx = new ConnectedSelectContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_connectedSelect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(632); match(TABLECOMBINATION);
			setState(638);
			switch (_input.LA(1)) {
			case 49:
				{
				setState(633); subSelect();
				}
				break;
			case 12:
				{
				setState(634); match(12);
				setState(635); fullSelect();
				setState(636); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSubSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubSelectContext subSelect() throws RecognitionException {
		SubSelectContext _localctx = new SubSelectContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_subSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(640); selectClause();
			setState(641); fromClause();
			setState(643);
			_la = _input.LA(1);
			if (_la==37) {
				{
				setState(642); whereClause();
				}
			}

			setState(646);
			_la = _input.LA(1);
			if (_la==10) {
				{
				setState(645); groupBy();
				}
			}

			setState(649);
			_la = _input.LA(1);
			if (_la==24) {
				{
				setState(648); havingClause();
				}
			}

			setState(652);
			switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
			case 1:
				{
				setState(651); orderBy();
				}
				break;
			}
			setState(655);
			switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
			case 1:
				{
				setState(654); fetchFirst();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(657); match(49);
			setState(659);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==ALL) {
				{
				setState(658); selecthow();
				}
			}

			setState(670);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(661); match(STAR);
				}
				break;
			case 11:
			case 12:
			case STRING:
			case COUNT:
			case NEGATIVE:
			case NULL:
			case INTEGER:
			case DATE:
			case IDENTIFIER:
				{
				{
				setState(662); selectListEntry();
				setState(667);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==13) {
					{
					{
					setState(663); match(13);
					setState(664); selectListEntry();
					}
					}
					setState(669);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelecthow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelecthowContext selecthow() throws RecognitionException {
		SelecthowContext _localctx = new SelecthowContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_selecthow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(672);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelectColumn(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelectExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectListEntryContext selectListEntry() throws RecognitionException {
		SelectListEntryContext _localctx = new SelectListEntryContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_selectListEntry);
		int _la;
		try {
			setState(688);
			switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
			case 1:
				_localctx = new SelectColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(674); columnName();
				setState(676);
				_la = _input.LA(1);
				if (_la==34) {
					{
					setState(675); match(34);
					}
				}

				setState(679);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(678); match(IDENTIFIER);
					}
				}

				}
				break;

			case 2:
				_localctx = new SelectExpressionContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(681); expression(0);
				setState(683);
				_la = _input.LA(1);
				if (_la==34) {
					{
					setState(682); match(34);
					}
				}

				setState(686);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(685); match(IDENTIFIER);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(690); match(4);
			setState(691); tableReference(0);
			setState(696);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(692); match(13);
				setState(693); tableReference(0);
				}
				}
				setState(698);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitJoinP(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNestedTable(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCrossJoinP(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIsSingleTable(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitJoin(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCrossJoin(this);
			else return visitor.visitChildren(this);
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
		int _startState = 110;
		enterRecursionRule(_localctx, 110, RULE_tableReference, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(728);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				{
				_localctx = new JoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(700); match(12);
				setState(701); tableReference(0);
				setState(703);
				_la = _input.LA(1);
				if (_la==JOINTYPE) {
					{
					setState(702); match(JOINTYPE);
					}
				}

				setState(705); match(41);
				setState(706); tableReference(0);
				setState(707); match(40);
				setState(708); searchCondition();
				setState(709); match(65);
				setState(711);
				switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
				case 1:
					{
					setState(710); correlationClause();
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
				setState(713); match(12);
				setState(714); tableReference(0);
				setState(715); match(CROSSJOIN);
				setState(716); tableReference(0);
				setState(717); match(65);
				setState(719);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(718); correlationClause();
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
				setState(721); match(12);
				setState(722); fullSelect();
				setState(723); match(65);
				setState(725);
				switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
				case 1:
					{
					setState(724); correlationClause();
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
				setState(727); singleTable();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(749);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,77,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(747);
					switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
					case 1:
						{
						_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(730);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(732);
						_la = _input.LA(1);
						if (_la==JOINTYPE) {
							{
							setState(731); match(JOINTYPE);
							}
						}

						setState(734); match(41);
						setState(735); tableReference(0);
						setState(736); match(40);
						setState(737); searchCondition();
						setState(739);
						switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
						case 1:
							{
							setState(738); correlationClause();
							}
							break;
						}
						}
						break;

					case 2:
						{
						_localctx = new CrossJoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(741);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(742); match(CROSSJOIN);
						setState(743); tableReference(0);
						setState(745);
						switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
						case 1:
							{
							setState(744); correlationClause();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(751);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,77,_ctx);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSingleTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableContext singleTable() throws RecognitionException {
		SingleTableContext _localctx = new SingleTableContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_singleTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(752); tableName();
			setState(754);
			switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
			case 1:
				{
				setState(753); correlationClause();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(756); match(37);
			setState(757); searchCondition();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGroupBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_groupBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(759); match(10);
			setState(760); match(35);
			setState(761); columnName();
			setState(766);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(762); match(13);
				setState(763); columnName();
				}
				}
				setState(768);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitHavingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(769); match(24);
			setState(770); searchCondition();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitOrderBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderByContext orderBy() throws RecognitionException {
		OrderByContext _localctx = new OrderByContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_orderBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(772); match(48);
			setState(773); match(35);
			setState(774); sortKey();
			setState(779);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==13) {
				{
				{
				setState(775); match(13);
				setState(776); sortKey();
				}
				}
				setState(781);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSortKeyCol(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSortKeyInt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortKeyContext sortKey() throws RecognitionException {
		SortKeyContext _localctx = new SortKeyContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_sortKey);
		int _la;
		try {
			setState(790);
			switch (_input.LA(1)) {
			case INTEGER:
				_localctx = new SortKeyIntContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(782); match(INTEGER);
				setState(784);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(783); match(DIRECTION);
					}
				}

				}
				break;
			case IDENTIFIER:
				_localctx = new SortKeyColContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(786); columnName();
				setState(788);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(787); match(DIRECTION);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCorrelationClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CorrelationClauseContext correlationClause() throws RecognitionException {
		CorrelationClauseContext _localctx = new CorrelationClauseContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_correlationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(793);
			_la = _input.LA(1);
			if (_la==34) {
				{
				setState(792); match(34);
				}
			}

			setState(795); match(IDENTIFIER);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFetchFirst(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FetchFirstContext fetchFirst() throws RecognitionException {
		FetchFirstContext _localctx = new FetchFirstContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_fetchFirst);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(797); match(23);
			setState(798); match(22);
			setState(800);
			_la = _input.LA(1);
			if (_la==INTEGER) {
				{
				setState(799); match(INTEGER);
				}
			}

			setState(802);
			_la = _input.LA(1);
			if ( !(_la==54 || _la==60) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(803); match(32);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitTable1Part(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitTable2Part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableNameContext tableName() throws RecognitionException {
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_tableName);
		try {
			setState(809);
			switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
			case 1:
				_localctx = new Table1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(805); match(IDENTIFIER);
				}
				break;

			case 2:
				_localctx = new Table2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(806); match(IDENTIFIER);
				setState(807); match(52);
				setState(808); match(IDENTIFIER);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCol2Part(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCol1Part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameContext columnName() throws RecognitionException {
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_columnName);
		try {
			setState(815);
			switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
			case 1:
				_localctx = new Col1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(811); match(IDENTIFIER);
				}
				break;

			case 2:
				_localctx = new Col2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(812); match(IDENTIFIER);
				setState(813); match(52);
				setState(814); match(IDENTIFIER);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSearchCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SearchConditionContext searchCondition() throws RecognitionException {
		SearchConditionContext _localctx = new SearchConditionContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_searchCondition);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(817); searchClause();
			setState(821);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(818); connectedSearchClause();
					}
					} 
				}
				setState(823);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitConnectedSearchClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConnectedSearchClauseContext connectedSearchClause() throws RecognitionException {
		ConnectedSearchClauseContext _localctx = new ConnectedSearchClauseContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_connectedSearchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(824);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OR) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(825); searchClause();
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSearchClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SearchClauseContext searchClause() throws RecognitionException {
		SearchClauseContext _localctx = new SearchClauseContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_searchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(828);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(827); match(NOT);
				}
			}

			setState(835);
			switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
			case 1:
				{
				setState(830); predicate();
				}
				break;

			case 2:
				{
				{
				setState(831); match(12);
				setState(832); searchCondition();
				setState(833); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitExistsPredicate(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNormalPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_predicate);
		try {
			setState(849);
			switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
			case 1:
				_localctx = new NormalPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(837); expression(0);
				setState(838); operator();
				setState(839); expression(0);
				}
				}
				break;

			case 2:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(841); expression(0);
				setState(842); match(NULLOPERATOR);
				}
				}
				break;

			case 3:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(844); match(6);
				setState(845); match(12);
				setState(846); subSelect();
				setState(847); match(65);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OperatorContext operator() throws RecognitionException {
		OperatorContext _localctx = new OperatorContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(851);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCountStar(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitExpSelect(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitMulDiv(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitAddSub(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIsLiteral(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCaseExp(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNullExp(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitPExpression(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitConcat(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFunction(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColLiteral(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitList(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCountDistinct(this);
			else return visitor.visitChildren(this);
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
		int _startState = 142;
		enterRecursionRule(_localctx, 142, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(911);
			switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
			case 1:
				{
				_localctx = new FunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(854); identifier();
				setState(855); match(12);
				setState(856); expression(0);
				setState(861);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==13) {
					{
					{
					setState(857); match(13);
					setState(858); expression(0);
					}
					}
					setState(863);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(864); match(65);
				}
				break;

			case 2:
				{
				_localctx = new CountDistinctContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(866); match(COUNT);
				setState(867); match(12);
				setState(868); match(DISTINCT);
				setState(869); expression(0);
				setState(870); match(65);
				}
				break;

			case 3:
				{
				_localctx = new ListContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(872); match(12);
				setState(873); expression(0);
				setState(876); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(874); match(13);
					setState(875); expression(0);
					}
					}
					setState(878); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==13 );
				setState(880); match(65);
				}
				break;

			case 4:
				{
				_localctx = new CountStarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(882); match(COUNT);
				setState(883); match(12);
				setState(884); match(STAR);
				setState(885); match(65);
				}
				break;

			case 5:
				{
				_localctx = new IsLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(886); literal();
				}
				break;

			case 6:
				{
				_localctx = new ColLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(887); columnName();
				}
				break;

			case 7:
				{
				_localctx = new ExpSelectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(888); match(12);
				setState(889); subSelect();
				setState(890); match(65);
				}
				break;

			case 8:
				{
				_localctx = new PExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(892); match(12);
				setState(893); expression(0);
				setState(894); match(65);
				}
				break;

			case 9:
				{
				_localctx = new NullExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(896); match(NULL);
				}
				break;

			case 10:
				{
				_localctx = new CaseExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(897); match(11);
				setState(898); caseCase();
				setState(902);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==53) {
					{
					{
					setState(899); caseCase();
					}
					}
					setState(904);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(905); match(36);
				setState(906); expression(0);
				setState(907); match(39);
				setState(909);
				switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
				case 1:
					{
					setState(908); match(11);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(924);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(922);
					switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
					case 1:
						{
						_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(913);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(914);
						((MulDivContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==31 || _la==STAR) ) {
							((MulDivContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(915); expression(14);
						}
						break;

					case 2:
						{
						_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(916);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(917);
						((AddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==28 || _la==NEGATIVE) ) {
							((AddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(918); expression(13);
						}
						break;

					case 3:
						{
						_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(919);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(920); match(CONCAT);
						setState(921); expression(12);
						}
						break;
					}
					} 
				}
				setState(926);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCaseCase(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseCaseContext caseCase() throws RecognitionException {
		CaseCaseContext _localctx = new CaseCaseContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_caseCase);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(927); match(53);
			setState(928); searchCondition();
			setState(929); match(44);
			setState(930); expression(0);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(932);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
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
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_literal);
		int _la;
		try {
			setState(943);
			switch (_input.LA(1)) {
			case NEGATIVE:
			case INTEGER:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(935);
				_la = _input.LA(1);
				if (_la==NEGATIVE) {
					{
					setState(934); match(NEGATIVE);
					}
				}

				setState(937); match(INTEGER);
				setState(940);
				switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
				case 1:
					{
					setState(938); match(52);
					setState(939); match(INTEGER);
					}
					break;
				}
				}
				break;
			case STRING:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(942); match(STRING);
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
		case 55: return tableReference_sempred((TableReferenceContext)_localctx, predIndex);

		case 71: return expression_sempred((ExpressionContext)_localctx, predIndex);
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3g\u03b4\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3"+
		"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\7\2\u00be\n\2\f\2\16\2\u00c1\13\2\5\2\u00c3"+
		"\n\2\3\2\3\2\3\2\3\2\5\2\u00c9\n\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4"+
		"\u00d3\n\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4\u00db\n\4\f\4\16\4\u00de\13\4\3"+
		"\4\3\4\5\4\u00e2\n\4\3\5\3\5\3\5\3\5\3\5\5\5\u00e9\n\5\3\5\3\5\3\5\5\5"+
		"\u00ee\n\5\3\6\3\6\3\6\3\6\5\6\u00f4\n\6\3\7\3\7\5\7\u00f8\n\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\7\7\u0100\n\7\f\7\16\7\u0103\13\7\3\7\3\7\5\7\u0107\n"+
		"\7\3\7\3\7\5\7\u010b\n\7\3\7\5\7\u010e\n\7\3\7\5\7\u0111\n\7\3\7\3\7\3"+
		"\7\3\b\3\b\3\b\3\b\3\b\7\b\u011b\n\b\f\b\16\b\u011e\13\b\3\b\3\b\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u012a\n\t\f\t\16\t\u012d\13\t\3\t\3\t\3"+
		"\t\5\t\u0132\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\7\f\u014e\n"+
		"\f\f\f\16\f\u0151\13\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\7\r\u015d"+
		"\n\r\f\r\16\r\u0160\13\r\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\20\3\20"+
		"\7\20\u016b\n\20\f\20\16\20\u016e\13\20\3\20\3\20\3\21\3\21\5\21\u0174"+
		"\n\21\3\22\3\22\3\22\3\22\7\22\u017a\n\22\f\22\16\22\u017d\13\22\3\22"+
		"\3\22\3\22\3\22\5\22\u0183\n\22\5\22\u0185\n\22\3\23\3\23\3\23\3\23\7"+
		"\23\u018b\n\23\f\23\16\23\u018e\13\23\3\23\3\23\3\24\7\24\u0193\n\24\f"+
		"\24\16\24\u0196\13\24\3\25\3\25\5\25\u019a\n\25\3\26\3\26\5\26\u019e\n"+
		"\26\3\26\3\26\3\26\5\26\u01a3\n\26\5\26\u01a5\n\26\3\27\3\27\3\27\3\27"+
		"\7\27\u01ab\n\27\f\27\16\27\u01ae\13\27\3\27\3\27\3\30\3\30\3\30\3\30"+
		"\3\31\3\31\3\31\3\31\7\31\u01ba\n\31\f\31\16\31\u01bd\13\31\3\31\3\31"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\7\33\u01cb\n\33\f\33"+
		"\16\33\u01ce\13\33\3\33\3\33\3\34\3\34\5\34\u01d4\n\34\3\34\3\34\3\34"+
		"\5\34\u01d9\n\34\5\34\u01db\n\34\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3"+
		"\36\3\36\3\36\3\37\3\37\3\37\3\37\3 \3 \5 \u01ed\n \3 \3 \3 \3 \3 \3 "+
		"\3 \3 \7 \u01f7\n \f \16 \u01fa\13 \3 \3 \3!\3!\3!\3!\3\"\3\"\3\"\3\""+
		"\3\"\3\"\5\"\u0208\n\"\3\"\3\"\3\"\3#\3#\3$\7$\u0210\n$\f$\16$\u0213\13"+
		"$\3$\3$\3%\3%\5%\u0219\n%\3&\3&\3&\5&\u021e\n&\3&\5&\u0221\n&\3\'\3\'"+
		"\3\'\3\'\3\'\3\'\7\'\u0229\n\'\f\'\16\'\u022c\13\'\3\'\3\'\3(\3(\3(\3"+
		")\3)\3)\3*\3*\3*\3*\3*\5*\u023b\n*\3+\3+\3+\3+\3+\3,\3,\3-\3-\3.\3.\3"+
		"/\3/\3\60\3\60\3\60\3\60\7\60\u024e\n\60\f\60\16\60\u0251\13\60\3\60\3"+
		"\60\3\61\3\61\3\61\3\61\3\61\7\61\u025a\n\61\f\61\16\61\u025d\13\61\3"+
		"\61\3\61\5\61\u0261\n\61\3\61\3\61\3\61\3\61\3\61\3\62\3\62\3\62\3\62"+
		"\3\62\5\62\u026d\n\62\3\62\7\62\u0270\n\62\f\62\16\62\u0273\13\62\3\62"+
		"\5\62\u0276\n\62\3\62\5\62\u0279\n\62\3\63\3\63\3\63\3\63\3\63\3\63\5"+
		"\63\u0281\n\63\3\64\3\64\3\64\5\64\u0286\n\64\3\64\5\64\u0289\n\64\3\64"+
		"\5\64\u028c\n\64\3\64\5\64\u028f\n\64\3\64\5\64\u0292\n\64\3\65\3\65\5"+
		"\65\u0296\n\65\3\65\3\65\3\65\3\65\7\65\u029c\n\65\f\65\16\65\u029f\13"+
		"\65\5\65\u02a1\n\65\3\66\3\66\3\67\3\67\5\67\u02a7\n\67\3\67\5\67\u02aa"+
		"\n\67\3\67\3\67\5\67\u02ae\n\67\3\67\5\67\u02b1\n\67\5\67\u02b3\n\67\3"+
		"8\38\38\38\78\u02b9\n8\f8\168\u02bc\138\39\39\39\39\59\u02c2\n9\39\39"+
		"\39\39\39\39\59\u02ca\n9\39\39\39\39\39\39\59\u02d2\n9\39\39\39\39\59"+
		"\u02d8\n9\39\59\u02db\n9\39\39\59\u02df\n9\39\39\39\39\39\59\u02e6\n9"+
		"\39\39\39\39\59\u02ec\n9\79\u02ee\n9\f9\169\u02f1\139\3:\3:\5:\u02f5\n"+
		":\3;\3;\3;\3<\3<\3<\3<\3<\7<\u02ff\n<\f<\16<\u0302\13<\3=\3=\3=\3>\3>"+
		"\3>\3>\3>\7>\u030c\n>\f>\16>\u030f\13>\3?\3?\5?\u0313\n?\3?\3?\5?\u0317"+
		"\n?\5?\u0319\n?\3@\5@\u031c\n@\3@\3@\3A\3A\3A\5A\u0323\nA\3A\3A\3A\3B"+
		"\3B\3B\3B\5B\u032c\nB\3C\3C\3C\3C\5C\u0332\nC\3D\3D\7D\u0336\nD\fD\16"+
		"D\u0339\13D\3E\3E\3E\3F\5F\u033f\nF\3F\3F\3F\3F\3F\5F\u0346\nF\3G\3G\3"+
		"G\3G\3G\3G\3G\3G\3G\3G\3G\3G\5G\u0354\nG\3H\3H\3I\3I\3I\3I\3I\3I\7I\u035e"+
		"\nI\fI\16I\u0361\13I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\6I\u036f\nI\r"+
		"I\16I\u0370\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3"+
		"I\7I\u0387\nI\fI\16I\u038a\13I\3I\3I\3I\3I\5I\u0390\nI\5I\u0392\nI\3I"+
		"\3I\3I\3I\3I\3I\3I\3I\3I\7I\u039d\nI\fI\16I\u03a0\13I\3J\3J\3J\3J\3J\3"+
		"K\3K\3L\5L\u03aa\nL\3L\3L\3L\5L\u03af\nL\3L\5L\u03b2\nL\3L\3\u0194\4p"+
		"\u0090M\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:"+
		"<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a"+
		"\u008c\u008e\u0090\u0092\u0094\u0096\2\r\5\2  \60\60<<\3\2Z[\4\2\24\24"+
		"99\4\2\3\3BB\4\2VV]]\4\288>>\3\2MN\3\2JK\4\2!!FF\4\2\36\36II\5\2GGaad"+
		"d\u03e7\2\u00c8\3\2\2\2\4\u00ca\3\2\2\2\6\u00ce\3\2\2\2\b\u00e3\3\2\2"+
		"\2\n\u00ef\3\2\2\2\f\u00f5\3\2\2\2\16\u0115\3\2\2\2\20\u0121\3\2\2\2\22"+
		"\u0133\3\2\2\2\24\u0142\3\2\2\2\26\u014a\3\2\2\2\30\u0154\3\2\2\2\32\u0161"+
		"\3\2\2\2\34\u0163\3\2\2\2\36\u0165\3\2\2\2 \u0173\3\2\2\2\"\u0175\3\2"+
		"\2\2$\u0186\3\2\2\2&\u0194\3\2\2\2(\u0199\3\2\2\2*\u019d\3\2\2\2,\u01a6"+
		"\3\2\2\2.\u01b1\3\2\2\2\60\u01b5\3\2\2\2\62\u01c0\3\2\2\2\64\u01c6\3\2"+
		"\2\2\66\u01d3\3\2\2\28\u01dc\3\2\2\2:\u01e0\3\2\2\2<\u01e6\3\2\2\2>\u01ea"+
		"\3\2\2\2@\u01fd\3\2\2\2B\u0201\3\2\2\2D\u020c\3\2\2\2F\u0211\3\2\2\2H"+
		"\u0216\3\2\2\2J\u021a\3\2\2\2L\u0222\3\2\2\2N\u022f\3\2\2\2P\u0232\3\2"+
		"\2\2R\u023a\3\2\2\2T\u023c\3\2\2\2V\u0241\3\2\2\2X\u0243\3\2\2\2Z\u0245"+
		"\3\2\2\2\\\u0247\3\2\2\2^\u0249\3\2\2\2`\u0254\3\2\2\2b\u026c\3\2\2\2"+
		"d\u027a\3\2\2\2f\u0282\3\2\2\2h\u0293\3\2\2\2j\u02a2\3\2\2\2l\u02b2\3"+
		"\2\2\2n\u02b4\3\2\2\2p\u02da\3\2\2\2r\u02f2\3\2\2\2t\u02f6\3\2\2\2v\u02f9"+
		"\3\2\2\2x\u0303\3\2\2\2z\u0306\3\2\2\2|\u0318\3\2\2\2~\u031b\3\2\2\2\u0080"+
		"\u031f\3\2\2\2\u0082\u032b\3\2\2\2\u0084\u0331\3\2\2\2\u0086\u0333\3\2"+
		"\2\2\u0088\u033a\3\2\2\2\u008a\u033e\3\2\2\2\u008c\u0353\3\2\2\2\u008e"+
		"\u0355\3\2\2\2\u0090\u0391\3\2\2\2\u0092\u03a1\3\2\2\2\u0094\u03a6\3\2"+
		"\2\2\u0096\u03b1\3\2\2\2\u0098\u0099\5\6\4\2\u0099\u009a\7\2\2\3\u009a"+
		"\u00c9\3\2\2\2\u009b\u009c\5\b\5\2\u009c\u009d\7\2\2\3\u009d\u00c9\3\2"+
		"\2\2\u009e\u009f\5\n\6\2\u009f\u00a0\7\2\2\3\u00a0\u00c9\3\2\2\2\u00a1"+
		"\u00a2\5\f\7\2\u00a2\u00a3\7\2\2\3\u00a3\u00c9\3\2\2\2\u00a4\u00a5\5>"+
		" \2\u00a5\u00a6\7\2\2\3\u00a6\u00c9\3\2\2\2\u00a7\u00a8\5:\36\2\u00a8"+
		"\u00a9\7\2\2\3\u00a9\u00c9\3\2\2\2\u00aa\u00ab\58\35\2\u00ab\u00ac\7\2"+
		"\2\3\u00ac\u00c9\3\2\2\2\u00ad\u00ae\5@!\2\u00ae\u00af\7\2\2\3\u00af\u00c9"+
		"\3\2\2\2\u00b0\u00b1\5<\37\2\u00b1\u00b2\7\2\2\3\u00b2\u00c9\3\2\2\2\u00b3"+
		"\u00b4\5B\"\2\u00b4\u00b5\7\2\2\3\u00b5\u00c9\3\2\2\2\u00b6\u00b7\5\4"+
		"\3\2\u00b7\u00b8\7\2\2\3\u00b8\u00c9\3\2\2\2\u00b9\u00ba\7\64\2\2\u00ba"+
		"\u00bf\5`\61\2\u00bb\u00bc\7\17\2\2\u00bc\u00be\5`\61\2\u00bd\u00bb\3"+
		"\2\2\2\u00be\u00c1\3\2\2\2\u00bf\u00bd\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0"+
		"\u00c3\3\2\2\2\u00c1\u00bf\3\2\2\2\u00c2\u00b9\3\2\2\2\u00c2\u00c3\3\2"+
		"\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c5\5b\62\2\u00c5\u00c6\3\2\2\2\u00c6"+
		"\u00c7\7\2\2\3\u00c7\u00c9\3\2\2\2\u00c8\u0098\3\2\2\2\u00c8\u009b\3\2"+
		"\2\2\u00c8\u009e\3\2\2\2\u00c8\u00a1\3\2\2\2\u00c8\u00a4\3\2\2\2\u00c8"+
		"\u00a7\3\2\2\2\u00c8\u00aa\3\2\2\2\u00c8\u00ad\3\2\2\2\u00c8\u00b0\3\2"+
		"\2\2\u00c8\u00b3\3\2\2\2\u00c8\u00b6\3\2\2\2\u00c8\u00c2\3\2\2\2\u00c9"+
		"\3\3\2\2\2\u00ca\u00cb\7@\2\2\u00cb\u00cc\7*\2\2\u00cc\u00cd\5\u0082B"+
		"\2\u00cd\5\3\2\2\2\u00ce\u00cf\7\34\2\2\u00cf\u00d0\7(\2\2\u00d0\u00e1"+
		"\5\u0082B\2\u00d1\u00d3\7\6\2\2\u00d2\u00d1\3\2\2\2\u00d2\u00d3\3\2\2"+
		"\2\u00d3\u00d4\3\2\2\2\u00d4\u00e2\5b\62\2\u00d5\u00d6\7\23\2\2\u00d6"+
		"\u00d7\7\16\2\2\u00d7\u00dc\5\u0090I\2\u00d8\u00d9\7\17\2\2\u00d9\u00db"+
		"\5\u0090I\2\u00da\u00d8\3\2\2\2\u00db\u00de\3\2\2\2\u00dc\u00da\3\2\2"+
		"\2\u00dc\u00dd\3\2\2\2\u00dd\u00df\3\2\2\2\u00de\u00dc\3\2\2\2\u00df\u00e0"+
		"\7C\2\2\u00e0\u00e2\3\2\2\2\u00e1\u00d2\3\2\2\2\u00e1\u00d5\3\2\2\2\u00e2"+
		"\7\3\2\2\2\u00e3\u00e4\7\25\2\2\u00e4\u00e5\5\u0082B\2\u00e5\u00e8\7D"+
		"\2\2\u00e6\u00e9\5\u0084C\2\u00e7\u00e9\5^\60\2\u00e8\u00e6\3\2\2\2\u00e8"+
		"\u00e7\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00eb\7J\2\2\u00eb\u00ed\5\u0090"+
		"I\2\u00ec\u00ee\5t;\2\u00ed\u00ec\3\2\2\2\u00ed\u00ee\3\2\2\2\u00ee\t"+
		"\3\2\2\2\u00ef\u00f0\7\26\2\2\u00f0\u00f1\7\6\2\2\u00f1\u00f3\5\u0082"+
		"B\2\u00f2\u00f4\5t;\2\u00f3\u00f2\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\13"+
		"\3\2\2\2\u00f5\u00f7\7\37\2\2\u00f6\u00f8\7U\2\2\u00f7\u00f6\3\2\2\2\u00f7"+
		"\u00f8\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00fa\7#\2\2\u00fa\u00fb\5\u0082"+
		"B\2\u00fb\u00fc\7\16\2\2\u00fc\u0101\5J&\2\u00fd\u00fe\7\17\2\2\u00fe"+
		"\u0100\5J&\2\u00ff\u00fd\3\2\2\2\u0100\u0103\3\2\2\2\u0101\u00ff\3\2\2"+
		"\2\u0101\u0102\3\2\2\2\u0102\u0106\3\2\2\2\u0103\u0101\3\2\2\2\u0104\u0105"+
		"\7\17\2\2\u0105\u0107\5L\'\2\u0106\u0104\3\2\2\2\u0106\u0107\3\2\2\2\u0107"+
		"\u0108\3\2\2\2\u0108\u010a\7C\2\2\u0109\u010b\5\36\20\2\u010a\u0109\3"+
		"\2\2\2\u010a\u010b\3\2\2\2\u010b\u010d\3\2\2\2\u010c\u010e\5\16\b\2\u010d"+
		"\u010c\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0110\3\2\2\2\u010f\u0111\5 "+
		"\21\2\u0110\u010f\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0112\3\2\2\2\u0112"+
		"\u0113\5(\25\2\u0113\u0114\5\66\34\2\u0114\r\3\2\2\2\u0115\u0116\7c\2"+
		"\2\u0116\u0117\7\16\2\2\u0117\u011c\7W\2\2\u0118\u0119\7\17\2\2\u0119"+
		"\u011b\7W\2\2\u011a\u0118\3\2\2\2\u011b\u011e\3\2\2\2\u011c\u011a\3\2"+
		"\2\2\u011c\u011d\3\2\2\2\u011d\u011f\3\2\2\2\u011e\u011c\3\2\2\2\u011f"+
		"\u0120\7C\2\2\u0120\17\3\2\2\2\u0121\u0122\7\37\2\2\u0122\u0123\7?\2\2"+
		"\u0123\u0124\7#\2\2\u0124\u0125\5\u0082B\2\u0125\u0126\7\16\2\2\u0126"+
		"\u012b\5J&\2\u0127\u0128\7\17\2\2\u0128\u012a\5J&\2\u0129\u0127\3\2\2"+
		"\2\u012a\u012d\3\2\2\2\u012b\u0129\3\2\2\2\u012b\u012c\3\2\2\2\u012c\u012e"+
		"\3\2\2\2\u012d\u012b\3\2\2\2\u012e\u0131\7C\2\2\u012f\u0132\5\22\n\2\u0130"+
		"\u0132\5\24\13\2\u0131\u012f\3\2\2\2\u0131\u0130\3\2\2\2\u0132\21\3\2"+
		"\2\2\u0133\u0134\7\5\2\2\u0134\u0135\7\6\2\2\u0135\u0136\5\34\17\2\u0136"+
		"\u0137\7\35\2\2\u0137\u0138\7A\2\2\u0138\u0139\7%\2\2\u0139\u013a\5\32"+
		"\16\2\u013a\u013b\7>\2\2\u013b\u013c\7A\2\2\u013c\u013d\7%\2\2\u013d\u013e"+
		"\5\32\16\2\u013e\u013f\7\13\2\2\u013f\u0140\7\33\2\2\u0140\u0141\7f\2"+
		"\2\u0141\23\3\2\2\2\u0142\u0143\7\7\2\2\u0143\u0144\5\26\f\2\u0144\u0145"+
		"\7\64\2\2\u0145\u0146\7\n\2\2\u0146\u0147\7\16\2\2\u0147\u0148\5\30\r"+
		"\2\u0148\u0149\7C\2\2\u0149\25\3\2\2\2\u014a\u014f\7e\2\2\u014b\u014c"+
		"\7\66\2\2\u014c\u014e\7e\2\2\u014d\u014b\3\2\2\2\u014e\u0151\3\2\2\2\u014f"+
		"\u014d\3\2\2\2\u014f\u0150\3\2\2\2\u0150\u0152\3\2\2\2\u0151\u014f\3\2"+
		"\2\2\u0152\u0153\7/\2\2\u0153\27\3\2\2\2\u0154\u0155\5\32\16\2\u0155\u0156"+
		"\7:\2\2\u0156\u015e\5\32\16\2\u0157\u0158\7\17\2\2\u0158\u0159\5\32\16"+
		"\2\u0159\u015a\7:\2\2\u015a\u015b\5\32\16\2\u015b\u015d\3\2\2\2\u015c"+
		"\u0157\3\2\2\2\u015d\u0160\3\2\2\2\u015e\u015c\3\2\2\2\u015e\u015f\3\2"+
		"\2\2\u015f\31\3\2\2\2\u0160\u015e\3\2\2\2\u0161\u0162\13\2\2\2\u0162\33"+
		"\3\2\2\2\u0163\u0164\t\2\2\2\u0164\35\3\2\2\2\u0165\u0166\7b\2\2\u0166"+
		"\u0167\7\16\2\2\u0167\u016c\7W\2\2\u0168\u0169\7\17\2\2\u0169\u016b\7"+
		"W\2\2\u016a\u0168\3\2\2\2\u016b\u016e\3\2\2\2\u016c\u016a\3\2\2\2\u016c"+
		"\u016d\3\2\2\2\u016d\u016f\3\2\2\2\u016e\u016c\3\2\2\2\u016f\u0170\7C"+
		"\2\2\u0170\37\3\2\2\2\u0171\u0174\7\\\2\2\u0172\u0174\5\"\22\2\u0173\u0171"+
		"\3\2\2\2\u0173\u0172\3\2\2\2\u0174!\3\2\2\2\u0175\u0176\7\t\2\2\u0176"+
		"\u017b\5$\23\2\u0177\u0178\7=\2\2\u0178\u017a\5$\23\2\u0179\u0177\3\2"+
		"\2\2\u017a\u017d\3\2\2\2\u017b\u0179\3\2\2\2\u017b\u017c\3\2\2\2\u017c"+
		"\u017e\3\2\2\2\u017d\u017b\3\2\2\2\u017e\u0184\7,\2\2\u017f\u0182\7\17"+
		"\2\2\u0180\u0183\5.\30\2\u0181\u0183\5\62\32\2\u0182\u0180\3\2\2\2\u0182"+
		"\u0181\3\2\2\2\u0183\u0185\3\2\2\2\u0184\u017f\3\2\2\2\u0184\u0185\3\2"+
		"\2\2\u0185#\3\2\2\2\u0186\u0187\7\t\2\2\u0187\u018c\7W\2\2\u0188\u0189"+
		"\7=\2\2\u0189\u018b\7W\2\2\u018a\u0188\3\2\2\2\u018b\u018e\3\2\2\2\u018c"+
		"\u018a\3\2\2\2\u018c\u018d\3\2\2\2\u018d\u018f\3\2\2\2\u018e\u018c\3\2"+
		"\2\2\u018f\u0190\7,\2\2\u0190%\3\2\2\2\u0191\u0193\13\2\2\2\u0192\u0191"+
		"\3\2\2\2\u0193\u0196\3\2\2\2\u0194\u0195\3\2\2\2\u0194\u0192\3\2\2\2\u0195"+
		"\'\3\2\2\2\u0196\u0194\3\2\2\2\u0197\u019a\7^\2\2\u0198\u019a\5*\26\2"+
		"\u0199\u0197\3\2\2\2\u0199\u0198\3\2\2\2\u019a)\3\2\2\2\u019b\u019e\7"+
		"]\2\2\u019c\u019e\5,\27\2\u019d\u019b\3\2\2\2\u019d\u019c\3\2\2\2\u019e"+
		"\u01a4\3\2\2\2\u019f\u01a2\7\17\2\2\u01a0\u01a3\5.\30\2\u01a1\u01a3\5"+
		"\62\32\2\u01a2\u01a0\3\2\2\2\u01a2\u01a1\3\2\2\2\u01a3\u01a5\3\2\2\2\u01a4"+
		"\u019f\3\2\2\2\u01a4\u01a5\3\2\2\2\u01a5+\3\2\2\2\u01a6\u01a7\7\t\2\2"+
		"\u01a7\u01ac\7W\2\2\u01a8\u01a9\7=\2\2\u01a9\u01ab\7W\2\2\u01aa\u01a8"+
		"\3\2\2\2\u01ab\u01ae\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad"+
		"\u01af\3\2\2\2\u01ae\u01ac\3\2\2\2\u01af\u01b0\7,\2\2\u01b0-\3\2\2\2\u01b1"+
		"\u01b2\7_\2\2\u01b2\u01b3\7\17\2\2\u01b3\u01b4\5\60\31\2\u01b4/\3\2\2"+
		"\2\u01b5\u01b6\7\t\2\2\u01b6\u01bb\5\u0084C\2\u01b7\u01b8\7=\2\2\u01b8"+
		"\u01ba\5\u0084C\2\u01b9\u01b7\3\2\2\2\u01ba\u01bd\3\2\2\2\u01bb\u01b9"+
		"\3\2\2\2\u01bb\u01bc\3\2\2\2\u01bc\u01be\3\2\2\2\u01bd\u01bb\3\2\2\2\u01be"+
		"\u01bf\7,\2\2\u01bf\61\3\2\2\2\u01c0\u01c1\7`\2\2\u01c1\u01c2\7\17\2\2"+
		"\u01c2\u01c3\5\u0084C\2\u01c3\u01c4\7\17\2\2\u01c4\u01c5\5\64\33\2\u01c5"+
		"\63\3\2\2\2\u01c6\u01c7\7\t\2\2\u01c7\u01cc\5&\24\2\u01c8\u01c9\7=\2\2"+
		"\u01c9\u01cb\5&\24\2\u01ca\u01c8\3\2\2\2\u01cb\u01ce\3\2\2\2\u01cc\u01ca"+
		"\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd\u01cf\3\2\2\2\u01ce\u01cc\3\2\2\2\u01cf"+
		"\u01d0\7,\2\2\u01d0\65\3\2\2\2\u01d1\u01d4\7]\2\2\u01d2\u01d4\5,\27\2"+
		"\u01d3\u01d1\3\2\2\2\u01d3\u01d2\3\2\2\2\u01d4\u01da\3\2\2\2\u01d5\u01d8"+
		"\7\17\2\2\u01d6\u01d9\5.\30\2\u01d7\u01d9\5&\24\2\u01d8\u01d6\3\2\2\2"+
		"\u01d8\u01d7\3\2\2\2\u01d9\u01db\3\2\2\2\u01da\u01d5\3\2\2\2\u01da\u01db"+
		"\3\2\2\2\u01db\67\3\2\2\2\u01dc\u01dd\7\65\2\2\u01dd\u01de\7#\2\2\u01de"+
		"\u01df\5\u0082B\2\u01df9\3\2\2\2\u01e0\u01e1\7\37\2\2\u01e1\u01e2\7-\2"+
		"\2\u01e2\u01e3\5\u0082B\2\u01e3\u01e4\7$\2\2\u01e4\u01e5\5b\62\2\u01e5"+
		";\3\2\2\2\u01e6\u01e7\7\65\2\2\u01e7\u01e8\7-\2\2\u01e8\u01e9\5\u0082"+
		"B\2\u01e9=\3\2\2\2\u01ea\u01ec\7\37\2\2\u01eb\u01ed\7Y\2\2\u01ec\u01eb"+
		"\3\2\2\2\u01ec\u01ed\3\2\2\2\u01ed\u01ee\3\2\2\2\u01ee\u01ef\7;\2\2\u01ef"+
		"\u01f0\5\u0082B\2\u01f0\u01f1\7*\2\2\u01f1\u01f2\5\u0082B\2\u01f2\u01f3"+
		"\7\16\2\2\u01f3\u01f8\5H%\2\u01f4\u01f5\7\17\2\2\u01f5\u01f7\5H%\2\u01f6"+
		"\u01f4\3\2\2\2\u01f7\u01fa\3\2\2\2\u01f8\u01f6\3\2\2\2\u01f8\u01f9\3\2"+
		"\2\2\u01f9\u01fb\3\2\2\2\u01fa\u01f8\3\2\2\2\u01fb\u01fc\7C\2\2\u01fc"+
		"?\3\2\2\2\u01fd\u01fe\7\65\2\2\u01fe\u01ff\7;\2\2\u01ff\u0200\5\u0082"+
		"B\2\u0200A\3\2\2\2\u0201\u0202\7\22\2\2\u0202\u0203\t\3\2\2\u0203\u0204"+
		"\7(\2\2\u0204\u0207\5\u0082B\2\u0205\u0206\7\21\2\2\u0206\u0208\5D#\2"+
		"\u0207\u0205\3\2\2\2\u0207\u0208\3\2\2\2\u0208\u0209\3\2\2\2\u0209\u020a"+
		"\7\6\2\2\u020a\u020b\5F$\2\u020bC\3\2\2\2\u020c\u020d\13\2\2\2\u020dE"+
		"\3\2\2\2\u020e\u0210\13\2\2\2\u020f\u020e\3\2\2\2\u0210\u0213\3\2\2\2"+
		"\u0211\u020f\3\2\2\2\u0211\u0212\3\2\2\2\u0212\u0214\3\2\2\2\u0213\u0211"+
		"\3\2\2\2\u0214\u0215\7\2\2\3\u0215G\3\2\2\2\u0216\u0218\5\u0084C\2\u0217"+
		"\u0219\7Q\2\2\u0218\u0217\3\2\2\2\u0218\u0219\3\2\2\2\u0219I\3\2\2\2\u021a"+
		"\u021b\5\u0084C\2\u021b\u021d\5R*\2\u021c\u021e\5N(\2\u021d\u021c\3\2"+
		"\2\2\u021d\u021e\3\2\2\2\u021e\u0220\3\2\2\2\u021f\u0221\5P)\2\u0220\u021f"+
		"\3\2\2\2\u0220\u0221\3\2\2\2\u0221K\3\2\2\2\u0222\u0223\7\20\2\2\u0223"+
		"\u0224\7\61\2\2\u0224\u0225\7\16\2\2\u0225\u022a\5\u0084C\2\u0226\u0227"+
		"\7\17\2\2\u0227\u0229\5\u0084C\2\u0228\u0226\3\2\2\2\u0229\u022c\3\2\2"+
		"\2\u022a\u0228\3\2\2\2\u022a\u022b\3\2\2\2\u022b\u022d\3\2\2\2\u022c\u022a"+
		"\3\2\2\2\u022d\u022e\7C\2\2\u022eM\3\2\2\2\u022f\u0230\7O\2\2\u0230\u0231"+
		"\7P\2\2\u0231O\3\2\2\2\u0232\u0233\7\20\2\2\u0233\u0234\7\61\2\2\u0234"+
		"Q\3\2\2\2\u0235\u023b\5T+\2\u0236\u023b\5V,\2\u0237\u023b\5X-\2\u0238"+
		"\u023b\5Z.\2\u0239\u023b\5\\/\2\u023a\u0235\3\2\2\2\u023a\u0236\3\2\2"+
		"\2\u023a\u0237\3\2\2\2\u023a\u0238\3\2\2\2\u023a\u0239\3\2\2\2\u023bS"+
		"\3\2\2\2\u023c\u023d\t\4\2\2\u023d\u023e\7\16\2\2\u023e\u023f\7W\2\2\u023f"+
		"\u0240\7C\2\2\u0240U\3\2\2\2\u0241\u0242\7\4\2\2\u0242W\3\2\2\2\u0243"+
		"\u0244\7\27\2\2\u0244Y\3\2\2\2\u0245\u0246\7a\2\2\u0246[\3\2\2\2\u0247"+
		"\u0248\t\5\2\2\u0248]\3\2\2\2\u0249\u024a\7\16\2\2\u024a\u024f\5\u0084"+
		"C\2\u024b\u024c\7\17\2\2\u024c\u024e\5\u0084C\2\u024d\u024b\3\2\2\2\u024e"+
		"\u0251\3\2\2\2\u024f\u024d\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0252\3\2"+
		"\2\2\u0251\u024f\3\2\2\2\u0252\u0253\7C\2\2\u0253_\3\2\2\2\u0254\u0260"+
		"\7d\2\2\u0255\u0256\7\16\2\2\u0256\u025b\5\u0084C\2\u0257\u0258\7\17\2"+
		"\2\u0258\u025a\5\u0084C\2\u0259\u0257\3\2\2\2\u025a\u025d\3\2\2\2\u025b"+
		"\u0259\3\2\2\2\u025b\u025c\3\2\2\2\u025c\u025e\3\2\2\2\u025d\u025b\3\2"+
		"\2\2\u025e\u025f\7C\2\2\u025f\u0261\3\2\2\2\u0260\u0255\3\2\2\2\u0260"+
		"\u0261\3\2\2\2\u0261\u0262\3\2\2\2\u0262\u0263\7$\2\2\u0263\u0264\7\16"+
		"\2\2\u0264\u0265\5b\62\2\u0265\u0266\7C\2\2\u0266a\3\2\2\2\u0267\u026d"+
		"\5f\64\2\u0268\u0269\7\16\2\2\u0269\u026a\5b\62\2\u026a\u026b\7C\2\2\u026b"+
		"\u026d\3\2\2\2\u026c\u0267\3\2\2\2\u026c\u0268\3\2\2\2\u026d\u0271\3\2"+
		"\2\2\u026e\u0270\5d\63\2\u026f\u026e\3\2\2\2\u0270\u0273\3\2\2\2\u0271"+
		"\u026f\3\2\2\2\u0271\u0272\3\2\2\2\u0272\u0275\3\2\2\2\u0273\u0271\3\2"+
		"\2\2\u0274\u0276\5z>\2\u0275\u0274\3\2\2\2\u0275\u0276\3\2\2\2\u0276\u0278"+
		"\3\2\2\2\u0277\u0279\5\u0080A\2\u0278\u0277\3\2\2\2\u0278\u0279\3\2\2"+
		"\2\u0279c\3\2\2\2\u027a\u0280\7T\2\2\u027b\u0281\5f\64\2\u027c\u027d\7"+
		"\16\2\2\u027d\u027e\5b\62\2\u027e\u027f\7C\2\2\u027f\u0281\3\2\2\2\u0280"+
		"\u027b\3\2\2\2\u0280\u027c\3\2\2\2\u0281e\3\2\2\2\u0282\u0283\5h\65\2"+
		"\u0283\u0285\5n8\2\u0284\u0286\5t;\2\u0285\u0284\3\2\2\2\u0285\u0286\3"+
		"\2\2\2\u0286\u0288\3\2\2\2\u0287\u0289\5v<\2\u0288\u0287\3\2\2\2\u0288"+
		"\u0289\3\2\2\2\u0289\u028b\3\2\2\2\u028a\u028c\5x=\2\u028b\u028a\3\2\2"+
		"\2\u028b\u028c\3\2\2\2\u028c\u028e\3\2\2\2\u028d\u028f\5z>\2\u028e\u028d"+
		"\3\2\2\2\u028e\u028f\3\2\2\2\u028f\u0291\3\2\2\2\u0290\u0292\5\u0080A"+
		"\2\u0291\u0290\3\2\2\2\u0291\u0292\3\2\2\2\u0292g\3\2\2\2\u0293\u0295"+
		"\7\63\2\2\u0294\u0296\5j\66\2\u0295\u0294\3\2\2\2\u0295\u0296\3\2\2\2"+
		"\u0296\u02a0\3\2\2\2\u0297\u02a1\7F\2\2\u0298\u029d\5l\67\2\u0299\u029a"+
		"\7\17\2\2\u029a\u029c\5l\67\2\u029b\u0299\3\2\2\2\u029c\u029f\3\2\2\2"+
		"\u029d\u029b\3\2\2\2\u029d\u029e\3\2\2\2\u029e\u02a1\3\2\2\2\u029f\u029d"+
		"\3\2\2\2\u02a0\u0297\3\2\2\2\u02a0\u0298\3\2\2\2\u02a1i\3\2\2\2\u02a2"+
		"\u02a3\t\6\2\2\u02a3k\3\2\2\2\u02a4\u02a6\5\u0084C\2\u02a5\u02a7\7$\2"+
		"\2\u02a6\u02a5\3\2\2\2\u02a6\u02a7\3\2\2\2\u02a7\u02a9\3\2\2\2\u02a8\u02aa"+
		"\7d\2\2\u02a9\u02a8\3\2\2\2\u02a9\u02aa\3\2\2\2\u02aa\u02b3\3\2\2\2\u02ab"+
		"\u02ad\5\u0090I\2\u02ac\u02ae\7$\2\2\u02ad\u02ac\3\2\2\2\u02ad\u02ae\3"+
		"\2\2\2\u02ae\u02b0\3\2\2\2\u02af\u02b1\7d\2\2\u02b0\u02af\3\2\2\2\u02b0"+
		"\u02b1\3\2\2\2\u02b1\u02b3\3\2\2\2\u02b2\u02a4\3\2\2\2\u02b2\u02ab\3\2"+
		"\2\2\u02b3m\3\2\2\2\u02b4\u02b5\7\6\2\2\u02b5\u02ba\5p9\2\u02b6\u02b7"+
		"\7\17\2\2\u02b7\u02b9\5p9\2\u02b8\u02b6\3\2\2\2\u02b9\u02bc\3\2\2\2\u02ba"+
		"\u02b8\3\2\2\2\u02ba\u02bb\3\2\2\2\u02bbo\3\2\2\2\u02bc\u02ba\3\2\2\2"+
		"\u02bd\u02be\b9\1\2\u02be\u02bf\7\16\2\2\u02bf\u02c1\5p9\2\u02c0\u02c2"+
		"\7R\2\2\u02c1\u02c0\3\2\2\2\u02c1\u02c2\3\2\2\2\u02c2\u02c3\3\2\2\2\u02c3"+
		"\u02c4\7+\2\2\u02c4\u02c5\5p9\2\u02c5\u02c6\7*\2\2\u02c6\u02c7\5\u0086"+
		"D\2\u02c7\u02c9\7C\2\2\u02c8\u02ca\5~@\2\u02c9\u02c8\3\2\2\2\u02c9\u02ca"+
		"\3\2\2\2\u02ca\u02db\3\2\2\2\u02cb\u02cc\7\16\2\2\u02cc\u02cd\5p9\2\u02cd"+
		"\u02ce\7S\2\2\u02ce\u02cf\5p9\2\u02cf\u02d1\7C\2\2\u02d0\u02d2\5~@\2\u02d1"+
		"\u02d0\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d2\u02db\3\2\2\2\u02d3\u02d4\7\16"+
		"\2\2\u02d4\u02d5\5b\62\2\u02d5\u02d7\7C\2\2\u02d6\u02d8\5~@\2\u02d7\u02d6"+
		"\3\2\2\2\u02d7\u02d8\3\2\2\2\u02d8\u02db\3\2\2\2\u02d9\u02db\5r:\2\u02da"+
		"\u02bd\3\2\2\2\u02da\u02cb\3\2\2\2\u02da\u02d3\3\2\2\2\u02da\u02d9\3\2"+
		"\2\2\u02db\u02ef\3\2\2\2\u02dc\u02de\f\b\2\2\u02dd\u02df\7R\2\2\u02de"+
		"\u02dd\3\2\2\2\u02de\u02df\3\2\2\2\u02df\u02e0\3\2\2\2\u02e0\u02e1\7+"+
		"\2\2\u02e1\u02e2\5p9\2\u02e2\u02e3\7*\2\2\u02e3\u02e5\5\u0086D\2\u02e4"+
		"\u02e6\5~@\2\u02e5\u02e4\3\2\2\2\u02e5\u02e6\3\2\2\2\u02e6\u02ee\3\2\2"+
		"\2\u02e7\u02e8\f\7\2\2\u02e8\u02e9\7S\2\2\u02e9\u02eb\5p9\2\u02ea\u02ec"+
		"\5~@\2\u02eb\u02ea\3\2\2\2\u02eb\u02ec\3\2\2\2\u02ec\u02ee\3\2\2\2\u02ed"+
		"\u02dc\3\2\2\2\u02ed\u02e7\3\2\2\2\u02ee\u02f1\3\2\2\2\u02ef\u02ed\3\2"+
		"\2\2\u02ef\u02f0\3\2\2\2\u02f0q\3\2\2\2\u02f1\u02ef\3\2\2\2\u02f2\u02f4"+
		"\5\u0082B\2\u02f3\u02f5\5~@\2\u02f4\u02f3\3\2\2\2\u02f4\u02f5\3\2\2\2"+
		"\u02f5s\3\2\2\2\u02f6\u02f7\7\'\2\2\u02f7\u02f8\5\u0086D\2\u02f8u\3\2"+
		"\2\2\u02f9\u02fa\7\f\2\2\u02fa\u02fb\7%\2\2\u02fb\u0300\5\u0084C\2\u02fc"+
		"\u02fd\7\17\2\2\u02fd\u02ff\5\u0084C\2\u02fe\u02fc\3\2\2\2\u02ff\u0302"+
		"\3\2\2\2\u0300\u02fe\3\2\2\2\u0300\u0301\3\2\2\2\u0301w\3\2\2\2\u0302"+
		"\u0300\3\2\2\2\u0303\u0304\7\32\2\2\u0304\u0305\5\u0086D\2\u0305y\3\2"+
		"\2\2\u0306\u0307\7\62\2\2\u0307\u0308\7%\2\2\u0308\u030d\5|?\2\u0309\u030a"+
		"\7\17\2\2\u030a\u030c\5|?\2\u030b\u0309\3\2\2\2\u030c\u030f\3\2\2\2\u030d"+
		"\u030b\3\2\2\2\u030d\u030e\3\2\2\2\u030e{\3\2\2\2\u030f\u030d\3\2\2\2"+
		"\u0310\u0312\7W\2\2\u0311\u0313\7Q\2\2\u0312\u0311\3\2\2\2\u0312\u0313"+
		"\3\2\2\2\u0313\u0319\3\2\2\2\u0314\u0316\5\u0084C\2\u0315\u0317\7Q\2\2"+
		"\u0316\u0315\3\2\2\2\u0316\u0317\3\2\2\2\u0317\u0319\3\2\2\2\u0318\u0310"+
		"\3\2\2\2\u0318\u0314\3\2\2\2\u0319}\3\2\2\2\u031a\u031c\7$\2\2\u031b\u031a"+
		"\3\2\2\2\u031b\u031c\3\2\2\2\u031c\u031d\3\2\2\2\u031d\u031e\7d\2\2\u031e"+
		"\177\3\2\2\2\u031f\u0320\7\31\2\2\u0320\u0322\7\30\2\2\u0321\u0323\7W"+
		"\2\2\u0322\u0321\3\2\2\2\u0322\u0323\3\2\2\2\u0323\u0324\3\2\2\2\u0324"+
		"\u0325\t\7\2\2\u0325\u0326\7\"\2\2\u0326\u0081\3\2\2\2\u0327\u032c\7d"+
		"\2\2\u0328\u0329\7d\2\2\u0329\u032a\7\66\2\2\u032a\u032c\7d\2\2\u032b"+
		"\u0327\3\2\2\2\u032b\u0328\3\2\2\2\u032c\u0083\3\2\2\2\u032d\u0332\7d"+
		"\2\2\u032e\u032f\7d\2\2\u032f\u0330\7\66\2\2\u0330\u0332\7d\2\2\u0331"+
		"\u032d\3\2\2\2\u0331\u032e\3\2\2\2\u0332\u0085\3\2\2\2\u0333\u0337\5\u008a"+
		"F\2\u0334\u0336\5\u0088E\2\u0335\u0334\3\2\2\2\u0336\u0339\3\2\2\2\u0337"+
		"\u0335\3\2\2\2\u0337\u0338\3\2\2\2\u0338\u0087\3\2\2\2\u0339\u0337\3\2"+
		"\2\2\u033a\u033b\t\b\2\2\u033b\u033c\5\u008aF\2\u033c\u0089\3\2\2\2\u033d"+
		"\u033f\7O\2\2\u033e\u033d\3\2\2\2\u033e\u033f\3\2\2\2\u033f\u0345\3\2"+
		"\2\2\u0340\u0346\5\u008cG\2\u0341\u0342\7\16\2\2\u0342\u0343\5\u0086D"+
		"\2\u0343\u0344\7C\2\2\u0344\u0346\3\2\2\2\u0345\u0340\3\2\2\2\u0345\u0341"+
		"\3\2\2\2\u0346\u008b\3\2\2\2\u0347\u0348\5\u0090I\2\u0348\u0349\5\u008e"+
		"H\2\u0349\u034a\5\u0090I\2\u034a\u0354\3\2\2\2\u034b\u034c\5\u0090I\2"+
		"\u034c\u034d\7L\2\2\u034d\u0354\3\2\2\2\u034e\u034f\7\b\2\2\u034f\u0350"+
		"\7\16\2\2\u0350\u0351\5f\64\2\u0351\u0352\7C\2\2\u0352\u0354\3\2\2\2\u0353"+
		"\u0347\3\2\2\2\u0353\u034b\3\2\2\2\u0353\u034e\3\2\2\2\u0354\u008d\3\2"+
		"\2\2\u0355\u0356\t\t\2\2\u0356\u008f\3\2\2\2\u0357\u0358\bI\1\2\u0358"+
		"\u0359\5\u0094K\2\u0359\u035a\7\16\2\2\u035a\u035f\5\u0090I\2\u035b\u035c"+
		"\7\17\2\2\u035c\u035e\5\u0090I\2\u035d\u035b\3\2\2\2\u035e\u0361\3\2\2"+
		"\2\u035f\u035d\3\2\2\2\u035f\u0360\3\2\2\2\u0360\u0362\3\2\2\2\u0361\u035f"+
		"\3\2\2\2\u0362\u0363\7C\2\2\u0363\u0392\3\2\2\2\u0364\u0365\7G\2\2\u0365"+
		"\u0366\7\16\2\2\u0366\u0367\7V\2\2\u0367\u0368\5\u0090I\2\u0368\u0369"+
		"\7C\2\2\u0369\u0392\3\2\2\2\u036a\u036b\7\16\2\2\u036b\u036e\5\u0090I"+
		"\2\u036c\u036d\7\17\2\2\u036d\u036f\5\u0090I\2\u036e\u036c\3\2\2\2\u036f"+
		"\u0370\3\2\2\2\u0370\u036e\3\2\2\2\u0370\u0371\3\2\2\2\u0371\u0372\3\2"+
		"\2\2\u0372\u0373\7C\2\2\u0373\u0392\3\2\2\2\u0374\u0375\7G\2\2\u0375\u0376"+
		"\7\16\2\2\u0376\u0377\7F\2\2\u0377\u0392\7C\2\2\u0378\u0392\5\u0096L\2"+
		"\u0379\u0392\5\u0084C\2\u037a\u037b\7\16\2\2\u037b\u037c\5f\64\2\u037c"+
		"\u037d\7C\2\2\u037d\u0392\3\2\2\2\u037e\u037f\7\16\2\2\u037f\u0380\5\u0090"+
		"I\2\u0380\u0381\7C\2\2\u0381\u0392\3\2\2\2\u0382\u0392\7P\2\2\u0383\u0384"+
		"\7\r\2\2\u0384\u0388\5\u0092J\2\u0385\u0387\5\u0092J\2\u0386\u0385\3\2"+
		"\2\2\u0387\u038a\3\2\2\2\u0388\u0386\3\2\2\2\u0388\u0389\3\2\2\2\u0389"+
		"\u038b\3\2\2\2\u038a\u0388\3\2\2\2\u038b\u038c\7&\2\2\u038c\u038d\5\u0090"+
		"I\2\u038d\u038f\7)\2\2\u038e\u0390\7\r\2\2\u038f\u038e\3\2\2\2\u038f\u0390"+
		"\3\2\2\2\u0390\u0392\3\2\2\2\u0391\u0357\3\2\2\2\u0391\u0364\3\2\2\2\u0391"+
		"\u036a\3\2\2\2\u0391\u0374\3\2\2\2\u0391\u0378\3\2\2\2\u0391\u0379\3\2"+
		"\2\2\u0391\u037a\3\2\2\2\u0391\u037e\3\2\2\2\u0391\u0382\3\2\2\2\u0391"+
		"\u0383\3\2\2\2\u0392\u039e\3\2\2\2\u0393\u0394\f\17\2\2\u0394\u0395\t"+
		"\n\2\2\u0395\u039d\5\u0090I\20\u0396\u0397\f\16\2\2\u0397\u0398\t\13\2"+
		"\2\u0398\u039d\5\u0090I\17\u0399\u039a\f\r\2\2\u039a\u039b\7H\2\2\u039b"+
		"\u039d\5\u0090I\16\u039c\u0393\3\2\2\2\u039c\u0396\3\2\2\2\u039c\u0399"+
		"\3\2\2\2\u039d\u03a0\3\2\2\2\u039e\u039c\3\2\2\2\u039e\u039f\3\2\2\2\u039f"+
		"\u0091\3\2\2\2\u03a0\u039e\3\2\2\2\u03a1\u03a2\7\67\2\2\u03a2\u03a3\5"+
		"\u0086D\2\u03a3\u03a4\7.\2\2\u03a4\u03a5\5\u0090I\2\u03a5\u0093\3\2\2"+
		"\2\u03a6\u03a7\t\f\2\2\u03a7\u0095\3\2\2\2\u03a8\u03aa\7I\2\2\u03a9\u03a8"+
		"\3\2\2\2\u03a9\u03aa\3\2\2\2\u03aa\u03ab\3\2\2\2\u03ab\u03ae\7W\2\2\u03ac"+
		"\u03ad\7\66\2\2\u03ad\u03af\7W\2\2\u03ae\u03ac\3\2\2\2\u03ae\u03af\3\2"+
		"\2\2\u03af\u03b2\3\2\2\2\u03b0\u03b2\7E\2\2\u03b1\u03a9\3\2\2\2\u03b1"+
		"\u03b0\3\2\2\2\u03b2\u0097\3\2\2\2h\u00bf\u00c2\u00c8\u00d2\u00dc\u00e1"+
		"\u00e8\u00ed\u00f3\u00f7\u0101\u0106\u010a\u010d\u0110\u011c\u012b\u0131"+
		"\u014f\u015e\u016c\u0173\u017b\u0182\u0184\u018c\u0194\u0199\u019d\u01a2"+
		"\u01a4\u01ac\u01bb\u01cc\u01d3\u01d8\u01da\u01ec\u01f8\u0207\u0211\u0218"+
		"\u021d\u0220\u022a\u023a\u024f\u025b\u0260\u026c\u0271\u0275\u0278\u0280"+
		"\u0285\u0288\u028b\u028e\u0291\u0295\u029d\u02a0\u02a6\u02a9\u02ad\u02b0"+
		"\u02b2\u02ba\u02c1\u02c9\u02d1\u02d7\u02da\u02de\u02e5\u02eb\u02ed\u02ef"+
		"\u02f4\u0300\u030d\u0312\u0316\u0318\u031b\u0322\u032b\u0331\u0337\u033e"+
		"\u0345\u0353\u035f\u0370\u0388\u038f\u0391\u039c\u039e\u03a9\u03ae\u03b1";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}