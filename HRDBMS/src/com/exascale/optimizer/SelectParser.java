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
		T__52=1, T__51=2, T__50=3, T__49=4, T__48=5, T__47=6, T__46=7, T__45=8, 
		T__44=9, T__43=10, T__42=11, T__41=12, T__40=13, T__39=14, T__38=15, T__37=16, 
		T__36=17, T__35=18, T__34=19, T__33=20, T__32=21, T__31=22, T__30=23, 
		T__29=24, T__28=25, T__27=26, T__26=27, T__25=28, T__24=29, T__23=30, 
		T__22=31, T__21=32, T__20=33, T__19=34, T__18=35, T__17=36, T__16=37, 
		T__15=38, T__14=39, T__13=40, T__12=41, T__11=42, T__10=43, T__9=44, T__8=45, 
		T__7=46, T__6=47, T__5=48, T__4=49, T__3=50, T__2=51, T__1=52, T__0=53, 
		STRING=54, STAR=55, COUNT=56, CONCAT=57, NEGATIVE=58, EQUALS=59, OPERATOR=60, 
		NULLOPERATOR=61, AND=62, OR=63, NOT=64, NULL=65, DIRECTION=66, JOINTYPE=67, 
		CROSSJOIN=68, TABLECOMBINATION=69, COLUMN=70, DISTINCT=71, INTEGER=72, 
		WS=73, UNIQUE=74, REPLACE=75, RESUME=76, NONE=77, ALL=78, ANYTEXT=79, 
		HASH=80, RANGE=81, DATE=82, COLORDER=83, ORGANIZATION=84, IDENTIFIER=85, 
		ANY=86;
	public static final String[] tokenNames = {
		"<INVALID>", "'DOUBLE'", "'INTEGER'", "'FROM'", "'EXISTS'", "'{'", "'GROUP'", 
		"'CASE'", "'('", "','", "'PRIMARY'", "'DELIMITER'", "'LOAD'", "'VALUES'", 
		"'VARCHAR'", "'UPDATE'", "'DELETE'", "'BIGINT'", "'FIRST'", "'FETCH'", 
		"'HAVING'", "'INSERT'", "'+'", "'CREATE'", "'/'", "'ONLY'", "'TABLE'", 
		"'AS'", "'BY'", "'ELSE'", "'WHERE'", "'INTO'", "'END'", "'ON'", "'JOIN'", 
		"'}'", "'VIEW'", "'THEN'", "'KEY'", "'ORDER'", "'SELECT'", "'WITH'", "'.'", 
		"'DROP'", "'WHEN'", "'ROW'", "'CHAR'", "'INDEX'", "'ROWS'", "'|'", "'RUNSTATS'", 
		"'FLOAT'", "')'", "'SET'", "STRING", "'*'", "'COUNT'", "'||'", "'-'", 
		"'='", "OPERATOR", "NULLOPERATOR", "'AND'", "'OR'", "'NOT'", "'NULL'", 
		"DIRECTION", "JOINTYPE", "'CROSS JOIN'", "TABLECOMBINATION", "'COLUMN'", 
		"'DISTINCT'", "INTEGER", "WS", "'UNIQUE'", "'REPLACE'", "'RESUME'", "'NONE'", 
		"'ALL'", "'ANY'", "'HASH'", "'RANGE'", "'DATE'", "'COLORDER'", "'ORGANIZATION'", 
		"IDENTIFIER", "ANY"
	};
	public static final int
		RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_valuesList = 3, 
		RULE_update = 4, RULE_delete = 5, RULE_createTable = 6, RULE_organization = 7, 
		RULE_colOrder = 8, RULE_groupExp = 9, RULE_realGroupExp = 10, RULE_groupDef = 11, 
		RULE_rangeExp = 12, RULE_nodeExp = 13, RULE_realNodeExp = 14, RULE_integerSet = 15, 
		RULE_hashExp = 16, RULE_columnSet = 17, RULE_rangeType = 18, RULE_rangeSet = 19, 
		RULE_deviceExp = 20, RULE_dropTable = 21, RULE_createView = 22, RULE_dropView = 23, 
		RULE_createIndex = 24, RULE_dropIndex = 25, RULE_load = 26, RULE_any = 27, 
		RULE_remainder = 28, RULE_indexDef = 29, RULE_colDef = 30, RULE_primaryKey = 31, 
		RULE_notNull = 32, RULE_primary = 33, RULE_dataType = 34, RULE_char2 = 35, 
		RULE_int2 = 36, RULE_long2 = 37, RULE_date2 = 38, RULE_float2 = 39, RULE_colList = 40, 
		RULE_commonTableExpression = 41, RULE_fullSelect = 42, RULE_connectedSelect = 43, 
		RULE_subSelect = 44, RULE_selectClause = 45, RULE_selecthow = 46, RULE_selectListEntry = 47, 
		RULE_fromClause = 48, RULE_tableReference = 49, RULE_singleTable = 50, 
		RULE_whereClause = 51, RULE_groupBy = 52, RULE_havingClause = 53, RULE_orderBy = 54, 
		RULE_sortKey = 55, RULE_correlationClause = 56, RULE_fetchFirst = 57, 
		RULE_tableName = 58, RULE_columnName = 59, RULE_searchCondition = 60, 
		RULE_connectedSearchClause = 61, RULE_searchClause = 62, RULE_predicate = 63, 
		RULE_operator = 64, RULE_expression = 65, RULE_caseCase = 66, RULE_identifier = 67, 
		RULE_literal = 68;
	public static final String[] ruleNames = {
		"select", "runstats", "insert", "valuesList", "update", "delete", "createTable", 
		"organization", "colOrder", "groupExp", "realGroupExp", "groupDef", "rangeExp", 
		"nodeExp", "realNodeExp", "integerSet", "hashExp", "columnSet", "rangeType", 
		"rangeSet", "deviceExp", "dropTable", "createView", "dropView", "createIndex", 
		"dropIndex", "load", "any", "remainder", "indexDef", "colDef", "primaryKey", 
		"notNull", "primary", "dataType", "char2", "int2", "long2", "date2", "float2", 
		"colList", "commonTableExpression", "fullSelect", "connectedSelect", "subSelect", 
		"selectClause", "selecthow", "selectListEntry", "fromClause", "tableReference", 
		"singleTable", "whereClause", "groupBy", "havingClause", "orderBy", "sortKey", 
		"correlationClause", "fetchFirst", "tableName", "columnName", "searchCondition", 
		"connectedSearchClause", "searchClause", "predicate", "operator", "expression", 
		"caseCase", "identifier", "literal"
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
			setState(186);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(138); insert();
				setState(139); match(EOF);
				}
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(141); update();
				setState(142); match(EOF);
				}
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(144); delete();
				setState(145); match(EOF);
				}
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(147); createTable();
				setState(148); match(EOF);
				}
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(150); createIndex();
				setState(151); match(EOF);
				}
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(153); createView();
				setState(154); match(EOF);
				}
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				{
				setState(156); dropTable();
				setState(157); match(EOF);
				}
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				{
				setState(159); dropIndex();
				setState(160); match(EOF);
				}
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				{
				setState(162); dropView();
				setState(163); match(EOF);
				}
				}
				break;

			case 10:
				enterOuterAlt(_localctx, 10);
				{
				{
				setState(165); load();
				setState(166); match(EOF);
				}
				}
				break;

			case 11:
				enterOuterAlt(_localctx, 11);
				{
				{
				setState(168); runstats();
				setState(169); match(EOF);
				}
				}
				break;

			case 12:
				enterOuterAlt(_localctx, 12);
				{
				{
				{
				setState(180);
				_la = _input.LA(1);
				if (_la==41) {
					{
					setState(171); match(41);
					setState(172); commonTableExpression();
					setState(177);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==9) {
						{
						{
						setState(173); match(9);
						setState(174); commonTableExpression();
						}
						}
						setState(179);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(182); fullSelect();
				}
				setState(184); match(EOF);
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
			setState(188); match(50);
			setState(189); match(33);
			setState(190); tableName();
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
		public List<ValuesListContext> valuesList() {
			return getRuleContexts(ValuesListContext.class);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public ValuesListContext valuesList(int i) {
			return getRuleContext(ValuesListContext.class,i);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
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
			setState(192); match(21);
			setState(193); match(31);
			setState(194); tableName();
			setState(208);
			switch (_input.LA(1)) {
			case 3:
			case 8:
			case 40:
				{
				{
				setState(196);
				_la = _input.LA(1);
				if (_la==3) {
					{
					setState(195); match(3);
					}
				}

				setState(198); fullSelect();
				}
				}
				break;
			case 13:
				{
				{
				setState(199); match(13);
				setState(200); valuesList();
				setState(205);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==9) {
					{
					{
					setState(201); match(9);
					setState(202); valuesList();
					}
					}
					setState(207);
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

	public static class ValuesListContext extends ParserRuleContext {
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ValuesListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valuesList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).enterValuesList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SelectListener ) ((SelectListener)listener).exitValuesList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitValuesList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValuesListContext valuesList() throws RecognitionException {
		ValuesListContext _localctx = new ValuesListContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_valuesList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(210); match(8);
			setState(211); expression(0);
			setState(216);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(212); match(9);
				setState(213); expression(0);
				}
				}
				setState(218);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(219); match(52);
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
		enterRule(_localctx, 8, RULE_update);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(221); match(15);
			setState(222); tableName();
			setState(223); match(53);
			setState(226);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(224); columnName();
				}
				break;
			case 8:
				{
				setState(225); colList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(228); match(EQUALS);
			setState(229); expression(0);
			setState(231);
			_la = _input.LA(1);
			if (_la==30) {
				{
				setState(230); whereClause();
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
		enterRule(_localctx, 10, RULE_delete);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(233); match(16);
			setState(234); match(3);
			setState(235); tableName();
			setState(237);
			_la = _input.LA(1);
			if (_la==30) {
				{
				setState(236); whereClause();
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
		enterRule(_localctx, 12, RULE_createTable);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(239); match(23);
			setState(241);
			_la = _input.LA(1);
			if (_la==COLUMN) {
				{
				setState(240); match(COLUMN);
				}
			}

			setState(243); match(26);
			setState(244); tableName();
			setState(245); match(8);
			setState(246); colDef();
			setState(251);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(247); match(9);
					setState(248); colDef();
					}
					} 
				}
				setState(253);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			}
			setState(256);
			_la = _input.LA(1);
			if (_la==9) {
				{
				setState(254); match(9);
				setState(255); primaryKey();
				}
			}

			setState(258); match(52);
			setState(260);
			_la = _input.LA(1);
			if (_la==COLORDER) {
				{
				setState(259); colOrder();
				}
			}

			setState(263);
			_la = _input.LA(1);
			if (_la==ORGANIZATION) {
				{
				setState(262); organization();
				}
			}

			setState(266);
			switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
			case 1:
				{
				setState(265); groupExp();
				}
				break;
			}
			setState(268); nodeExp();
			setState(269); deviceExp();
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
		enterRule(_localctx, 14, RULE_organization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(271); match(ORGANIZATION);
			setState(272); match(8);
			setState(273); match(INTEGER);
			setState(278);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(274); match(9);
				setState(275); match(INTEGER);
				}
				}
				setState(280);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(281); match(52);
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
		enterRule(_localctx, 16, RULE_colOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(283); match(COLORDER);
			setState(284); match(8);
			setState(285); match(INTEGER);
			setState(290);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(286); match(9);
				setState(287); match(INTEGER);
				}
				}
				setState(292);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(293); match(52);
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
		enterRule(_localctx, 18, RULE_groupExp);
		try {
			setState(297);
			switch (_input.LA(1)) {
			case NONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(295); match(NONE);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 2);
				{
				setState(296); realGroupExp();
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
		enterRule(_localctx, 20, RULE_realGroupExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(299); match(5);
			setState(300); groupDef();
			setState(305);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==49) {
				{
				{
				setState(301); match(49);
				setState(302); groupDef();
				}
				}
				setState(307);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(308); match(35);
			setState(314);
			_la = _input.LA(1);
			if (_la==9) {
				{
				setState(309); match(9);
				setState(312);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(310); hashExp();
					}
					break;
				case RANGE:
					{
					setState(311); rangeType();
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
		enterRule(_localctx, 22, RULE_groupDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(316); match(5);
			setState(317); match(INTEGER);
			setState(322);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==49) {
				{
				{
				setState(318); match(49);
				setState(319); match(INTEGER);
				}
				}
				setState(324);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(325); match(35);
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
		enterRule(_localctx, 24, RULE_rangeExp);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(330);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(327);
					matchWildcard();
					}
					} 
				}
				setState(332);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
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
		enterRule(_localctx, 26, RULE_nodeExp);
		try {
			setState(335);
			switch (_input.LA(1)) {
			case ANYTEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(333); match(ANYTEXT);
				}
				break;
			case 5:
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(334); realNodeExp();
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
		enterRule(_localctx, 28, RULE_realNodeExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(339);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(337); match(ALL);
				}
				break;
			case 5:
				{
				setState(338); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(346);
			_la = _input.LA(1);
			if (_la==9) {
				{
				setState(341); match(9);
				setState(344);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(342); hashExp();
					}
					break;
				case RANGE:
					{
					setState(343); rangeType();
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
		enterRule(_localctx, 30, RULE_integerSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(348); match(5);
			setState(349); match(INTEGER);
			setState(354);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==49) {
				{
				{
				setState(350); match(49);
				setState(351); match(INTEGER);
				}
				}
				setState(356);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(357); match(35);
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
		enterRule(_localctx, 32, RULE_hashExp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(359); match(HASH);
			setState(360); match(9);
			setState(361); columnSet();
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
		enterRule(_localctx, 34, RULE_columnSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(363); match(5);
			setState(364); columnName();
			setState(369);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==49) {
				{
				{
				setState(365); match(49);
				setState(366); columnName();
				}
				}
				setState(371);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(372); match(35);
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
		enterRule(_localctx, 36, RULE_rangeType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(374); match(RANGE);
			setState(375); match(9);
			setState(376); columnName();
			setState(377); match(9);
			setState(378); rangeSet();
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
		enterRule(_localctx, 38, RULE_rangeSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(380); match(5);
			setState(381); rangeExp();
			setState(386);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==49) {
				{
				{
				setState(382); match(49);
				setState(383); rangeExp();
				}
				}
				setState(388);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(389); match(35);
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
		enterRule(_localctx, 40, RULE_deviceExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(393);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(391); match(ALL);
				}
				break;
			case 5:
				{
				setState(392); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(400);
			_la = _input.LA(1);
			if (_la==9) {
				{
				setState(395); match(9);
				setState(398);
				switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
				case 1:
					{
					setState(396); hashExp();
					}
					break;

				case 2:
					{
					setState(397); rangeExp();
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
		enterRule(_localctx, 42, RULE_dropTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(402); match(43);
			setState(403); match(26);
			setState(404); tableName();
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
		enterRule(_localctx, 44, RULE_createView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(406); match(23);
			setState(407); match(36);
			setState(408); tableName();
			setState(409); match(27);
			setState(410); fullSelect();
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
		enterRule(_localctx, 46, RULE_dropView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(412); match(43);
			setState(413); match(36);
			setState(414); tableName();
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
		enterRule(_localctx, 48, RULE_createIndex);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(416); match(23);
			setState(418);
			_la = _input.LA(1);
			if (_la==UNIQUE) {
				{
				setState(417); match(UNIQUE);
				}
			}

			setState(420); match(47);
			setState(421); tableName();
			setState(422); match(33);
			setState(423); tableName();
			setState(424); match(8);
			setState(425); indexDef();
			setState(430);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(426); match(9);
				setState(427); indexDef();
				}
				}
				setState(432);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(433); match(52);
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
		enterRule(_localctx, 50, RULE_dropIndex);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(435); match(43);
			setState(436); match(47);
			setState(437); tableName();
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
		enterRule(_localctx, 52, RULE_load);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(439); match(12);
			setState(440);
			_la = _input.LA(1);
			if ( !(_la==REPLACE || _la==RESUME) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(441); match(31);
			setState(442); tableName();
			setState(445);
			_la = _input.LA(1);
			if (_la==11) {
				{
				setState(443); match(11);
				setState(444); any();
				}
			}

			setState(447); match(3);
			setState(448); remainder();
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
		enterRule(_localctx, 54, RULE_any);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(450);
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
		enterRule(_localctx, 56, RULE_remainder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(455);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1) | (1L << 2) | (1L << 3) | (1L << 4) | (1L << 5) | (1L << 6) | (1L << 7) | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 11) | (1L << 12) | (1L << 13) | (1L << 14) | (1L << 15) | (1L << 16) | (1L << 17) | (1L << 18) | (1L << 19) | (1L << 20) | (1L << 21) | (1L << 22) | (1L << 23) | (1L << 24) | (1L << 25) | (1L << 26) | (1L << 27) | (1L << 28) | (1L << 29) | (1L << 30) | (1L << 31) | (1L << 32) | (1L << 33) | (1L << 34) | (1L << 35) | (1L << 36) | (1L << 37) | (1L << 38) | (1L << 39) | (1L << 40) | (1L << 41) | (1L << 42) | (1L << 43) | (1L << 44) | (1L << 45) | (1L << 46) | (1L << 47) | (1L << 48) | (1L << 49) | (1L << 50) | (1L << 51) | (1L << 52) | (1L << 53) | (1L << STRING) | (1L << STAR) | (1L << COUNT) | (1L << CONCAT) | (1L << NEGATIVE) | (1L << EQUALS) | (1L << OPERATOR) | (1L << NULLOPERATOR) | (1L << AND) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (COLUMN - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (COLORDER - 64)) | (1L << (ORGANIZATION - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0)) {
				{
				{
				setState(452);
				matchWildcard();
				}
				}
				setState(457);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(458); match(EOF);
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
		enterRule(_localctx, 58, RULE_indexDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(460); columnName();
			setState(462);
			_la = _input.LA(1);
			if (_la==DIRECTION) {
				{
				setState(461); match(DIRECTION);
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
		enterRule(_localctx, 60, RULE_colDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(464); columnName();
			setState(465); dataType();
			setState(467);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(466); notNull();
				}
			}

			setState(470);
			_la = _input.LA(1);
			if (_la==10) {
				{
				setState(469); primary();
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
		enterRule(_localctx, 62, RULE_primaryKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(472); match(10);
			setState(473); match(38);
			setState(474); match(8);
			setState(475); columnName();
			setState(480);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(476); match(9);
				setState(477); columnName();
				}
				}
				setState(482);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(483); match(52);
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
		enterRule(_localctx, 64, RULE_notNull);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(485); match(NOT);
			setState(486); match(NULL);
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
		enterRule(_localctx, 66, RULE_primary);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(488); match(10);
			setState(489); match(38);
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
		enterRule(_localctx, 68, RULE_dataType);
		try {
			setState(496);
			switch (_input.LA(1)) {
			case 14:
			case 46:
				enterOuterAlt(_localctx, 1);
				{
				setState(491); char2();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(492); int2();
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 3);
				{
				setState(493); long2();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 4);
				{
				setState(494); date2();
				}
				break;
			case 1:
			case 51:
				enterOuterAlt(_localctx, 5);
				{
				setState(495); float2();
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
		enterRule(_localctx, 70, RULE_char2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(498);
			_la = _input.LA(1);
			if ( !(_la==14 || _la==46) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(499); match(8);
			setState(500); match(INTEGER);
			setState(501); match(52);
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
		enterRule(_localctx, 72, RULE_int2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(503); match(2);
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
		enterRule(_localctx, 74, RULE_long2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(505); match(17);
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
		enterRule(_localctx, 76, RULE_date2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(507); match(DATE);
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
		enterRule(_localctx, 78, RULE_float2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(509);
			_la = _input.LA(1);
			if ( !(_la==1 || _la==51) ) {
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
		enterRule(_localctx, 80, RULE_colList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(511); match(8);
			setState(512); columnName();
			setState(517);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(513); match(9);
				setState(514); columnName();
				}
				}
				setState(519);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(520); match(52);
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
		enterRule(_localctx, 82, RULE_commonTableExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(522); match(IDENTIFIER);
			setState(534);
			_la = _input.LA(1);
			if (_la==8) {
				{
				setState(523); match(8);
				setState(524); columnName();
				setState(529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==9) {
					{
					{
					setState(525); match(9);
					setState(526); columnName();
					}
					}
					setState(531);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(532); match(52);
				}
			}

			setState(536); match(27);
			setState(537); match(8);
			setState(538); fullSelect();
			setState(539); match(52);
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
		enterRule(_localctx, 84, RULE_fullSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(546);
			switch (_input.LA(1)) {
			case 40:
				{
				setState(541); subSelect();
				}
				break;
			case 8:
				{
				setState(542); match(8);
				setState(543); fullSelect();
				setState(544); match(52);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(551);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==TABLECOMBINATION) {
				{
				{
				setState(548); connectedSelect();
				}
				}
				setState(553);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(555);
			_la = _input.LA(1);
			if (_la==39) {
				{
				setState(554); orderBy();
				}
			}

			setState(558);
			_la = _input.LA(1);
			if (_la==19) {
				{
				setState(557); fetchFirst();
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
		enterRule(_localctx, 86, RULE_connectedSelect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(560); match(TABLECOMBINATION);
			setState(566);
			switch (_input.LA(1)) {
			case 40:
				{
				setState(561); subSelect();
				}
				break;
			case 8:
				{
				setState(562); match(8);
				setState(563); fullSelect();
				setState(564); match(52);
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
		enterRule(_localctx, 88, RULE_subSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568); selectClause();
			setState(569); fromClause();
			setState(571);
			_la = _input.LA(1);
			if (_la==30) {
				{
				setState(570); whereClause();
				}
			}

			setState(574);
			_la = _input.LA(1);
			if (_la==6) {
				{
				setState(573); groupBy();
				}
			}

			setState(577);
			_la = _input.LA(1);
			if (_la==20) {
				{
				setState(576); havingClause();
				}
			}

			setState(580);
			switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
			case 1:
				{
				setState(579); orderBy();
				}
				break;
			}
			setState(583);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				{
				setState(582); fetchFirst();
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
		enterRule(_localctx, 90, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(585); match(40);
			setState(587);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==ALL) {
				{
				setState(586); selecthow();
				}
			}

			setState(598);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(589); match(STAR);
				}
				break;
			case 7:
			case 8:
			case STRING:
			case COUNT:
			case NEGATIVE:
			case NULL:
			case INTEGER:
			case DATE:
			case IDENTIFIER:
				{
				{
				setState(590); selectListEntry();
				setState(595);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==9) {
					{
					{
					setState(591); match(9);
					setState(592); selectListEntry();
					}
					}
					setState(597);
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
		enterRule(_localctx, 92, RULE_selecthow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(600);
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
		enterRule(_localctx, 94, RULE_selectListEntry);
		int _la;
		try {
			setState(616);
			switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
			case 1:
				_localctx = new SelectColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(602); columnName();
				setState(604);
				_la = _input.LA(1);
				if (_la==27) {
					{
					setState(603); match(27);
					}
				}

				setState(607);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(606); match(IDENTIFIER);
					}
				}

				}
				break;

			case 2:
				_localctx = new SelectExpressionContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(609); expression(0);
				setState(611);
				_la = _input.LA(1);
				if (_la==27) {
					{
					setState(610); match(27);
					}
				}

				setState(614);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(613); match(IDENTIFIER);
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
		enterRule(_localctx, 96, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(618); match(3);
			setState(619); tableReference(0);
			setState(624);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(620); match(9);
				setState(621); tableReference(0);
				}
				}
				setState(626);
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
		int _startState = 98;
		enterRecursionRule(_localctx, 98, RULE_tableReference, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(656);
			switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
			case 1:
				{
				_localctx = new JoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(628); match(8);
				setState(629); tableReference(0);
				setState(631);
				_la = _input.LA(1);
				if (_la==JOINTYPE) {
					{
					setState(630); match(JOINTYPE);
					}
				}

				setState(633); match(34);
				setState(634); tableReference(0);
				setState(635); match(33);
				setState(636); searchCondition();
				setState(637); match(52);
				setState(639);
				switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
				case 1:
					{
					setState(638); correlationClause();
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
				setState(641); match(8);
				setState(642); tableReference(0);
				setState(643); match(CROSSJOIN);
				setState(644); tableReference(0);
				setState(645); match(52);
				setState(647);
				switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
				case 1:
					{
					setState(646); correlationClause();
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
				setState(649); match(8);
				setState(650); fullSelect();
				setState(651); match(52);
				setState(653);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
				case 1:
					{
					setState(652); correlationClause();
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
				setState(655); singleTable();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(677);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,74,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(675);
					switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
					case 1:
						{
						_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(658);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(660);
						_la = _input.LA(1);
						if (_la==JOINTYPE) {
							{
							setState(659); match(JOINTYPE);
							}
						}

						setState(662); match(34);
						setState(663); tableReference(0);
						setState(664); match(33);
						setState(665); searchCondition();
						setState(667);
						switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
						case 1:
							{
							setState(666); correlationClause();
							}
							break;
						}
						}
						break;

					case 2:
						{
						_localctx = new CrossJoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(669);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(670); match(CROSSJOIN);
						setState(671); tableReference(0);
						setState(673);
						switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
						case 1:
							{
							setState(672); correlationClause();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(679);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,74,_ctx);
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
		enterRule(_localctx, 100, RULE_singleTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(680); tableName();
			setState(682);
			switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
			case 1:
				{
				setState(681); correlationClause();
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
		enterRule(_localctx, 102, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(684); match(30);
			setState(685); searchCondition();
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
		enterRule(_localctx, 104, RULE_groupBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(687); match(6);
			setState(688); match(28);
			setState(689); columnName();
			setState(694);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(690); match(9);
				setState(691); columnName();
				}
				}
				setState(696);
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
		enterRule(_localctx, 106, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(697); match(20);
			setState(698); searchCondition();
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
		enterRule(_localctx, 108, RULE_orderBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(700); match(39);
			setState(701); match(28);
			setState(702); sortKey();
			setState(707);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==9) {
				{
				{
				setState(703); match(9);
				setState(704); sortKey();
				}
				}
				setState(709);
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
		enterRule(_localctx, 110, RULE_sortKey);
		int _la;
		try {
			setState(718);
			switch (_input.LA(1)) {
			case INTEGER:
				_localctx = new SortKeyIntContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(710); match(INTEGER);
				setState(712);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(711); match(DIRECTION);
					}
				}

				}
				break;
			case IDENTIFIER:
				_localctx = new SortKeyColContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(714); columnName();
				setState(716);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(715); match(DIRECTION);
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
		enterRule(_localctx, 112, RULE_correlationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(721);
			_la = _input.LA(1);
			if (_la==27) {
				{
				setState(720); match(27);
				}
			}

			setState(723); match(IDENTIFIER);
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
		enterRule(_localctx, 114, RULE_fetchFirst);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(725); match(19);
			setState(726); match(18);
			setState(728);
			_la = _input.LA(1);
			if (_la==INTEGER) {
				{
				setState(727); match(INTEGER);
				}
			}

			setState(730);
			_la = _input.LA(1);
			if ( !(_la==45 || _la==48) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(731); match(25);
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
		enterRule(_localctx, 116, RULE_tableName);
		try {
			setState(737);
			switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
			case 1:
				_localctx = new Table1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(733); match(IDENTIFIER);
				}
				break;

			case 2:
				_localctx = new Table2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(734); match(IDENTIFIER);
				setState(735); match(42);
				setState(736); match(IDENTIFIER);
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
		enterRule(_localctx, 118, RULE_columnName);
		try {
			setState(743);
			switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
			case 1:
				_localctx = new Col1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(739); match(IDENTIFIER);
				}
				break;

			case 2:
				_localctx = new Col2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(740); match(IDENTIFIER);
				setState(741); match(42);
				setState(742); match(IDENTIFIER);
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
		enterRule(_localctx, 120, RULE_searchCondition);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(745); searchClause();
			setState(749);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,85,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(746); connectedSearchClause();
					}
					} 
				}
				setState(751);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,85,_ctx);
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
		enterRule(_localctx, 122, RULE_connectedSearchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(752);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OR) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(753); searchClause();
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
		enterRule(_localctx, 124, RULE_searchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(756);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(755); match(NOT);
				}
			}

			setState(763);
			switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
			case 1:
				{
				setState(758); predicate();
				}
				break;

			case 2:
				{
				{
				setState(759); match(8);
				setState(760); searchCondition();
				setState(761); match(52);
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
		enterRule(_localctx, 126, RULE_predicate);
		try {
			setState(777);
			switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
			case 1:
				_localctx = new NormalPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(765); expression(0);
				setState(766); operator();
				setState(767); expression(0);
				}
				}
				break;

			case 2:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(769); expression(0);
				setState(770); match(NULLOPERATOR);
				}
				}
				break;

			case 3:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(772); match(4);
				setState(773); match(8);
				setState(774); subSelect();
				setState(775); match(52);
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
		enterRule(_localctx, 128, RULE_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(779);
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
		int _startState = 130;
		enterRecursionRule(_localctx, 130, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(839);
			switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
			case 1:
				{
				_localctx = new FunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(782); identifier();
				setState(783); match(8);
				setState(784); expression(0);
				setState(789);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==9) {
					{
					{
					setState(785); match(9);
					setState(786); expression(0);
					}
					}
					setState(791);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(792); match(52);
				}
				break;

			case 2:
				{
				_localctx = new CountDistinctContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(794); match(COUNT);
				setState(795); match(8);
				setState(796); match(DISTINCT);
				setState(797); expression(0);
				setState(798); match(52);
				}
				break;

			case 3:
				{
				_localctx = new ListContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(800); match(8);
				setState(801); expression(0);
				setState(804); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(802); match(9);
					setState(803); expression(0);
					}
					}
					setState(806); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==9 );
				setState(808); match(52);
				}
				break;

			case 4:
				{
				_localctx = new CountStarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(810); match(COUNT);
				setState(811); match(8);
				setState(812); match(STAR);
				setState(813); match(52);
				}
				break;

			case 5:
				{
				_localctx = new IsLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(814); literal();
				}
				break;

			case 6:
				{
				_localctx = new ColLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(815); columnName();
				}
				break;

			case 7:
				{
				_localctx = new ExpSelectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(816); match(8);
				setState(817); subSelect();
				setState(818); match(52);
				}
				break;

			case 8:
				{
				_localctx = new PExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(820); match(8);
				setState(821); expression(0);
				setState(822); match(52);
				}
				break;

			case 9:
				{
				_localctx = new NullExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(824); match(NULL);
				}
				break;

			case 10:
				{
				_localctx = new CaseExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(825); match(7);
				setState(826); caseCase();
				setState(830);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==44) {
					{
					{
					setState(827); caseCase();
					}
					}
					setState(832);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(833); match(29);
				setState(834); expression(0);
				setState(835); match(32);
				setState(837);
				switch ( getInterpreter().adaptivePredict(_input,92,_ctx) ) {
				case 1:
					{
					setState(836); match(7);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(852);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,95,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(850);
					switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
					case 1:
						{
						_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(841);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(842);
						((MulDivContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==24 || _la==STAR) ) {
							((MulDivContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(843); expression(14);
						}
						break;

					case 2:
						{
						_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(844);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(845);
						((AddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==22 || _la==NEGATIVE) ) {
							((AddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(846); expression(13);
						}
						break;

					case 3:
						{
						_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(847);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(848); match(CONCAT);
						setState(849); expression(12);
						}
						break;
					}
					} 
				}
				setState(854);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,95,_ctx);
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
		enterRule(_localctx, 132, RULE_caseCase);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(855); match(44);
			setState(856); searchCondition();
			setState(857); match(37);
			setState(858); expression(0);
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
		enterRule(_localctx, 134, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(860);
			_la = _input.LA(1);
			if ( !(((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & ((1L << (COUNT - 56)) | (1L << (DATE - 56)) | (1L << (IDENTIFIER - 56)))) != 0)) ) {
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
		enterRule(_localctx, 136, RULE_literal);
		int _la;
		try {
			setState(871);
			switch (_input.LA(1)) {
			case NEGATIVE:
			case INTEGER:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(863);
				_la = _input.LA(1);
				if (_la==NEGATIVE) {
					{
					setState(862); match(NEGATIVE);
					}
				}

				setState(865); match(INTEGER);
				setState(868);
				switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
				case 1:
					{
					setState(866); match(42);
					setState(867); match(INTEGER);
					}
					break;
				}
				}
				break;
			case STRING:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(870); match(STRING);
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
		case 49: return tableReference_sempred((TableReferenceContext)_localctx, predIndex);

		case 65: return expression_sempred((ExpressionContext)_localctx, predIndex);
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3X\u036c\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\3\2\3\2\3\2\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3"+
		"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\7\2\u00b2"+
		"\n\2\f\2\16\2\u00b5\13\2\5\2\u00b7\n\2\3\2\3\2\3\2\3\2\5\2\u00bd\n\2\3"+
		"\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4\u00c7\n\4\3\4\3\4\3\4\3\4\3\4\7\4\u00ce"+
		"\n\4\f\4\16\4\u00d1\13\4\5\4\u00d3\n\4\3\5\3\5\3\5\3\5\7\5\u00d9\n\5\f"+
		"\5\16\5\u00dc\13\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\5\6\u00e5\n\6\3\6\3\6\3"+
		"\6\5\6\u00ea\n\6\3\7\3\7\3\7\3\7\5\7\u00f0\n\7\3\b\3\b\5\b\u00f4\n\b\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\7\b\u00fc\n\b\f\b\16\b\u00ff\13\b\3\b\3\b\5\b\u0103"+
		"\n\b\3\b\3\b\5\b\u0107\n\b\3\b\5\b\u010a\n\b\3\b\5\b\u010d\n\b\3\b\3\b"+
		"\3\b\3\t\3\t\3\t\3\t\3\t\7\t\u0117\n\t\f\t\16\t\u011a\13\t\3\t\3\t\3\n"+
		"\3\n\3\n\3\n\3\n\7\n\u0123\n\n\f\n\16\n\u0126\13\n\3\n\3\n\3\13\3\13\5"+
		"\13\u012c\n\13\3\f\3\f\3\f\3\f\7\f\u0132\n\f\f\f\16\f\u0135\13\f\3\f\3"+
		"\f\3\f\3\f\5\f\u013b\n\f\5\f\u013d\n\f\3\r\3\r\3\r\3\r\7\r\u0143\n\r\f"+
		"\r\16\r\u0146\13\r\3\r\3\r\3\16\7\16\u014b\n\16\f\16\16\16\u014e\13\16"+
		"\3\17\3\17\5\17\u0152\n\17\3\20\3\20\5\20\u0156\n\20\3\20\3\20\3\20\5"+
		"\20\u015b\n\20\5\20\u015d\n\20\3\21\3\21\3\21\3\21\7\21\u0163\n\21\f\21"+
		"\16\21\u0166\13\21\3\21\3\21\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\7"+
		"\23\u0172\n\23\f\23\16\23\u0175\13\23\3\23\3\23\3\24\3\24\3\24\3\24\3"+
		"\24\3\24\3\25\3\25\3\25\3\25\7\25\u0183\n\25\f\25\16\25\u0186\13\25\3"+
		"\25\3\25\3\26\3\26\5\26\u018c\n\26\3\26\3\26\3\26\5\26\u0191\n\26\5\26"+
		"\u0193\n\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31"+
		"\3\31\3\31\3\32\3\32\5\32\u01a5\n\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\7\32\u01af\n\32\f\32\16\32\u01b2\13\32\3\32\3\32\3\33\3\33\3\33"+
		"\3\33\3\34\3\34\3\34\3\34\3\34\3\34\5\34\u01c0\n\34\3\34\3\34\3\34\3\35"+
		"\3\35\3\36\7\36\u01c8\n\36\f\36\16\36\u01cb\13\36\3\36\3\36\3\37\3\37"+
		"\5\37\u01d1\n\37\3 \3 \3 \5 \u01d6\n \3 \5 \u01d9\n \3!\3!\3!\3!\3!\3"+
		"!\7!\u01e1\n!\f!\16!\u01e4\13!\3!\3!\3\"\3\"\3\"\3#\3#\3#\3$\3$\3$\3$"+
		"\3$\5$\u01f3\n$\3%\3%\3%\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3*\3*\7"+
		"*\u0206\n*\f*\16*\u0209\13*\3*\3*\3+\3+\3+\3+\3+\7+\u0212\n+\f+\16+\u0215"+
		"\13+\3+\3+\5+\u0219\n+\3+\3+\3+\3+\3+\3,\3,\3,\3,\3,\5,\u0225\n,\3,\7"+
		",\u0228\n,\f,\16,\u022b\13,\3,\5,\u022e\n,\3,\5,\u0231\n,\3-\3-\3-\3-"+
		"\3-\3-\5-\u0239\n-\3.\3.\3.\5.\u023e\n.\3.\5.\u0241\n.\3.\5.\u0244\n."+
		"\3.\5.\u0247\n.\3.\5.\u024a\n.\3/\3/\5/\u024e\n/\3/\3/\3/\3/\7/\u0254"+
		"\n/\f/\16/\u0257\13/\5/\u0259\n/\3\60\3\60\3\61\3\61\5\61\u025f\n\61\3"+
		"\61\5\61\u0262\n\61\3\61\3\61\5\61\u0266\n\61\3\61\5\61\u0269\n\61\5\61"+
		"\u026b\n\61\3\62\3\62\3\62\3\62\7\62\u0271\n\62\f\62\16\62\u0274\13\62"+
		"\3\63\3\63\3\63\3\63\5\63\u027a\n\63\3\63\3\63\3\63\3\63\3\63\3\63\5\63"+
		"\u0282\n\63\3\63\3\63\3\63\3\63\3\63\3\63\5\63\u028a\n\63\3\63\3\63\3"+
		"\63\3\63\5\63\u0290\n\63\3\63\5\63\u0293\n\63\3\63\3\63\5\63\u0297\n\63"+
		"\3\63\3\63\3\63\3\63\3\63\5\63\u029e\n\63\3\63\3\63\3\63\3\63\5\63\u02a4"+
		"\n\63\7\63\u02a6\n\63\f\63\16\63\u02a9\13\63\3\64\3\64\5\64\u02ad\n\64"+
		"\3\65\3\65\3\65\3\66\3\66\3\66\3\66\3\66\7\66\u02b7\n\66\f\66\16\66\u02ba"+
		"\13\66\3\67\3\67\3\67\38\38\38\38\38\78\u02c4\n8\f8\168\u02c7\138\39\3"+
		"9\59\u02cb\n9\39\39\59\u02cf\n9\59\u02d1\n9\3:\5:\u02d4\n:\3:\3:\3;\3"+
		";\3;\5;\u02db\n;\3;\3;\3;\3<\3<\3<\3<\5<\u02e4\n<\3=\3=\3=\3=\5=\u02ea"+
		"\n=\3>\3>\7>\u02ee\n>\f>\16>\u02f1\13>\3?\3?\3?\3@\5@\u02f7\n@\3@\3@\3"+
		"@\3@\3@\5@\u02fe\n@\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\5A\u030c\nA\3"+
		"B\3B\3C\3C\3C\3C\3C\3C\7C\u0316\nC\fC\16C\u0319\13C\3C\3C\3C\3C\3C\3C"+
		"\3C\3C\3C\3C\3C\3C\6C\u0327\nC\rC\16C\u0328\3C\3C\3C\3C\3C\3C\3C\3C\3"+
		"C\3C\3C\3C\3C\3C\3C\3C\3C\3C\3C\3C\7C\u033f\nC\fC\16C\u0342\13C\3C\3C"+
		"\3C\3C\5C\u0348\nC\5C\u034a\nC\3C\3C\3C\3C\3C\3C\3C\3C\3C\7C\u0355\nC"+
		"\fC\16C\u0358\13C\3D\3D\3D\3D\3D\3E\3E\3F\5F\u0362\nF\3F\3F\3F\5F\u0367"+
		"\nF\3F\5F\u036a\nF\3F\3\u014c\4d\u0084G\2\4\6\b\n\f\16\20\22\24\26\30"+
		"\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080"+
		"\u0082\u0084\u0086\u0088\u008a\2\f\3\2MN\4\2\20\20\60\60\4\2\3\3\65\65"+
		"\4\2IIPP\4\2//\62\62\3\2@A\3\2=>\4\2\32\3299\4\2\30\30<<\5\2::TTWW\u03a2"+
		"\2\u00bc\3\2\2\2\4\u00be\3\2\2\2\6\u00c2\3\2\2\2\b\u00d4\3\2\2\2\n\u00df"+
		"\3\2\2\2\f\u00eb\3\2\2\2\16\u00f1\3\2\2\2\20\u0111\3\2\2\2\22\u011d\3"+
		"\2\2\2\24\u012b\3\2\2\2\26\u012d\3\2\2\2\30\u013e\3\2\2\2\32\u014c\3\2"+
		"\2\2\34\u0151\3\2\2\2\36\u0155\3\2\2\2 \u015e\3\2\2\2\"\u0169\3\2\2\2"+
		"$\u016d\3\2\2\2&\u0178\3\2\2\2(\u017e\3\2\2\2*\u018b\3\2\2\2,\u0194\3"+
		"\2\2\2.\u0198\3\2\2\2\60\u019e\3\2\2\2\62\u01a2\3\2\2\2\64\u01b5\3\2\2"+
		"\2\66\u01b9\3\2\2\28\u01c4\3\2\2\2:\u01c9\3\2\2\2<\u01ce\3\2\2\2>\u01d2"+
		"\3\2\2\2@\u01da\3\2\2\2B\u01e7\3\2\2\2D\u01ea\3\2\2\2F\u01f2\3\2\2\2H"+
		"\u01f4\3\2\2\2J\u01f9\3\2\2\2L\u01fb\3\2\2\2N\u01fd\3\2\2\2P\u01ff\3\2"+
		"\2\2R\u0201\3\2\2\2T\u020c\3\2\2\2V\u0224\3\2\2\2X\u0232\3\2\2\2Z\u023a"+
		"\3\2\2\2\\\u024b\3\2\2\2^\u025a\3\2\2\2`\u026a\3\2\2\2b\u026c\3\2\2\2"+
		"d\u0292\3\2\2\2f\u02aa\3\2\2\2h\u02ae\3\2\2\2j\u02b1\3\2\2\2l\u02bb\3"+
		"\2\2\2n\u02be\3\2\2\2p\u02d0\3\2\2\2r\u02d3\3\2\2\2t\u02d7\3\2\2\2v\u02e3"+
		"\3\2\2\2x\u02e9\3\2\2\2z\u02eb\3\2\2\2|\u02f2\3\2\2\2~\u02f6\3\2\2\2\u0080"+
		"\u030b\3\2\2\2\u0082\u030d\3\2\2\2\u0084\u0349\3\2\2\2\u0086\u0359\3\2"+
		"\2\2\u0088\u035e\3\2\2\2\u008a\u0369\3\2\2\2\u008c\u008d\5\6\4\2\u008d"+
		"\u008e\7\2\2\3\u008e\u00bd\3\2\2\2\u008f\u0090\5\n\6\2\u0090\u0091\7\2"+
		"\2\3\u0091\u00bd\3\2\2\2\u0092\u0093\5\f\7\2\u0093\u0094\7\2\2\3\u0094"+
		"\u00bd\3\2\2\2\u0095\u0096\5\16\b\2\u0096\u0097\7\2\2\3\u0097\u00bd\3"+
		"\2\2\2\u0098\u0099\5\62\32\2\u0099\u009a\7\2\2\3\u009a\u00bd\3\2\2\2\u009b"+
		"\u009c\5.\30\2\u009c\u009d\7\2\2\3\u009d\u00bd\3\2\2\2\u009e\u009f\5,"+
		"\27\2\u009f\u00a0\7\2\2\3\u00a0\u00bd\3\2\2\2\u00a1\u00a2\5\64\33\2\u00a2"+
		"\u00a3\7\2\2\3\u00a3\u00bd\3\2\2\2\u00a4\u00a5\5\60\31\2\u00a5\u00a6\7"+
		"\2\2\3\u00a6\u00bd\3\2\2\2\u00a7\u00a8\5\66\34\2\u00a8\u00a9\7\2\2\3\u00a9"+
		"\u00bd\3\2\2\2\u00aa\u00ab\5\4\3\2\u00ab\u00ac\7\2\2\3\u00ac\u00bd\3\2"+
		"\2\2\u00ad\u00ae\7+\2\2\u00ae\u00b3\5T+\2\u00af\u00b0\7\13\2\2\u00b0\u00b2"+
		"\5T+\2\u00b1\u00af\3\2\2\2\u00b2\u00b5\3\2\2\2\u00b3\u00b1\3\2\2\2\u00b3"+
		"\u00b4\3\2\2\2\u00b4\u00b7\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b6\u00ad\3\2"+
		"\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b9\5V,\2\u00b9\u00ba"+
		"\3\2\2\2\u00ba\u00bb\7\2\2\3\u00bb\u00bd\3\2\2\2\u00bc\u008c\3\2\2\2\u00bc"+
		"\u008f\3\2\2\2\u00bc\u0092\3\2\2\2\u00bc\u0095\3\2\2\2\u00bc\u0098\3\2"+
		"\2\2\u00bc\u009b\3\2\2\2\u00bc\u009e\3\2\2\2\u00bc\u00a1\3\2\2\2\u00bc"+
		"\u00a4\3\2\2\2\u00bc\u00a7\3\2\2\2\u00bc\u00aa\3\2\2\2\u00bc\u00b6\3\2"+
		"\2\2\u00bd\3\3\2\2\2\u00be\u00bf\7\64\2\2\u00bf\u00c0\7#\2\2\u00c0\u00c1"+
		"\5v<\2\u00c1\5\3\2\2\2\u00c2\u00c3\7\27\2\2\u00c3\u00c4\7!\2\2\u00c4\u00d2"+
		"\5v<\2\u00c5\u00c7\7\5\2\2\u00c6\u00c5\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7"+
		"\u00c8\3\2\2\2\u00c8\u00d3\5V,\2\u00c9\u00ca\7\17\2\2\u00ca\u00cf\5\b"+
		"\5\2\u00cb\u00cc\7\13\2\2\u00cc\u00ce\5\b\5\2\u00cd\u00cb\3\2\2\2\u00ce"+
		"\u00d1\3\2\2\2\u00cf\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d3\3\2"+
		"\2\2\u00d1\u00cf\3\2\2\2\u00d2\u00c6\3\2\2\2\u00d2\u00c9\3\2\2\2\u00d3"+
		"\7\3\2\2\2\u00d4\u00d5\7\n\2\2\u00d5\u00da\5\u0084C\2\u00d6\u00d7\7\13"+
		"\2\2\u00d7\u00d9\5\u0084C\2\u00d8\u00d6\3\2\2\2\u00d9\u00dc\3\2\2\2\u00da"+
		"\u00d8\3\2\2\2\u00da\u00db\3\2\2\2\u00db\u00dd\3\2\2\2\u00dc\u00da\3\2"+
		"\2\2\u00dd\u00de\7\66\2\2\u00de\t\3\2\2\2\u00df\u00e0\7\21\2\2\u00e0\u00e1"+
		"\5v<\2\u00e1\u00e4\7\67\2\2\u00e2\u00e5\5x=\2\u00e3\u00e5\5R*\2\u00e4"+
		"\u00e2\3\2\2\2\u00e4\u00e3\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\u00e7\7="+
		"\2\2\u00e7\u00e9\5\u0084C\2\u00e8\u00ea\5h\65\2\u00e9\u00e8\3\2\2\2\u00e9"+
		"\u00ea\3\2\2\2\u00ea\13\3\2\2\2\u00eb\u00ec\7\22\2\2\u00ec\u00ed\7\5\2"+
		"\2\u00ed\u00ef\5v<\2\u00ee\u00f0\5h\65\2\u00ef\u00ee\3\2\2\2\u00ef\u00f0"+
		"\3\2\2\2\u00f0\r\3\2\2\2\u00f1\u00f3\7\31\2\2\u00f2\u00f4\7H\2\2\u00f3"+
		"\u00f2\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f5\3\2\2\2\u00f5\u00f6\7\34"+
		"\2\2\u00f6\u00f7\5v<\2\u00f7\u00f8\7\n\2\2\u00f8\u00fd\5> \2\u00f9\u00fa"+
		"\7\13\2\2\u00fa\u00fc\5> \2\u00fb\u00f9\3\2\2\2\u00fc\u00ff\3\2\2\2\u00fd"+
		"\u00fb\3\2\2\2\u00fd\u00fe\3\2\2\2\u00fe\u0102\3\2\2\2\u00ff\u00fd\3\2"+
		"\2\2\u0100\u0101\7\13\2\2\u0101\u0103\5@!\2\u0102\u0100\3\2\2\2\u0102"+
		"\u0103\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0106\7\66\2\2\u0105\u0107\5"+
		"\22\n\2\u0106\u0105\3\2\2\2\u0106\u0107\3\2\2\2\u0107\u0109\3\2\2\2\u0108"+
		"\u010a\5\20\t\2\u0109\u0108\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010c\3"+
		"\2\2\2\u010b\u010d\5\24\13\2\u010c\u010b\3\2\2\2\u010c\u010d\3\2\2\2\u010d"+
		"\u010e\3\2\2\2\u010e\u010f\5\34\17\2\u010f\u0110\5*\26\2\u0110\17\3\2"+
		"\2\2\u0111\u0112\7V\2\2\u0112\u0113\7\n\2\2\u0113\u0118\7J\2\2\u0114\u0115"+
		"\7\13\2\2\u0115\u0117\7J\2\2\u0116\u0114\3\2\2\2\u0117\u011a\3\2\2\2\u0118"+
		"\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011b\3\2\2\2\u011a\u0118\3\2"+
		"\2\2\u011b\u011c\7\66\2\2\u011c\21\3\2\2\2\u011d\u011e\7U\2\2\u011e\u011f"+
		"\7\n\2\2\u011f\u0124\7J\2\2\u0120\u0121\7\13\2\2\u0121\u0123\7J\2\2\u0122"+
		"\u0120\3\2\2\2\u0123\u0126\3\2\2\2\u0124\u0122\3\2\2\2\u0124\u0125\3\2"+
		"\2\2\u0125\u0127\3\2\2\2\u0126\u0124\3\2\2\2\u0127\u0128\7\66\2\2\u0128"+
		"\23\3\2\2\2\u0129\u012c\7O\2\2\u012a\u012c\5\26\f\2\u012b\u0129\3\2\2"+
		"\2\u012b\u012a\3\2\2\2\u012c\25\3\2\2\2\u012d\u012e\7\7\2\2\u012e\u0133"+
		"\5\30\r\2\u012f\u0130\7\63\2\2\u0130\u0132\5\30\r\2\u0131\u012f\3\2\2"+
		"\2\u0132\u0135\3\2\2\2\u0133\u0131\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0136"+
		"\3\2\2\2\u0135\u0133\3\2\2\2\u0136\u013c\7%\2\2\u0137\u013a\7\13\2\2\u0138"+
		"\u013b\5\"\22\2\u0139\u013b\5&\24\2\u013a\u0138\3\2\2\2\u013a\u0139\3"+
		"\2\2\2\u013b\u013d\3\2\2\2\u013c\u0137\3\2\2\2\u013c\u013d\3\2\2\2\u013d"+
		"\27\3\2\2\2\u013e\u013f\7\7\2\2\u013f\u0144\7J\2\2\u0140\u0141\7\63\2"+
		"\2\u0141\u0143\7J\2\2\u0142\u0140\3\2\2\2\u0143\u0146\3\2\2\2\u0144\u0142"+
		"\3\2\2\2\u0144\u0145\3\2\2\2\u0145\u0147\3\2\2\2\u0146\u0144\3\2\2\2\u0147"+
		"\u0148\7%\2\2\u0148\31\3\2\2\2\u0149\u014b\13\2\2\2\u014a\u0149\3\2\2"+
		"\2\u014b\u014e\3\2\2\2\u014c\u014d\3\2\2\2\u014c\u014a\3\2\2\2\u014d\33"+
		"\3\2\2\2\u014e\u014c\3\2\2\2\u014f\u0152\7Q\2\2\u0150\u0152\5\36\20\2"+
		"\u0151\u014f\3\2\2\2\u0151\u0150\3\2\2\2\u0152\35\3\2\2\2\u0153\u0156"+
		"\7P\2\2\u0154\u0156\5 \21\2\u0155\u0153\3\2\2\2\u0155\u0154\3\2\2\2\u0156"+
		"\u015c\3\2\2\2\u0157\u015a\7\13\2\2\u0158\u015b\5\"\22\2\u0159\u015b\5"+
		"&\24\2\u015a\u0158\3\2\2\2\u015a\u0159\3\2\2\2\u015b\u015d\3\2\2\2\u015c"+
		"\u0157\3\2\2\2\u015c\u015d\3\2\2\2\u015d\37\3\2\2\2\u015e\u015f\7\7\2"+
		"\2\u015f\u0164\7J\2\2\u0160\u0161\7\63\2\2\u0161\u0163\7J\2\2\u0162\u0160"+
		"\3\2\2\2\u0163\u0166\3\2\2\2\u0164\u0162\3\2\2\2\u0164\u0165\3\2\2\2\u0165"+
		"\u0167\3\2\2\2\u0166\u0164\3\2\2\2\u0167\u0168\7%\2\2\u0168!\3\2\2\2\u0169"+
		"\u016a\7R\2\2\u016a\u016b\7\13\2\2\u016b\u016c\5$\23\2\u016c#\3\2\2\2"+
		"\u016d\u016e\7\7\2\2\u016e\u0173\5x=\2\u016f\u0170\7\63\2\2\u0170\u0172"+
		"\5x=\2\u0171\u016f\3\2\2\2\u0172\u0175\3\2\2\2\u0173\u0171\3\2\2\2\u0173"+
		"\u0174\3\2\2\2\u0174\u0176\3\2\2\2\u0175\u0173\3\2\2\2\u0176\u0177\7%"+
		"\2\2\u0177%\3\2\2\2\u0178\u0179\7S\2\2\u0179\u017a\7\13\2\2\u017a\u017b"+
		"\5x=\2\u017b\u017c\7\13\2\2\u017c\u017d\5(\25\2\u017d\'\3\2\2\2\u017e"+
		"\u017f\7\7\2\2\u017f\u0184\5\32\16\2\u0180\u0181\7\63\2\2\u0181\u0183"+
		"\5\32\16\2\u0182\u0180\3\2\2\2\u0183\u0186\3\2\2\2\u0184\u0182\3\2\2\2"+
		"\u0184\u0185\3\2\2\2\u0185\u0187\3\2\2\2\u0186\u0184\3\2\2\2\u0187\u0188"+
		"\7%\2\2\u0188)\3\2\2\2\u0189\u018c\7P\2\2\u018a\u018c\5 \21\2\u018b\u0189"+
		"\3\2\2\2\u018b\u018a\3\2\2\2\u018c\u0192\3\2\2\2\u018d\u0190\7\13\2\2"+
		"\u018e\u0191\5\"\22\2\u018f\u0191\5\32\16\2\u0190\u018e\3\2\2\2\u0190"+
		"\u018f\3\2\2\2\u0191\u0193\3\2\2\2\u0192\u018d\3\2\2\2\u0192\u0193\3\2"+
		"\2\2\u0193+\3\2\2\2\u0194\u0195\7-\2\2\u0195\u0196\7\34\2\2\u0196\u0197"+
		"\5v<\2\u0197-\3\2\2\2\u0198\u0199\7\31\2\2\u0199\u019a\7&\2\2\u019a\u019b"+
		"\5v<\2\u019b\u019c\7\35\2\2\u019c\u019d\5V,\2\u019d/\3\2\2\2\u019e\u019f"+
		"\7-\2\2\u019f\u01a0\7&\2\2\u01a0\u01a1\5v<\2\u01a1\61\3\2\2\2\u01a2\u01a4"+
		"\7\31\2\2\u01a3\u01a5\7L\2\2\u01a4\u01a3\3\2\2\2\u01a4\u01a5\3\2\2\2\u01a5"+
		"\u01a6\3\2\2\2\u01a6\u01a7\7\61\2\2\u01a7\u01a8\5v<\2\u01a8\u01a9\7#\2"+
		"\2\u01a9\u01aa\5v<\2\u01aa\u01ab\7\n\2\2\u01ab\u01b0\5<\37\2\u01ac\u01ad"+
		"\7\13\2\2\u01ad\u01af\5<\37\2\u01ae\u01ac\3\2\2\2\u01af\u01b2\3\2\2\2"+
		"\u01b0\u01ae\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b3\3\2\2\2\u01b2\u01b0"+
		"\3\2\2\2\u01b3\u01b4\7\66\2\2\u01b4\63\3\2\2\2\u01b5\u01b6\7-\2\2\u01b6"+
		"\u01b7\7\61\2\2\u01b7\u01b8\5v<\2\u01b8\65\3\2\2\2\u01b9\u01ba\7\16\2"+
		"\2\u01ba\u01bb\t\2\2\2\u01bb\u01bc\7!\2\2\u01bc\u01bf\5v<\2\u01bd\u01be"+
		"\7\r\2\2\u01be\u01c0\58\35\2\u01bf\u01bd\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0"+
		"\u01c1\3\2\2\2\u01c1\u01c2\7\5\2\2\u01c2\u01c3\5:\36\2\u01c3\67\3\2\2"+
		"\2\u01c4\u01c5\13\2\2\2\u01c59\3\2\2\2\u01c6\u01c8\13\2\2\2\u01c7\u01c6"+
		"\3\2\2\2\u01c8\u01cb\3\2\2\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca"+
		"\u01cc\3\2\2\2\u01cb\u01c9\3\2\2\2\u01cc\u01cd\7\2\2\3\u01cd;\3\2\2\2"+
		"\u01ce\u01d0\5x=\2\u01cf\u01d1\7D\2\2\u01d0\u01cf\3\2\2\2\u01d0\u01d1"+
		"\3\2\2\2\u01d1=\3\2\2\2\u01d2\u01d3\5x=\2\u01d3\u01d5\5F$\2\u01d4\u01d6"+
		"\5B\"\2\u01d5\u01d4\3\2\2\2\u01d5\u01d6\3\2\2\2\u01d6\u01d8\3\2\2\2\u01d7"+
		"\u01d9\5D#\2\u01d8\u01d7\3\2\2\2\u01d8\u01d9\3\2\2\2\u01d9?\3\2\2\2\u01da"+
		"\u01db\7\f\2\2\u01db\u01dc\7(\2\2\u01dc\u01dd\7\n\2\2\u01dd\u01e2\5x="+
		"\2\u01de\u01df\7\13\2\2\u01df\u01e1\5x=\2\u01e0\u01de\3\2\2\2\u01e1\u01e4"+
		"\3\2\2\2\u01e2\u01e0\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e5\3\2\2\2\u01e4"+
		"\u01e2\3\2\2\2\u01e5\u01e6\7\66\2\2\u01e6A\3\2\2\2\u01e7\u01e8\7B\2\2"+
		"\u01e8\u01e9\7C\2\2\u01e9C\3\2\2\2\u01ea\u01eb\7\f\2\2\u01eb\u01ec\7("+
		"\2\2\u01ecE\3\2\2\2\u01ed\u01f3\5H%\2\u01ee\u01f3\5J&\2\u01ef\u01f3\5"+
		"L\'\2\u01f0\u01f3\5N(\2\u01f1\u01f3\5P)\2\u01f2\u01ed\3\2\2\2\u01f2\u01ee"+
		"\3\2\2\2\u01f2\u01ef\3\2\2\2\u01f2\u01f0\3\2\2\2\u01f2\u01f1\3\2\2\2\u01f3"+
		"G\3\2\2\2\u01f4\u01f5\t\3\2\2\u01f5\u01f6\7\n\2\2\u01f6\u01f7\7J\2\2\u01f7"+
		"\u01f8\7\66\2\2\u01f8I\3\2\2\2\u01f9\u01fa\7\4\2\2\u01faK\3\2\2\2\u01fb"+
		"\u01fc\7\23\2\2\u01fcM\3\2\2\2\u01fd\u01fe\7T\2\2\u01feO\3\2\2\2\u01ff"+
		"\u0200\t\4\2\2\u0200Q\3\2\2\2\u0201\u0202\7\n\2\2\u0202\u0207\5x=\2\u0203"+
		"\u0204\7\13\2\2\u0204\u0206\5x=\2\u0205\u0203\3\2\2\2\u0206\u0209\3\2"+
		"\2\2\u0207\u0205\3\2\2\2\u0207\u0208\3\2\2\2\u0208\u020a\3\2\2\2\u0209"+
		"\u0207\3\2\2\2\u020a\u020b\7\66\2\2\u020bS\3\2\2\2\u020c\u0218\7W\2\2"+
		"\u020d\u020e\7\n\2\2\u020e\u0213\5x=\2\u020f\u0210\7\13\2\2\u0210\u0212"+
		"\5x=\2\u0211\u020f\3\2\2\2\u0212\u0215\3\2\2\2\u0213\u0211\3\2\2\2\u0213"+
		"\u0214\3\2\2\2\u0214\u0216\3\2\2\2\u0215\u0213\3\2\2\2\u0216\u0217\7\66"+
		"\2\2\u0217\u0219\3\2\2\2\u0218\u020d\3\2\2\2\u0218\u0219\3\2\2\2\u0219"+
		"\u021a\3\2\2\2\u021a\u021b\7\35\2\2\u021b\u021c\7\n\2\2\u021c\u021d\5"+
		"V,\2\u021d\u021e\7\66\2\2\u021eU\3\2\2\2\u021f\u0225\5Z.\2\u0220\u0221"+
		"\7\n\2\2\u0221\u0222\5V,\2\u0222\u0223\7\66\2\2\u0223\u0225\3\2\2\2\u0224"+
		"\u021f\3\2\2\2\u0224\u0220\3\2\2\2\u0225\u0229\3\2\2\2\u0226\u0228\5X"+
		"-\2\u0227\u0226\3\2\2\2\u0228\u022b\3\2\2\2\u0229\u0227\3\2\2\2\u0229"+
		"\u022a\3\2\2\2\u022a\u022d\3\2\2\2\u022b\u0229\3\2\2\2\u022c\u022e\5n"+
		"8\2\u022d\u022c\3\2\2\2\u022d\u022e\3\2\2\2\u022e\u0230\3\2\2\2\u022f"+
		"\u0231\5t;\2\u0230\u022f\3\2\2\2\u0230\u0231\3\2\2\2\u0231W\3\2\2\2\u0232"+
		"\u0238\7G\2\2\u0233\u0239\5Z.\2\u0234\u0235\7\n\2\2\u0235\u0236\5V,\2"+
		"\u0236\u0237\7\66\2\2\u0237\u0239\3\2\2\2\u0238\u0233\3\2\2\2\u0238\u0234"+
		"\3\2\2\2\u0239Y\3\2\2\2\u023a\u023b\5\\/\2\u023b\u023d\5b\62\2\u023c\u023e"+
		"\5h\65\2\u023d\u023c\3\2\2\2\u023d\u023e\3\2\2\2\u023e\u0240\3\2\2\2\u023f"+
		"\u0241\5j\66\2\u0240\u023f\3\2\2\2\u0240\u0241\3\2\2\2\u0241\u0243\3\2"+
		"\2\2\u0242\u0244\5l\67\2\u0243\u0242\3\2\2\2\u0243\u0244\3\2\2\2\u0244"+
		"\u0246\3\2\2\2\u0245\u0247\5n8\2\u0246\u0245\3\2\2\2\u0246\u0247\3\2\2"+
		"\2\u0247\u0249\3\2\2\2\u0248\u024a\5t;\2\u0249\u0248\3\2\2\2\u0249\u024a"+
		"\3\2\2\2\u024a[\3\2\2\2\u024b\u024d\7*\2\2\u024c\u024e\5^\60\2\u024d\u024c"+
		"\3\2\2\2\u024d\u024e\3\2\2\2\u024e\u0258\3\2\2\2\u024f\u0259\79\2\2\u0250"+
		"\u0255\5`\61\2\u0251\u0252\7\13\2\2\u0252\u0254\5`\61\2\u0253\u0251\3"+
		"\2\2\2\u0254\u0257\3\2\2\2\u0255\u0253\3\2\2\2\u0255\u0256\3\2\2\2\u0256"+
		"\u0259\3\2\2\2\u0257\u0255\3\2\2\2\u0258\u024f\3\2\2\2\u0258\u0250\3\2"+
		"\2\2\u0259]\3\2\2\2\u025a\u025b\t\5\2\2\u025b_\3\2\2\2\u025c\u025e\5x"+
		"=\2\u025d\u025f\7\35\2\2\u025e\u025d\3\2\2\2\u025e\u025f\3\2\2\2\u025f"+
		"\u0261\3\2\2\2\u0260\u0262\7W\2\2\u0261\u0260\3\2\2\2\u0261\u0262\3\2"+
		"\2\2\u0262\u026b\3\2\2\2\u0263\u0265\5\u0084C\2\u0264\u0266\7\35\2\2\u0265"+
		"\u0264\3\2\2\2\u0265\u0266\3\2\2\2\u0266\u0268\3\2\2\2\u0267\u0269\7W"+
		"\2\2\u0268\u0267\3\2\2\2\u0268\u0269\3\2\2\2\u0269\u026b\3\2\2\2\u026a"+
		"\u025c\3\2\2\2\u026a\u0263\3\2\2\2\u026ba\3\2\2\2\u026c\u026d\7\5\2\2"+
		"\u026d\u0272\5d\63\2\u026e\u026f\7\13\2\2\u026f\u0271\5d\63\2\u0270\u026e"+
		"\3\2\2\2\u0271\u0274\3\2\2\2\u0272\u0270\3\2\2\2\u0272\u0273\3\2\2\2\u0273"+
		"c\3\2\2\2\u0274\u0272\3\2\2\2\u0275\u0276\b\63\1\2\u0276\u0277\7\n\2\2"+
		"\u0277\u0279\5d\63\2\u0278\u027a\7E\2\2\u0279\u0278\3\2\2\2\u0279\u027a"+
		"\3\2\2\2\u027a\u027b\3\2\2\2\u027b\u027c\7$\2\2\u027c\u027d\5d\63\2\u027d"+
		"\u027e\7#\2\2\u027e\u027f\5z>\2\u027f\u0281\7\66\2\2\u0280\u0282\5r:\2"+
		"\u0281\u0280\3\2\2\2\u0281\u0282\3\2\2\2\u0282\u0293\3\2\2\2\u0283\u0284"+
		"\7\n\2\2\u0284\u0285\5d\63\2\u0285\u0286\7F\2\2\u0286\u0287\5d\63\2\u0287"+
		"\u0289\7\66\2\2\u0288\u028a\5r:\2\u0289\u0288\3\2\2\2\u0289\u028a\3\2"+
		"\2\2\u028a\u0293\3\2\2\2\u028b\u028c\7\n\2\2\u028c\u028d\5V,\2\u028d\u028f"+
		"\7\66\2\2\u028e\u0290\5r:\2\u028f\u028e\3\2\2\2\u028f\u0290\3\2\2\2\u0290"+
		"\u0293\3\2\2\2\u0291\u0293\5f\64\2\u0292\u0275\3\2\2\2\u0292\u0283\3\2"+
		"\2\2\u0292\u028b\3\2\2\2\u0292\u0291\3\2\2\2\u0293\u02a7\3\2\2\2\u0294"+
		"\u0296\f\b\2\2\u0295\u0297\7E\2\2\u0296\u0295\3\2\2\2\u0296\u0297\3\2"+
		"\2\2\u0297\u0298\3\2\2\2\u0298\u0299\7$\2\2\u0299\u029a\5d\63\2\u029a"+
		"\u029b\7#\2\2\u029b\u029d\5z>\2\u029c\u029e\5r:\2\u029d\u029c\3\2\2\2"+
		"\u029d\u029e\3\2\2\2\u029e\u02a6\3\2\2\2\u029f\u02a0\f\7\2\2\u02a0\u02a1"+
		"\7F\2\2\u02a1\u02a3\5d\63\2\u02a2\u02a4\5r:\2\u02a3\u02a2\3\2\2\2\u02a3"+
		"\u02a4\3\2\2\2\u02a4\u02a6\3\2\2\2\u02a5\u0294\3\2\2\2\u02a5\u029f\3\2"+
		"\2\2\u02a6\u02a9\3\2\2\2\u02a7\u02a5\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8"+
		"e\3\2\2\2\u02a9\u02a7\3\2\2\2\u02aa\u02ac\5v<\2\u02ab\u02ad\5r:\2\u02ac"+
		"\u02ab\3\2\2\2\u02ac\u02ad\3\2\2\2\u02adg\3\2\2\2\u02ae\u02af\7 \2\2\u02af"+
		"\u02b0\5z>\2\u02b0i\3\2\2\2\u02b1\u02b2\7\b\2\2\u02b2\u02b3\7\36\2\2\u02b3"+
		"\u02b8\5x=\2\u02b4\u02b5\7\13\2\2\u02b5\u02b7\5x=\2\u02b6\u02b4\3\2\2"+
		"\2\u02b7\u02ba\3\2\2\2\u02b8\u02b6\3\2\2\2\u02b8\u02b9\3\2\2\2\u02b9k"+
		"\3\2\2\2\u02ba\u02b8\3\2\2\2\u02bb\u02bc\7\26\2\2\u02bc\u02bd\5z>\2\u02bd"+
		"m\3\2\2\2\u02be\u02bf\7)\2\2\u02bf\u02c0\7\36\2\2\u02c0\u02c5\5p9\2\u02c1"+
		"\u02c2\7\13\2\2\u02c2\u02c4\5p9\2\u02c3\u02c1\3\2\2\2\u02c4\u02c7\3\2"+
		"\2\2\u02c5\u02c3\3\2\2\2\u02c5\u02c6\3\2\2\2\u02c6o\3\2\2\2\u02c7\u02c5"+
		"\3\2\2\2\u02c8\u02ca\7J\2\2\u02c9\u02cb\7D\2\2\u02ca\u02c9\3\2\2\2\u02ca"+
		"\u02cb\3\2\2\2\u02cb\u02d1\3\2\2\2\u02cc\u02ce\5x=\2\u02cd\u02cf\7D\2"+
		"\2\u02ce\u02cd\3\2\2\2\u02ce\u02cf\3\2\2\2\u02cf\u02d1\3\2\2\2\u02d0\u02c8"+
		"\3\2\2\2\u02d0\u02cc\3\2\2\2\u02d1q\3\2\2\2\u02d2\u02d4\7\35\2\2\u02d3"+
		"\u02d2\3\2\2\2\u02d3\u02d4\3\2\2\2\u02d4\u02d5\3\2\2\2\u02d5\u02d6\7W"+
		"\2\2\u02d6s\3\2\2\2\u02d7\u02d8\7\25\2\2\u02d8\u02da\7\24\2\2\u02d9\u02db"+
		"\7J\2\2\u02da\u02d9\3\2\2\2\u02da\u02db\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc"+
		"\u02dd\t\6\2\2\u02dd\u02de\7\33\2\2\u02deu\3\2\2\2\u02df\u02e4\7W\2\2"+
		"\u02e0\u02e1\7W\2\2\u02e1\u02e2\7,\2\2\u02e2\u02e4\7W\2\2\u02e3\u02df"+
		"\3\2\2\2\u02e3\u02e0\3\2\2\2\u02e4w\3\2\2\2\u02e5\u02ea\7W\2\2\u02e6\u02e7"+
		"\7W\2\2\u02e7\u02e8\7,\2\2\u02e8\u02ea\7W\2\2\u02e9\u02e5\3\2\2\2\u02e9"+
		"\u02e6\3\2\2\2\u02eay\3\2\2\2\u02eb\u02ef\5~@\2\u02ec\u02ee\5|?\2\u02ed"+
		"\u02ec\3\2\2\2\u02ee\u02f1\3\2\2\2\u02ef\u02ed\3\2\2\2\u02ef\u02f0\3\2"+
		"\2\2\u02f0{\3\2\2\2\u02f1\u02ef\3\2\2\2\u02f2\u02f3\t\7\2\2\u02f3\u02f4"+
		"\5~@\2\u02f4}\3\2\2\2\u02f5\u02f7\7B\2\2\u02f6\u02f5\3\2\2\2\u02f6\u02f7"+
		"\3\2\2\2\u02f7\u02fd\3\2\2\2\u02f8\u02fe\5\u0080A\2\u02f9\u02fa\7\n\2"+
		"\2\u02fa\u02fb\5z>\2\u02fb\u02fc\7\66\2\2\u02fc\u02fe\3\2\2\2\u02fd\u02f8"+
		"\3\2\2\2\u02fd\u02f9\3\2\2\2\u02fe\177\3\2\2\2\u02ff\u0300\5\u0084C\2"+
		"\u0300\u0301\5\u0082B\2\u0301\u0302\5\u0084C\2\u0302\u030c\3\2\2\2\u0303"+
		"\u0304\5\u0084C\2\u0304\u0305\7?\2\2\u0305\u030c\3\2\2\2\u0306\u0307\7"+
		"\6\2\2\u0307\u0308\7\n\2\2\u0308\u0309\5Z.\2\u0309\u030a\7\66\2\2\u030a"+
		"\u030c\3\2\2\2\u030b\u02ff\3\2\2\2\u030b\u0303\3\2\2\2\u030b\u0306\3\2"+
		"\2\2\u030c\u0081\3\2\2\2\u030d\u030e\t\b\2\2\u030e\u0083\3\2\2\2\u030f"+
		"\u0310\bC\1\2\u0310\u0311\5\u0088E\2\u0311\u0312\7\n\2\2\u0312\u0317\5"+
		"\u0084C\2\u0313\u0314\7\13\2\2\u0314\u0316\5\u0084C\2\u0315\u0313\3\2"+
		"\2\2\u0316\u0319\3\2\2\2\u0317\u0315\3\2\2\2\u0317\u0318\3\2\2\2\u0318"+
		"\u031a\3\2\2\2\u0319\u0317\3\2\2\2\u031a\u031b\7\66\2\2\u031b\u034a\3"+
		"\2\2\2\u031c\u031d\7:\2\2\u031d\u031e\7\n\2\2\u031e\u031f\7I\2\2\u031f"+
		"\u0320\5\u0084C\2\u0320\u0321\7\66\2\2\u0321\u034a\3\2\2\2\u0322\u0323"+
		"\7\n\2\2\u0323\u0326\5\u0084C\2\u0324\u0325\7\13\2\2\u0325\u0327\5\u0084"+
		"C\2\u0326\u0324\3\2\2\2\u0327\u0328\3\2\2\2\u0328\u0326\3\2\2\2\u0328"+
		"\u0329\3\2\2\2\u0329\u032a\3\2\2\2\u032a\u032b\7\66\2\2\u032b\u034a\3"+
		"\2\2\2\u032c\u032d\7:\2\2\u032d\u032e\7\n\2\2\u032e\u032f\79\2\2\u032f"+
		"\u034a\7\66\2\2\u0330\u034a\5\u008aF\2\u0331\u034a\5x=\2\u0332\u0333\7"+
		"\n\2\2\u0333\u0334\5Z.\2\u0334\u0335\7\66\2\2\u0335\u034a\3\2\2\2\u0336"+
		"\u0337\7\n\2\2\u0337\u0338\5\u0084C\2\u0338\u0339\7\66\2\2\u0339\u034a"+
		"\3\2\2\2\u033a\u034a\7C\2\2\u033b\u033c\7\t\2\2\u033c\u0340\5\u0086D\2"+
		"\u033d\u033f\5\u0086D\2\u033e\u033d\3\2\2\2\u033f\u0342\3\2\2\2\u0340"+
		"\u033e\3\2\2\2\u0340\u0341\3\2\2\2\u0341\u0343\3\2\2\2\u0342\u0340\3\2"+
		"\2\2\u0343\u0344\7\37\2\2\u0344\u0345\5\u0084C\2\u0345\u0347\7\"\2\2\u0346"+
		"\u0348\7\t\2\2\u0347\u0346\3\2\2\2\u0347\u0348\3\2\2\2\u0348\u034a\3\2"+
		"\2\2\u0349\u030f\3\2\2\2\u0349\u031c\3\2\2\2\u0349\u0322\3\2\2\2\u0349"+
		"\u032c\3\2\2\2\u0349\u0330\3\2\2\2\u0349\u0331\3\2\2\2\u0349\u0332\3\2"+
		"\2\2\u0349\u0336\3\2\2\2\u0349\u033a\3\2\2\2\u0349\u033b\3\2\2\2\u034a"+
		"\u0356\3\2\2\2\u034b\u034c\f\17\2\2\u034c\u034d\t\t\2\2\u034d\u0355\5"+
		"\u0084C\20\u034e\u034f\f\16\2\2\u034f\u0350\t\n\2\2\u0350\u0355\5\u0084"+
		"C\17\u0351\u0352\f\r\2\2\u0352\u0353\7;\2\2\u0353\u0355\5\u0084C\16\u0354"+
		"\u034b\3\2\2\2\u0354\u034e\3\2\2\2\u0354\u0351\3\2\2\2\u0355\u0358\3\2"+
		"\2\2\u0356\u0354\3\2\2\2\u0356\u0357\3\2\2\2\u0357\u0085\3\2\2\2\u0358"+
		"\u0356\3\2\2\2\u0359\u035a\7.\2\2\u035a\u035b\5z>\2\u035b\u035c\7\'\2"+
		"\2\u035c\u035d\5\u0084C\2\u035d\u0087\3\2\2\2\u035e\u035f\t\13\2\2\u035f"+
		"\u0089\3\2\2\2\u0360\u0362\7<\2\2\u0361\u0360\3\2\2\2\u0361\u0362\3\2"+
		"\2\2\u0362\u0363\3\2\2\2\u0363\u0366\7J\2\2\u0364\u0365\7,\2\2\u0365\u0367"+
		"\7J\2\2\u0366\u0364\3\2\2\2\u0366\u0367\3\2\2\2\u0367\u036a\3\2\2\2\u0368"+
		"\u036a\78\2\2\u0369\u0361\3\2\2\2\u0369\u0368\3\2\2\2\u036a\u008b\3\2"+
		"\2\2e\u00b3\u00b6\u00bc\u00c6\u00cf\u00d2\u00da\u00e4\u00e9\u00ef\u00f3"+
		"\u00fd\u0102\u0106\u0109\u010c\u0118\u0124\u012b\u0133\u013a\u013c\u0144"+
		"\u014c\u0151\u0155\u015a\u015c\u0164\u0173\u0184\u018b\u0190\u0192\u01a4"+
		"\u01b0\u01bf\u01c9\u01d0\u01d5\u01d8\u01e2\u01f2\u0207\u0213\u0218\u0224"+
		"\u0229\u022d\u0230\u0238\u023d\u0240\u0243\u0246\u0249\u024d\u0255\u0258"+
		"\u025e\u0261\u0265\u0268\u026a\u0272\u0279\u0281\u0289\u028f\u0292\u0296"+
		"\u029d\u02a3\u02a5\u02a7\u02ac\u02b8\u02c5\u02ca\u02ce\u02d0\u02d3\u02da"+
		"\u02e3\u02e9\u02ef\u02f6\u02fd\u030b\u0317\u0328\u0340\u0347\u0349\u0354"+
		"\u0356\u0361\u0366\u0369";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}