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
		T__47=1, T__46=2, T__45=3, T__44=4, T__43=5, T__42=6, T__41=7, T__40=8, 
		T__39=9, T__38=10, T__37=11, T__36=12, T__35=13, T__34=14, T__33=15, T__32=16, 
		T__31=17, T__30=18, T__29=19, T__28=20, T__27=21, T__26=22, T__25=23, 
		T__24=24, T__23=25, T__22=26, T__21=27, T__20=28, T__19=29, T__18=30, 
		T__17=31, T__16=32, T__15=33, T__14=34, T__13=35, T__12=36, T__11=37, 
		T__10=38, T__9=39, T__8=40, T__7=41, T__6=42, T__5=43, T__4=44, T__3=45, 
		T__2=46, T__1=47, T__0=48, STRING=49, STAR=50, COUNT=51, CONCAT=52, NEGATIVE=53, 
		EQUALS=54, OPERATOR=55, NULLOPERATOR=56, AND=57, OR=58, NOT=59, NULL=60, 
		DIRECTION=61, JOINTYPE=62, CROSSJOIN=63, TABLECOMBINATION=64, DISTINCT=65, 
		INTEGER=66, WS=67, UNIQUE=68, REPLACE=69, RESUME=70, NONE=71, ALL=72, 
		ANYTEXT=73, HASH=74, RANGE=75, DATE=76, IDENTIFIER=77, ANY=78;
	public static final String[] tokenNames = {
		"<INVALID>", "'GROUP'", "'INDEX'", "'BY'", "'EXISTS'", "','", "'DELETE'", 
		"'WHERE'", "'HAVING'", "'VIEW'", "'('", "'AS'", "'{'", "'LOAD'", "'PRIMARY'", 
		"'VALUES'", "'BIGINT'", "'FIRST'", "'}'", "'JOIN'", "'ORDER'", "'INTO'", 
		"'SET'", "'VARCHAR'", "')'", "'.'", "'TABLE'", "'CHAR'", "'+'", "'UPDATE'", 
		"'RUNSTATS'", "'ONLY'", "'KEY'", "'CREATE'", "'FETCH'", "'ROW'", "'ON'", 
		"'ROWS'", "'DOUBLE'", "'DELIMITER'", "'FLOAT'", "'DROP'", "'WITH'", "'FROM'", 
		"'INSERT'", "'/'", "'|'", "'SELECT'", "'INTEGER'", "STRING", "'*'", "'COUNT'", 
		"'||'", "'-'", "'='", "OPERATOR", "NULLOPERATOR", "'AND'", "'OR'", "'NOT'", 
		"'NULL'", "DIRECTION", "JOINTYPE", "'CROSS JOIN'", "TABLECOMBINATION", 
		"'DISTINCT'", "INTEGER", "WS", "'UNIQUE'", "'REPLACE'", "'RESUME'", "'NONE'", 
		"'ALL'", "'ANY'", "'HASH'", "'RANGE'", "'DATE'", "IDENTIFIER", "ANY"
	};
	public static final int
		RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_update = 3, 
		RULE_delete = 4, RULE_createTable = 5, RULE_groupExp = 6, RULE_realGroupExp = 7, 
		RULE_groupDef = 8, RULE_rangeExp = 9, RULE_nodeExp = 10, RULE_realNodeExp = 11, 
		RULE_integerSet = 12, RULE_hashExp = 13, RULE_columnSet = 14, RULE_rangeType = 15, 
		RULE_rangeSet = 16, RULE_deviceExp = 17, RULE_dropTable = 18, RULE_createView = 19, 
		RULE_dropView = 20, RULE_createIndex = 21, RULE_dropIndex = 22, RULE_load = 23, 
		RULE_any = 24, RULE_remainder = 25, RULE_indexDef = 26, RULE_colDef = 27, 
		RULE_primaryKey = 28, RULE_notNull = 29, RULE_primary = 30, RULE_dataType = 31, 
		RULE_char2 = 32, RULE_int2 = 33, RULE_long2 = 34, RULE_date2 = 35, RULE_float2 = 36, 
		RULE_colList = 37, RULE_commonTableExpression = 38, RULE_fullSelect = 39, 
		RULE_connectedSelect = 40, RULE_subSelect = 41, RULE_selectClause = 42, 
		RULE_selecthow = 43, RULE_selectListEntry = 44, RULE_fromClause = 45, 
		RULE_tableReference = 46, RULE_singleTable = 47, RULE_whereClause = 48, 
		RULE_groupBy = 49, RULE_havingClause = 50, RULE_orderBy = 51, RULE_sortKey = 52, 
		RULE_correlationClause = 53, RULE_fetchFirst = 54, RULE_tableName = 55, 
		RULE_columnName = 56, RULE_searchCondition = 57, RULE_connectedSearchClause = 58, 
		RULE_searchClause = 59, RULE_predicate = 60, RULE_operator = 61, RULE_expression = 62, 
		RULE_identifier = 63, RULE_literal = 64;
	public static final String[] ruleNames = {
		"select", "runstats", "insert", "update", "delete", "createTable", "groupExp", 
		"realGroupExp", "groupDef", "rangeExp", "nodeExp", "realNodeExp", "integerSet", 
		"hashExp", "columnSet", "rangeType", "rangeSet", "deviceExp", "dropTable", 
		"createView", "dropView", "createIndex", "dropIndex", "load", "any", "remainder", 
		"indexDef", "colDef", "primaryKey", "notNull", "primary", "dataType", 
		"char2", "int2", "long2", "date2", "float2", "colList", "commonTableExpression", 
		"fullSelect", "connectedSelect", "subSelect", "selectClause", "selecthow", 
		"selectListEntry", "fromClause", "tableReference", "singleTable", "whereClause", 
		"groupBy", "havingClause", "orderBy", "sortKey", "correlationClause", 
		"fetchFirst", "tableName", "columnName", "searchCondition", "connectedSearchClause", 
		"searchClause", "predicate", "operator", "expression", "identifier", "literal"
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
		public CreateTableContext createTable() {
			return getRuleContext(CreateTableContext.class,0);
		}
		public DropIndexContext dropIndex() {
			return getRuleContext(DropIndexContext.class,0);
		}
		public RunstatsContext runstats() {
			return getRuleContext(RunstatsContext.class,0);
		}
		public List<CommonTableExpressionContext> commonTableExpression() {
			return getRuleContexts(CommonTableExpressionContext.class);
		}
		public UpdateContext update() {
			return getRuleContext(UpdateContext.class,0);
		}
		public CreateIndexContext createIndex() {
			return getRuleContext(CreateIndexContext.class,0);
		}
		public CommonTableExpressionContext commonTableExpression(int i) {
			return getRuleContext(CommonTableExpressionContext.class,i);
		}
		public LoadContext load() {
			return getRuleContext(LoadContext.class,0);
		}
		public InsertContext insert() {
			return getRuleContext(InsertContext.class,0);
		}
		public DropTableContext dropTable() {
			return getRuleContext(DropTableContext.class,0);
		}
		public DeleteContext delete() {
			return getRuleContext(DeleteContext.class,0);
		}
		public DropViewContext dropView() {
			return getRuleContext(DropViewContext.class,0);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public CreateViewContext createView() {
			return getRuleContext(CreateViewContext.class,0);
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
			setState(153);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(130); insert();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(131); update();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(132); delete();
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(133); createTable();
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(134); createIndex();
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(135); createView();
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(136); dropTable();
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(137); dropIndex();
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(138); dropView();
				}
				break;

			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(139); load();
				}
				break;

			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(140); runstats();
				}
				break;

			case 12:
				enterOuterAlt(_localctx, 12);
				{
				{
				setState(150);
				_la = _input.LA(1);
				if (_la==42) {
					{
					setState(141); match(42);
					setState(142); commonTableExpression();
					setState(147);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==5) {
						{
						{
						setState(143); match(5);
						setState(144); commonTableExpression();
						}
						}
						setState(149);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(152); fullSelect();
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
			setState(155); match(30);
			setState(156); match(36);
			setState(157); tableName();
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
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
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
			setState(159); match(44);
			setState(160); match(21);
			setState(161); tableName();
			setState(178);
			switch (_input.LA(1)) {
			case 10:
			case 43:
			case 47:
				{
				{
				setState(163);
				_la = _input.LA(1);
				if (_la==43) {
					{
					setState(162); match(43);
					}
				}

				setState(165); fullSelect();
				}
				}
				break;
			case 15:
				{
				{
				setState(166); match(15);
				setState(167); match(10);
				setState(168); expression(0);
				setState(173);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==5) {
					{
					{
					setState(169); match(5);
					setState(170); expression(0);
					}
					}
					setState(175);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(176); match(24);
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
		public ColListContext colList() {
			return getRuleContext(ColListContext.class,0);
		}
		public TerminalNode EQUALS() { return getToken(SelectParser.EQUALS, 0); }
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
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
			setState(180); match(29);
			setState(181); tableName();
			setState(182); match(22);
			setState(185);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(183); columnName();
				}
				break;
			case 10:
				{
				setState(184); colList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(187); match(EQUALS);
			setState(188); expression(0);
			setState(190);
			_la = _input.LA(1);
			if (_la==7) {
				{
				setState(189); whereClause();
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
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
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
			setState(192); match(6);
			setState(193); match(43);
			setState(194); tableName();
			setState(196);
			_la = _input.LA(1);
			if (_la==7) {
				{
				setState(195); whereClause();
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
		public PrimaryKeyContext primaryKey() {
			return getRuleContext(PrimaryKeyContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public DeviceExpContext deviceExp() {
			return getRuleContext(DeviceExpContext.class,0);
		}
		public NodeExpContext nodeExp() {
			return getRuleContext(NodeExpContext.class,0);
		}
		public GroupExpContext groupExp() {
			return getRuleContext(GroupExpContext.class,0);
		}
		public ColDefContext colDef(int i) {
			return getRuleContext(ColDefContext.class,i);
		}
		public List<ColDefContext> colDef() {
			return getRuleContexts(ColDefContext.class);
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
			setState(198); match(33);
			setState(199); match(26);
			setState(200); tableName();
			setState(201); match(10);
			setState(202); colDef();
			setState(207);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(203); match(5);
					setState(204); colDef();
					}
					} 
				}
				setState(209);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			}
			setState(212);
			_la = _input.LA(1);
			if (_la==5) {
				{
				setState(210); match(5);
				setState(211); primaryKey();
				}
			}

			setState(214); match(24);
			setState(216);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				{
				setState(215); groupExp();
				}
				break;
			}
			setState(218); nodeExp();
			setState(219); deviceExp();
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
		enterRule(_localctx, 12, RULE_groupExp);
		try {
			setState(223);
			switch (_input.LA(1)) {
			case NONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(221); match(NONE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 2);
				{
				setState(222); realGroupExp();
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
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public List<GroupDefContext> groupDef() {
			return getRuleContexts(GroupDefContext.class);
		}
		public GroupDefContext groupDef(int i) {
			return getRuleContext(GroupDefContext.class,i);
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
		enterRule(_localctx, 14, RULE_realGroupExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(225); match(12);
			setState(226); groupDef();
			setState(231);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==46) {
				{
				{
				setState(227); match(46);
				setState(228); groupDef();
				}
				}
				setState(233);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(234); match(18);
			setState(240);
			_la = _input.LA(1);
			if (_la==5) {
				{
				setState(235); match(5);
				setState(238);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(236); hashExp();
					}
					break;
				case RANGE:
					{
					setState(237); rangeType();
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
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
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
		enterRule(_localctx, 16, RULE_groupDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(242); match(12);
			setState(243); match(INTEGER);
			setState(248);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==46) {
				{
				{
				setState(244); match(46);
				setState(245); match(INTEGER);
				}
				}
				setState(250);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(251); match(18);
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
		enterRule(_localctx, 18, RULE_rangeExp);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(256);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=1 && _alt!=-1 ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(253);
					matchWildcard();
					}
					} 
				}
				setState(258);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
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
		enterRule(_localctx, 20, RULE_nodeExp);
		try {
			setState(261);
			switch (_input.LA(1)) {
			case ANYTEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(259); match(ANYTEXT);
				}
				break;
			case 12:
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(260); realNodeExp();
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
		public TerminalNode ALL() { return getToken(SelectParser.ALL, 0); }
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public IntegerSetContext integerSet() {
			return getRuleContext(IntegerSetContext.class,0);
		}
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
		enterRule(_localctx, 22, RULE_realNodeExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(265);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(263); match(ALL);
				}
				break;
			case 12:
				{
				setState(264); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(272);
			_la = _input.LA(1);
			if (_la==5) {
				{
				setState(267); match(5);
				setState(270);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(268); hashExp();
					}
					break;
				case RANGE:
					{
					setState(269); rangeType();
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
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
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
		enterRule(_localctx, 24, RULE_integerSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(274); match(12);
			setState(275); match(INTEGER);
			setState(280);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==46) {
				{
				{
				setState(276); match(46);
				setState(277); match(INTEGER);
				}
				}
				setState(282);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(283); match(18);
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
		enterRule(_localctx, 26, RULE_hashExp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(285); match(HASH);
			setState(286); match(5);
			setState(287); columnSet();
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
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
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
		enterRule(_localctx, 28, RULE_columnSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(289); match(12);
			setState(290); columnName();
			setState(295);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==46) {
				{
				{
				setState(291); match(46);
				setState(292); columnName();
				}
				}
				setState(297);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(298); match(18);
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
		enterRule(_localctx, 30, RULE_rangeType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(300); match(RANGE);
			setState(301); match(5);
			setState(302); columnName();
			setState(303); match(5);
			setState(304); rangeSet();
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
		enterRule(_localctx, 32, RULE_rangeSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(306); match(12);
			setState(307); rangeExp();
			setState(312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==46) {
				{
				{
				setState(308); match(46);
				setState(309); rangeExp();
				}
				}
				setState(314);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(315); match(18);
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
		public TerminalNode ALL() { return getToken(SelectParser.ALL, 0); }
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public IntegerSetContext integerSet() {
			return getRuleContext(IntegerSetContext.class,0);
		}
		public RangeExpContext rangeExp() {
			return getRuleContext(RangeExpContext.class,0);
		}
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
		enterRule(_localctx, 34, RULE_deviceExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(319);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(317); match(ALL);
				}
				break;
			case 12:
				{
				setState(318); integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(326);
			_la = _input.LA(1);
			if (_la==5) {
				{
				setState(321); match(5);
				setState(324);
				switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
				case 1:
					{
					setState(322); hashExp();
					}
					break;

				case 2:
					{
					setState(323); rangeExp();
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
		enterRule(_localctx, 36, RULE_dropTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(328); match(41);
			setState(329); match(26);
			setState(330); tableName();
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
		enterRule(_localctx, 38, RULE_createView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(332); match(33);
			setState(333); match(9);
			setState(334); tableName();
			setState(335); match(11);
			setState(336); fullSelect();
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
		enterRule(_localctx, 40, RULE_dropView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(338); match(41);
			setState(339); match(9);
			setState(340); tableName();
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
		public TableNameContext tableName(int i) {
			return getRuleContext(TableNameContext.class,i);
		}
		public List<IndexDefContext> indexDef() {
			return getRuleContexts(IndexDefContext.class);
		}
		public IndexDefContext indexDef(int i) {
			return getRuleContext(IndexDefContext.class,i);
		}
		public List<TableNameContext> tableName() {
			return getRuleContexts(TableNameContext.class);
		}
		public TerminalNode UNIQUE() { return getToken(SelectParser.UNIQUE, 0); }
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
		enterRule(_localctx, 42, RULE_createIndex);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(342); match(33);
			setState(344);
			_la = _input.LA(1);
			if (_la==UNIQUE) {
				{
				setState(343); match(UNIQUE);
				}
			}

			setState(346); match(2);
			setState(347); tableName();
			setState(348); match(36);
			setState(349); tableName();
			setState(350); match(10);
			setState(351); indexDef();
			setState(356);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==5) {
				{
				{
				setState(352); match(5);
				setState(353); indexDef();
				}
				}
				setState(358);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(359); match(24);
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
		enterRule(_localctx, 44, RULE_dropIndex);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(361); match(41);
			setState(362); match(2);
			setState(363); tableName();
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
		public RemainderContext remainder() {
			return getRuleContext(RemainderContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public TerminalNode REPLACE() { return getToken(SelectParser.REPLACE, 0); }
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
		enterRule(_localctx, 46, RULE_load);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(365); match(13);
			setState(366);
			_la = _input.LA(1);
			if ( !(_la==REPLACE || _la==RESUME) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(367); match(21);
			setState(368); tableName();
			setState(371);
			_la = _input.LA(1);
			if (_la==39) {
				{
				setState(369); match(39);
				setState(370); any();
				}
			}

			setState(373); match(43);
			setState(374); remainder();
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
		enterRule(_localctx, 48, RULE_any);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(376);
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
		enterRule(_localctx, 50, RULE_remainder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(381);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1) | (1L << 2) | (1L << 3) | (1L << 4) | (1L << 5) | (1L << 6) | (1L << 7) | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 11) | (1L << 12) | (1L << 13) | (1L << 14) | (1L << 15) | (1L << 16) | (1L << 17) | (1L << 18) | (1L << 19) | (1L << 20) | (1L << 21) | (1L << 22) | (1L << 23) | (1L << 24) | (1L << 25) | (1L << 26) | (1L << 27) | (1L << 28) | (1L << 29) | (1L << 30) | (1L << 31) | (1L << 32) | (1L << 33) | (1L << 34) | (1L << 35) | (1L << 36) | (1L << 37) | (1L << 38) | (1L << 39) | (1L << 40) | (1L << 41) | (1L << 42) | (1L << 43) | (1L << 44) | (1L << 45) | (1L << 46) | (1L << 47) | (1L << 48) | (1L << STRING) | (1L << STAR) | (1L << COUNT) | (1L << CONCAT) | (1L << NEGATIVE) | (1L << EQUALS) | (1L << OPERATOR) | (1L << NULLOPERATOR) | (1L << AND) | (1L << OR) | (1L << NOT) | (1L << NULL) | (1L << DIRECTION) | (1L << JOINTYPE) | (1L << CROSSJOIN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (TABLECOMBINATION - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0)) {
				{
				{
				setState(378);
				matchWildcard();
				}
				}
				setState(383);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(384); match(EOF);
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
		enterRule(_localctx, 52, RULE_indexDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(386); columnName();
			setState(388);
			_la = _input.LA(1);
			if (_la==DIRECTION) {
				{
				setState(387); match(DIRECTION);
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
		public PrimaryContext primary() {
			return getRuleContext(PrimaryContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public NotNullContext notNull() {
			return getRuleContext(NotNullContext.class,0);
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
		enterRule(_localctx, 54, RULE_colDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(390); columnName();
			setState(391); dataType();
			setState(393);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(392); notNull();
				}
			}

			setState(396);
			_la = _input.LA(1);
			if (_la==14) {
				{
				setState(395); primary();
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
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
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
		enterRule(_localctx, 56, RULE_primaryKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(398); match(14);
			setState(399); match(32);
			setState(400); match(10);
			setState(401); columnName();
			setState(406);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==5) {
				{
				{
				setState(402); match(5);
				setState(403); columnName();
				}
				}
				setState(408);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(409); match(24);
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
		enterRule(_localctx, 58, RULE_notNull);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(411); match(NOT);
			setState(412); match(NULL);
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
		enterRule(_localctx, 60, RULE_primary);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(414); match(14);
			setState(415); match(32);
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
		public Long2Context long2() {
			return getRuleContext(Long2Context.class,0);
		}
		public Int2Context int2() {
			return getRuleContext(Int2Context.class,0);
		}
		public Date2Context date2() {
			return getRuleContext(Date2Context.class,0);
		}
		public Float2Context float2() {
			return getRuleContext(Float2Context.class,0);
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
		enterRule(_localctx, 62, RULE_dataType);
		try {
			setState(422);
			switch (_input.LA(1)) {
			case 23:
			case 27:
				enterOuterAlt(_localctx, 1);
				{
				setState(417); char2();
				}
				break;
			case 48:
				enterOuterAlt(_localctx, 2);
				{
				setState(418); int2();
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 3);
				{
				setState(419); long2();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 4);
				{
				setState(420); date2();
				}
				break;
			case 38:
			case 40:
				enterOuterAlt(_localctx, 5);
				{
				setState(421); float2();
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
		enterRule(_localctx, 64, RULE_char2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(424);
			_la = _input.LA(1);
			if ( !(_la==23 || _la==27) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(425); match(10);
			setState(426); match(INTEGER);
			setState(427); match(24);
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
		enterRule(_localctx, 66, RULE_int2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(429); match(48);
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
		enterRule(_localctx, 68, RULE_long2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(431); match(16);
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
		enterRule(_localctx, 70, RULE_date2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(433); match(DATE);
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
		enterRule(_localctx, 72, RULE_float2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(435);
			_la = _input.LA(1);
			if ( !(_la==38 || _la==40) ) {
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
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
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
		enterRule(_localctx, 74, RULE_colList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(437); match(10);
			setState(438); columnName();
			setState(443);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==5) {
				{
				{
				setState(439); match(5);
				setState(440); columnName();
				}
				}
				setState(445);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(446); match(24);
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
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
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
		enterRule(_localctx, 76, RULE_commonTableExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(448); match(IDENTIFIER);
			setState(460);
			_la = _input.LA(1);
			if (_la==10) {
				{
				setState(449); match(10);
				setState(450); columnName();
				setState(455);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==5) {
					{
					{
					setState(451); match(5);
					setState(452); columnName();
					}
					}
					setState(457);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(458); match(24);
				}
			}

			setState(462); match(11);
			setState(463); match(10);
			setState(464); fullSelect();
			setState(465); match(24);
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
		public OrderByContext orderBy() {
			return getRuleContext(OrderByContext.class,0);
		}
		public FetchFirstContext fetchFirst() {
			return getRuleContext(FetchFirstContext.class,0);
		}
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
		public List<ConnectedSelectContext> connectedSelect() {
			return getRuleContexts(ConnectedSelectContext.class);
		}
		public ConnectedSelectContext connectedSelect(int i) {
			return getRuleContext(ConnectedSelectContext.class,i);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
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
		enterRule(_localctx, 78, RULE_fullSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(472);
			switch (_input.LA(1)) {
			case 47:
				{
				setState(467); subSelect();
				}
				break;
			case 10:
				{
				setState(468); match(10);
				setState(469); fullSelect();
				setState(470); match(24);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(477);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==TABLECOMBINATION) {
				{
				{
				setState(474); connectedSelect();
				}
				}
				setState(479);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(481);
			_la = _input.LA(1);
			if (_la==20) {
				{
				setState(480); orderBy();
				}
			}

			setState(484);
			_la = _input.LA(1);
			if (_la==34) {
				{
				setState(483); fetchFirst();
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
		public TerminalNode TABLECOMBINATION() { return getToken(SelectParser.TABLECOMBINATION, 0); }
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
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
		enterRule(_localctx, 80, RULE_connectedSelect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(486); match(TABLECOMBINATION);
			setState(492);
			switch (_input.LA(1)) {
			case 47:
				{
				setState(487); subSelect();
				}
				break;
			case 10:
				{
				setState(488); match(10);
				setState(489); fullSelect();
				setState(490); match(24);
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
		public OrderByContext orderBy() {
			return getRuleContext(OrderByContext.class,0);
		}
		public FetchFirstContext fetchFirst() {
			return getRuleContext(FetchFirstContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
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
		enterRule(_localctx, 82, RULE_subSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(494); selectClause();
			setState(495); fromClause();
			setState(497);
			_la = _input.LA(1);
			if (_la==7) {
				{
				setState(496); whereClause();
				}
			}

			setState(500);
			_la = _input.LA(1);
			if (_la==1) {
				{
				setState(499); groupBy();
				}
			}

			setState(503);
			_la = _input.LA(1);
			if (_la==8) {
				{
				setState(502); havingClause();
				}
			}

			setState(506);
			switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
			case 1:
				{
				setState(505); orderBy();
				}
				break;
			}
			setState(509);
			switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
			case 1:
				{
				setState(508); fetchFirst();
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
		public List<SelectListEntryContext> selectListEntry() {
			return getRuleContexts(SelectListEntryContext.class);
		}
		public SelectListEntryContext selectListEntry(int i) {
			return getRuleContext(SelectListEntryContext.class,i);
		}
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
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
		enterRule(_localctx, 84, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(511); match(47);
			setState(513);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==ALL) {
				{
				setState(512); selecthow();
				}
			}

			setState(524);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(515); match(STAR);
				}
				break;
			case 10:
			case STRING:
			case COUNT:
			case NEGATIVE:
			case NULL:
			case INTEGER:
			case DATE:
			case IDENTIFIER:
				{
				{
				setState(516); selectListEntry();
				setState(521);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==5) {
					{
					{
					setState(517); match(5);
					setState(518); selectListEntry();
					}
					}
					setState(523);
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
		public TerminalNode ALL() { return getToken(SelectParser.ALL, 0); }
		public TerminalNode DISTINCT() { return getToken(SelectParser.DISTINCT, 0); }
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
		enterRule(_localctx, 86, RULE_selecthow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(526);
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
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
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
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
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
		enterRule(_localctx, 88, RULE_selectListEntry);
		int _la;
		try {
			setState(542);
			switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
			case 1:
				_localctx = new SelectColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(528); columnName();
				setState(530);
				_la = _input.LA(1);
				if (_la==11) {
					{
					setState(529); match(11);
					}
				}

				setState(533);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(532); match(IDENTIFIER);
					}
				}

				}
				break;

			case 2:
				_localctx = new SelectExpressionContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(535); expression(0);
				setState(537);
				_la = _input.LA(1);
				if (_la==11) {
					{
					setState(536); match(11);
					}
				}

				setState(540);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(539); match(IDENTIFIER);
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
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
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
		enterRule(_localctx, 90, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(544); match(43);
			setState(545); tableReference(0);
			setState(550);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==5) {
				{
				{
				setState(546); match(5);
				setState(547); tableReference(0);
				}
				}
				setState(552);
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
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public TerminalNode JOINTYPE() { return getToken(SelectParser.JOINTYPE, 0); }
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
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
	public static class CrossJoinContext extends TableReferenceContext {
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public TerminalNode CROSSJOIN() { return getToken(SelectParser.CROSSJOIN, 0); }
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
	public static class JoinContext extends TableReferenceContext {
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public TerminalNode JOINTYPE() { return getToken(SelectParser.JOINTYPE, 0); }
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
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
	public static class CrossJoinPContext extends TableReferenceContext {
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public TerminalNode CROSSJOIN() { return getToken(SelectParser.CROSSJOIN, 0); }
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

	public final TableReferenceContext tableReference() throws RecognitionException {
		return tableReference(0);
	}

	private TableReferenceContext tableReference(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TableReferenceContext _localctx = new TableReferenceContext(_ctx, _parentState);
		TableReferenceContext _prevctx = _localctx;
		int _startState = 92;
		enterRecursionRule(_localctx, 92, RULE_tableReference, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(582);
			switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
			case 1:
				{
				_localctx = new JoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(554); match(10);
				setState(555); tableReference(0);
				setState(557);
				_la = _input.LA(1);
				if (_la==JOINTYPE) {
					{
					setState(556); match(JOINTYPE);
					}
				}

				setState(559); match(19);
				setState(560); tableReference(0);
				setState(561); match(36);
				setState(562); searchCondition();
				setState(563); match(24);
				setState(565);
				switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
				case 1:
					{
					setState(564); correlationClause();
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
				setState(567); match(10);
				setState(568); tableReference(0);
				setState(569); match(CROSSJOIN);
				setState(570); tableReference(0);
				setState(571); match(24);
				setState(573);
				switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
				case 1:
					{
					setState(572); correlationClause();
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
				setState(575); match(10);
				setState(576); fullSelect();
				setState(577); match(24);
				setState(579);
				switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
				case 1:
					{
					setState(578); correlationClause();
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
				setState(581); singleTable();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(603);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,68,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(601);
					switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
					case 1:
						{
						_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(584);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(586);
						_la = _input.LA(1);
						if (_la==JOINTYPE) {
							{
							setState(585); match(JOINTYPE);
							}
						}

						setState(588); match(19);
						setState(589); tableReference(0);
						setState(590); match(36);
						setState(591); searchCondition();
						setState(593);
						switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
						case 1:
							{
							setState(592); correlationClause();
							}
							break;
						}
						}
						break;

					case 2:
						{
						_localctx = new CrossJoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(595);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(596); match(CROSSJOIN);
						setState(597); tableReference(0);
						setState(599);
						switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
						case 1:
							{
							setState(598); correlationClause();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(605);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,68,_ctx);
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
		enterRule(_localctx, 94, RULE_singleTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(606); tableName();
			setState(608);
			switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
			case 1:
				{
				setState(607); correlationClause();
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
		enterRule(_localctx, 96, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(610); match(7);
			setState(611); searchCondition();
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
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
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
		enterRule(_localctx, 98, RULE_groupBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(613); match(1);
			setState(614); match(3);
			setState(615); columnName();
			setState(620);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==5) {
				{
				{
				setState(616); match(5);
				setState(617); columnName();
				}
				}
				setState(622);
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
		enterRule(_localctx, 100, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(623); match(8);
			setState(624); searchCondition();
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
		enterRule(_localctx, 102, RULE_orderBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(626); match(20);
			setState(627); match(3);
			setState(628); sortKey();
			setState(633);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==5) {
				{
				{
				setState(629); match(5);
				setState(630); sortKey();
				}
				}
				setState(635);
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
	public static class SortKeyIntContext extends SortKeyContext {
		public TerminalNode DIRECTION() { return getToken(SelectParser.DIRECTION, 0); }
		public TerminalNode INTEGER() { return getToken(SelectParser.INTEGER, 0); }
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

	public final SortKeyContext sortKey() throws RecognitionException {
		SortKeyContext _localctx = new SortKeyContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_sortKey);
		int _la;
		try {
			setState(644);
			switch (_input.LA(1)) {
			case INTEGER:
				_localctx = new SortKeyIntContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(636); match(INTEGER);
				setState(638);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(637); match(DIRECTION);
					}
				}

				}
				break;
			case IDENTIFIER:
				_localctx = new SortKeyColContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(640); columnName();
				setState(642);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(641); match(DIRECTION);
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
		enterRule(_localctx, 106, RULE_correlationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(647);
			_la = _input.LA(1);
			if (_la==11) {
				{
				setState(646); match(11);
				}
			}

			setState(649); match(IDENTIFIER);
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
		enterRule(_localctx, 108, RULE_fetchFirst);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651); match(34);
			setState(652); match(17);
			setState(654);
			_la = _input.LA(1);
			if (_la==INTEGER) {
				{
				setState(653); match(INTEGER);
				}
			}

			setState(656);
			_la = _input.LA(1);
			if ( !(_la==35 || _la==37) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(657); match(31);
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
		enterRule(_localctx, 110, RULE_tableName);
		try {
			setState(663);
			switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
			case 1:
				_localctx = new Table1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(659); match(IDENTIFIER);
				}
				break;

			case 2:
				_localctx = new Table2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(660); match(IDENTIFIER);
				setState(661); match(25);
				setState(662); match(IDENTIFIER);
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

	public final ColumnNameContext columnName() throws RecognitionException {
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_columnName);
		try {
			setState(669);
			switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
			case 1:
				_localctx = new Col1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(665); match(IDENTIFIER);
				}
				break;

			case 2:
				_localctx = new Col2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(666); match(IDENTIFIER);
				setState(667); match(25);
				setState(668); match(IDENTIFIER);
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
		public ConnectedSearchClauseContext connectedSearchClause(int i) {
			return getRuleContext(ConnectedSearchClauseContext.class,i);
		}
		public SearchClauseContext searchClause() {
			return getRuleContext(SearchClauseContext.class,0);
		}
		public List<ConnectedSearchClauseContext> connectedSearchClause() {
			return getRuleContexts(ConnectedSearchClauseContext.class);
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
		enterRule(_localctx, 114, RULE_searchCondition);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(671); searchClause();
			setState(675);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,79,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					{
					{
					setState(672); connectedSearchClause();
					}
					} 
				}
				setState(677);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,79,_ctx);
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
		public TerminalNode AND() { return getToken(SelectParser.AND, 0); }
		public TerminalNode OR() { return getToken(SelectParser.OR, 0); }
		public SearchClauseContext searchClause() {
			return getRuleContext(SearchClauseContext.class,0);
		}
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
		enterRule(_localctx, 116, RULE_connectedSearchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(678);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OR) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			setState(679); searchClause();
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
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SelectParser.NOT, 0); }
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
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
		enterRule(_localctx, 118, RULE_searchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(682);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(681); match(NOT);
				}
			}

			setState(689);
			switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
			case 1:
				{
				setState(684); predicate();
				}
				break;

			case 2:
				{
				{
				setState(685); match(10);
				setState(686); searchCondition();
				setState(687); match(24);
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
	public static class NormalPredicateContext extends PredicateContext {
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public OperatorContext operator() {
			return getRuleContext(OperatorContext.class,0);
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
		public TerminalNode NULLOPERATOR() { return getToken(SelectParser.NULLOPERATOR, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
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

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_predicate);
		try {
			setState(703);
			switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
			case 1:
				_localctx = new NormalPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(691); expression(0);
				setState(692); operator();
				setState(693); expression(0);
				}
				}
				break;

			case 2:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(695); expression(0);
				setState(696); match(NULLOPERATOR);
				}
				}
				break;

			case 3:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(698); match(4);
				setState(699); match(10);
				setState(700); subSelect();
				setState(701); match(24);
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
		enterRule(_localctx, 122, RULE_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(705);
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
	public static class AddSubContext extends ExpressionContext {
		public Token op;
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public TerminalNode NEGATIVE() { return getToken(SelectParser.NEGATIVE, 0); }
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
	public static class FunctionContext extends ExpressionContext {
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
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

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 124;
		enterRecursionRule(_localctx, 124, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(751);
			switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
			case 1:
				{
				_localctx = new FunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(708); identifier();
				setState(709); match(10);
				setState(710); expression(0);
				setState(715);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==5) {
					{
					{
					setState(711); match(5);
					setState(712); expression(0);
					}
					}
					setState(717);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(718); match(24);
				}
				break;

			case 2:
				{
				_localctx = new CountDistinctContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(720); match(COUNT);
				setState(721); match(10);
				setState(722); match(DISTINCT);
				setState(723); expression(0);
				setState(724); match(24);
				}
				break;

			case 3:
				{
				_localctx = new ListContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(726); match(10);
				setState(727); expression(0);
				setState(730); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(728); match(5);
					setState(729); expression(0);
					}
					}
					setState(732); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==5 );
				setState(734); match(24);
				}
				break;

			case 4:
				{
				_localctx = new CountStarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(736); match(COUNT);
				setState(737); match(10);
				setState(738); match(STAR);
				setState(739); match(24);
				}
				break;

			case 5:
				{
				_localctx = new IsLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(740); literal();
				}
				break;

			case 6:
				{
				_localctx = new ColLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(741); columnName();
				}
				break;

			case 7:
				{
				_localctx = new ExpSelectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(742); match(10);
				setState(743); subSelect();
				setState(744); match(24);
				}
				break;

			case 8:
				{
				_localctx = new PExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(746); match(10);
				setState(747); expression(0);
				setState(748); match(24);
				}
				break;

			case 9:
				{
				_localctx = new NullExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(750); match(NULL);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(764);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
			while ( _alt!=2 && _alt!=-1 ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(762);
					switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
					case 1:
						{
						_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(753);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(754);
						((MulDivContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==45 || _la==STAR) ) {
							((MulDivContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(755); expression(13);
						}
						break;

					case 2:
						{
						_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(756);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(757);
						((AddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==28 || _la==NEGATIVE) ) {
							((AddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						consume();
						setState(758); expression(12);
						}
						break;

					case 3:
						{
						_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(759);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(760); match(CONCAT);
						setState(761); expression(11);
						}
						break;
					}
					} 
				}
				setState(766);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
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
		enterRule(_localctx, 126, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(767);
			_la = _input.LA(1);
			if ( !(((((_la - 51)) & ~0x3f) == 0 && ((1L << (_la - 51)) & ((1L << (COUNT - 51)) | (1L << (DATE - 51)) | (1L << (IDENTIFIER - 51)))) != 0)) ) {
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
	public static class NumericLiteralContext extends LiteralContext {
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public TerminalNode NEGATIVE() { return getToken(SelectParser.NEGATIVE, 0); }
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
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

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_literal);
		int _la;
		try {
			setState(778);
			switch (_input.LA(1)) {
			case NEGATIVE:
			case INTEGER:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(770);
				_la = _input.LA(1);
				if (_la==NEGATIVE) {
					{
					setState(769); match(NEGATIVE);
					}
				}

				setState(772); match(INTEGER);
				setState(775);
				switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
				case 1:
					{
					setState(773); match(25);
					setState(774); match(INTEGER);
					}
					break;
				}
				}
				break;
			case STRING:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(777); match(STRING);
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
		case 46: return tableReference_sempred((TableReferenceContext)_localctx, predIndex);

		case 62: return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2: return precpred(_ctx, 12);

		case 3: return precpred(_ctx, 11);

		case 4: return precpred(_ctx, 10);
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3P\u030f\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\7\2\u0094\n\2\f\2\16\2\u0097\13\2\5\2\u0099\n\2\3"+
		"\2\5\2\u009c\n\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4\u00a6\n\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\7\4\u00ae\n\4\f\4\16\4\u00b1\13\4\3\4\3\4\5\4\u00b5\n"+
		"\4\3\5\3\5\3\5\3\5\3\5\5\5\u00bc\n\5\3\5\3\5\3\5\5\5\u00c1\n\5\3\6\3\6"+
		"\3\6\3\6\5\6\u00c7\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7\u00d0\n\7\f\7\16"+
		"\7\u00d3\13\7\3\7\3\7\5\7\u00d7\n\7\3\7\3\7\5\7\u00db\n\7\3\7\3\7\3\7"+
		"\3\b\3\b\5\b\u00e2\n\b\3\t\3\t\3\t\3\t\7\t\u00e8\n\t\f\t\16\t\u00eb\13"+
		"\t\3\t\3\t\3\t\3\t\5\t\u00f1\n\t\5\t\u00f3\n\t\3\n\3\n\3\n\3\n\7\n\u00f9"+
		"\n\n\f\n\16\n\u00fc\13\n\3\n\3\n\3\13\7\13\u0101\n\13\f\13\16\13\u0104"+
		"\13\13\3\f\3\f\5\f\u0108\n\f\3\r\3\r\5\r\u010c\n\r\3\r\3\r\3\r\5\r\u0111"+
		"\n\r\5\r\u0113\n\r\3\16\3\16\3\16\3\16\7\16\u0119\n\16\f\16\16\16\u011c"+
		"\13\16\3\16\3\16\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\7\20\u0128\n"+
		"\20\f\20\16\20\u012b\13\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21\3\22"+
		"\3\22\3\22\3\22\7\22\u0139\n\22\f\22\16\22\u013c\13\22\3\22\3\22\3\23"+
		"\3\23\5\23\u0142\n\23\3\23\3\23\3\23\5\23\u0147\n\23\5\23\u0149\n\23\3"+
		"\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3"+
		"\27\3\27\5\27\u015b\n\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\7\27"+
		"\u0165\n\27\f\27\16\27\u0168\13\27\3\27\3\27\3\30\3\30\3\30\3\30\3\31"+
		"\3\31\3\31\3\31\3\31\3\31\5\31\u0176\n\31\3\31\3\31\3\31\3\32\3\32\3\33"+
		"\7\33\u017e\n\33\f\33\16\33\u0181\13\33\3\33\3\33\3\34\3\34\5\34\u0187"+
		"\n\34\3\35\3\35\3\35\5\35\u018c\n\35\3\35\5\35\u018f\n\35\3\36\3\36\3"+
		"\36\3\36\3\36\3\36\7\36\u0197\n\36\f\36\16\36\u019a\13\36\3\36\3\36\3"+
		"\37\3\37\3\37\3 \3 \3 \3!\3!\3!\3!\3!\5!\u01a9\n!\3\"\3\"\3\"\3\"\3\""+
		"\3#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3\'\3\'\7\'\u01bc\n\'\f\'\16\'\u01bf"+
		"\13\'\3\'\3\'\3(\3(\3(\3(\3(\7(\u01c8\n(\f(\16(\u01cb\13(\3(\3(\5(\u01cf"+
		"\n(\3(\3(\3(\3(\3(\3)\3)\3)\3)\3)\5)\u01db\n)\3)\7)\u01de\n)\f)\16)\u01e1"+
		"\13)\3)\5)\u01e4\n)\3)\5)\u01e7\n)\3*\3*\3*\3*\3*\3*\5*\u01ef\n*\3+\3"+
		"+\3+\5+\u01f4\n+\3+\5+\u01f7\n+\3+\5+\u01fa\n+\3+\5+\u01fd\n+\3+\5+\u0200"+
		"\n+\3,\3,\5,\u0204\n,\3,\3,\3,\3,\7,\u020a\n,\f,\16,\u020d\13,\5,\u020f"+
		"\n,\3-\3-\3.\3.\5.\u0215\n.\3.\5.\u0218\n.\3.\3.\5.\u021c\n.\3.\5.\u021f"+
		"\n.\5.\u0221\n.\3/\3/\3/\3/\7/\u0227\n/\f/\16/\u022a\13/\3\60\3\60\3\60"+
		"\3\60\5\60\u0230\n\60\3\60\3\60\3\60\3\60\3\60\3\60\5\60\u0238\n\60\3"+
		"\60\3\60\3\60\3\60\3\60\3\60\5\60\u0240\n\60\3\60\3\60\3\60\3\60\5\60"+
		"\u0246\n\60\3\60\5\60\u0249\n\60\3\60\3\60\5\60\u024d\n\60\3\60\3\60\3"+
		"\60\3\60\3\60\5\60\u0254\n\60\3\60\3\60\3\60\3\60\5\60\u025a\n\60\7\60"+
		"\u025c\n\60\f\60\16\60\u025f\13\60\3\61\3\61\5\61\u0263\n\61\3\62\3\62"+
		"\3\62\3\63\3\63\3\63\3\63\3\63\7\63\u026d\n\63\f\63\16\63\u0270\13\63"+
		"\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\7\65\u027a\n\65\f\65\16\65\u027d"+
		"\13\65\3\66\3\66\5\66\u0281\n\66\3\66\3\66\5\66\u0285\n\66\5\66\u0287"+
		"\n\66\3\67\5\67\u028a\n\67\3\67\3\67\38\38\38\58\u0291\n8\38\38\38\39"+
		"\39\39\39\59\u029a\n9\3:\3:\3:\3:\5:\u02a0\n:\3;\3;\7;\u02a4\n;\f;\16"+
		";\u02a7\13;\3<\3<\3<\3=\5=\u02ad\n=\3=\3=\3=\3=\3=\5=\u02b4\n=\3>\3>\3"+
		">\3>\3>\3>\3>\3>\3>\3>\3>\3>\5>\u02c2\n>\3?\3?\3@\3@\3@\3@\3@\3@\7@\u02cc"+
		"\n@\f@\16@\u02cf\13@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\6@\u02dd\n@\r"+
		"@\16@\u02de\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\5@\u02f2"+
		"\n@\3@\3@\3@\3@\3@\3@\3@\3@\3@\7@\u02fd\n@\f@\16@\u0300\13@\3A\3A\3B\5"+
		"B\u0305\nB\3B\3B\3B\5B\u030a\nB\3B\5B\u030d\nB\3B\3\u0102\4^~C\2\4\6\b"+
		"\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVX"+
		"Z\\^`bdfhjlnprtvxz|~\u0080\u0082\2\f\3\2GH\4\2\31\31\35\35\4\2((**\4\2"+
		"CCJJ\4\2%%\'\'\3\2;<\3\289\4\2//\64\64\4\2\36\36\67\67\4\2\65\65NO\u0340"+
		"\2\u009b\3\2\2\2\4\u009d\3\2\2\2\6\u00a1\3\2\2\2\b\u00b6\3\2\2\2\n\u00c2"+
		"\3\2\2\2\f\u00c8\3\2\2\2\16\u00e1\3\2\2\2\20\u00e3\3\2\2\2\22\u00f4\3"+
		"\2\2\2\24\u0102\3\2\2\2\26\u0107\3\2\2\2\30\u010b\3\2\2\2\32\u0114\3\2"+
		"\2\2\34\u011f\3\2\2\2\36\u0123\3\2\2\2 \u012e\3\2\2\2\"\u0134\3\2\2\2"+
		"$\u0141\3\2\2\2&\u014a\3\2\2\2(\u014e\3\2\2\2*\u0154\3\2\2\2,\u0158\3"+
		"\2\2\2.\u016b\3\2\2\2\60\u016f\3\2\2\2\62\u017a\3\2\2\2\64\u017f\3\2\2"+
		"\2\66\u0184\3\2\2\28\u0188\3\2\2\2:\u0190\3\2\2\2<\u019d\3\2\2\2>\u01a0"+
		"\3\2\2\2@\u01a8\3\2\2\2B\u01aa\3\2\2\2D\u01af\3\2\2\2F\u01b1\3\2\2\2H"+
		"\u01b3\3\2\2\2J\u01b5\3\2\2\2L\u01b7\3\2\2\2N\u01c2\3\2\2\2P\u01da\3\2"+
		"\2\2R\u01e8\3\2\2\2T\u01f0\3\2\2\2V\u0201\3\2\2\2X\u0210\3\2\2\2Z\u0220"+
		"\3\2\2\2\\\u0222\3\2\2\2^\u0248\3\2\2\2`\u0260\3\2\2\2b\u0264\3\2\2\2"+
		"d\u0267\3\2\2\2f\u0271\3\2\2\2h\u0274\3\2\2\2j\u0286\3\2\2\2l\u0289\3"+
		"\2\2\2n\u028d\3\2\2\2p\u0299\3\2\2\2r\u029f\3\2\2\2t\u02a1\3\2\2\2v\u02a8"+
		"\3\2\2\2x\u02ac\3\2\2\2z\u02c1\3\2\2\2|\u02c3\3\2\2\2~\u02f1\3\2\2\2\u0080"+
		"\u0301\3\2\2\2\u0082\u030c\3\2\2\2\u0084\u009c\5\6\4\2\u0085\u009c\5\b"+
		"\5\2\u0086\u009c\5\n\6\2\u0087\u009c\5\f\7\2\u0088\u009c\5,\27\2\u0089"+
		"\u009c\5(\25\2\u008a\u009c\5&\24\2\u008b\u009c\5.\30\2\u008c\u009c\5*"+
		"\26\2\u008d\u009c\5\60\31\2\u008e\u009c\5\4\3\2\u008f\u0090\7,\2\2\u0090"+
		"\u0095\5N(\2\u0091\u0092\7\7\2\2\u0092\u0094\5N(\2\u0093\u0091\3\2\2\2"+
		"\u0094\u0097\3\2\2\2\u0095\u0093\3\2\2\2\u0095\u0096\3\2\2\2\u0096\u0099"+
		"\3\2\2\2\u0097\u0095\3\2\2\2\u0098\u008f\3\2\2\2\u0098\u0099\3\2\2\2\u0099"+
		"\u009a\3\2\2\2\u009a\u009c\5P)\2\u009b\u0084\3\2\2\2\u009b\u0085\3\2\2"+
		"\2\u009b\u0086\3\2\2\2\u009b\u0087\3\2\2\2\u009b\u0088\3\2\2\2\u009b\u0089"+
		"\3\2\2\2\u009b\u008a\3\2\2\2\u009b\u008b\3\2\2\2\u009b\u008c\3\2\2\2\u009b"+
		"\u008d\3\2\2\2\u009b\u008e\3\2\2\2\u009b\u0098\3\2\2\2\u009c\3\3\2\2\2"+
		"\u009d\u009e\7 \2\2\u009e\u009f\7&\2\2\u009f\u00a0\5p9\2\u00a0\5\3\2\2"+
		"\2\u00a1\u00a2\7.\2\2\u00a2\u00a3\7\27\2\2\u00a3\u00b4\5p9\2\u00a4\u00a6"+
		"\7-\2\2\u00a5\u00a4\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7"+
		"\u00b5\5P)\2\u00a8\u00a9\7\21\2\2\u00a9\u00aa\7\f\2\2\u00aa\u00af\5~@"+
		"\2\u00ab\u00ac\7\7\2\2\u00ac\u00ae\5~@\2\u00ad\u00ab\3\2\2\2\u00ae\u00b1"+
		"\3\2\2\2\u00af\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0\u00b2\3\2\2\2\u00b1"+
		"\u00af\3\2\2\2\u00b2\u00b3\7\32\2\2\u00b3\u00b5\3\2\2\2\u00b4\u00a5\3"+
		"\2\2\2\u00b4\u00a8\3\2\2\2\u00b5\7\3\2\2\2\u00b6\u00b7\7\37\2\2\u00b7"+
		"\u00b8\5p9\2\u00b8\u00bb\7\30\2\2\u00b9\u00bc\5r:\2\u00ba\u00bc\5L\'\2"+
		"\u00bb\u00b9\3\2\2\2\u00bb\u00ba\3\2\2\2\u00bc\u00bd\3\2\2\2\u00bd\u00be"+
		"\78\2\2\u00be\u00c0\5~@\2\u00bf\u00c1\5b\62\2\u00c0\u00bf\3\2\2\2\u00c0"+
		"\u00c1\3\2\2\2\u00c1\t\3\2\2\2\u00c2\u00c3\7\b\2\2\u00c3\u00c4\7-\2\2"+
		"\u00c4\u00c6\5p9\2\u00c5\u00c7\5b\62\2\u00c6\u00c5\3\2\2\2\u00c6\u00c7"+
		"\3\2\2\2\u00c7\13\3\2\2\2\u00c8\u00c9\7#\2\2\u00c9\u00ca\7\34\2\2\u00ca"+
		"\u00cb\5p9\2\u00cb\u00cc\7\f\2\2\u00cc\u00d1\58\35\2\u00cd\u00ce\7\7\2"+
		"\2\u00ce\u00d0\58\35\2\u00cf\u00cd\3\2\2\2\u00d0\u00d3\3\2\2\2\u00d1\u00cf"+
		"\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d6\3\2\2\2\u00d3\u00d1\3\2\2\2\u00d4"+
		"\u00d5\7\7\2\2\u00d5\u00d7\5:\36\2\u00d6\u00d4\3\2\2\2\u00d6\u00d7\3\2"+
		"\2\2\u00d7\u00d8\3\2\2\2\u00d8\u00da\7\32\2\2\u00d9\u00db\5\16\b\2\u00da"+
		"\u00d9\3\2\2\2\u00da\u00db\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc\u00dd\5\26"+
		"\f\2\u00dd\u00de\5$\23\2\u00de\r\3\2\2\2\u00df\u00e2\7I\2\2\u00e0\u00e2"+
		"\5\20\t\2\u00e1\u00df\3\2\2\2\u00e1\u00e0\3\2\2\2\u00e2\17\3\2\2\2\u00e3"+
		"\u00e4\7\16\2\2\u00e4\u00e9\5\22\n\2\u00e5\u00e6\7\60\2\2\u00e6\u00e8"+
		"\5\22\n\2\u00e7\u00e5\3\2\2\2\u00e8\u00eb\3\2\2\2\u00e9\u00e7\3\2\2\2"+
		"\u00e9\u00ea\3\2\2\2\u00ea\u00ec\3\2\2\2\u00eb\u00e9\3\2\2\2\u00ec\u00f2"+
		"\7\24\2\2\u00ed\u00f0\7\7\2\2\u00ee\u00f1\5\34\17\2\u00ef\u00f1\5 \21"+
		"\2\u00f0\u00ee\3\2\2\2\u00f0\u00ef\3\2\2\2\u00f1\u00f3\3\2\2\2\u00f2\u00ed"+
		"\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\21\3\2\2\2\u00f4\u00f5\7\16\2\2\u00f5"+
		"\u00fa\7D\2\2\u00f6\u00f7\7\60\2\2\u00f7\u00f9\7D\2\2\u00f8\u00f6\3\2"+
		"\2\2\u00f9\u00fc\3\2\2\2\u00fa\u00f8\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb"+
		"\u00fd\3\2\2\2\u00fc\u00fa\3\2\2\2\u00fd\u00fe\7\24\2\2\u00fe\23\3\2\2"+
		"\2\u00ff\u0101\13\2\2\2\u0100\u00ff\3\2\2\2\u0101\u0104\3\2\2\2\u0102"+
		"\u0103\3\2\2\2\u0102\u0100\3\2\2\2\u0103\25\3\2\2\2\u0104\u0102\3\2\2"+
		"\2\u0105\u0108\7K\2\2\u0106\u0108\5\30\r\2\u0107\u0105\3\2\2\2\u0107\u0106"+
		"\3\2\2\2\u0108\27\3\2\2\2\u0109\u010c\7J\2\2\u010a\u010c\5\32\16\2\u010b"+
		"\u0109\3\2\2\2\u010b\u010a\3\2\2\2\u010c\u0112\3\2\2\2\u010d\u0110\7\7"+
		"\2\2\u010e\u0111\5\34\17\2\u010f\u0111\5 \21\2\u0110\u010e\3\2\2\2\u0110"+
		"\u010f\3\2\2\2\u0111\u0113\3\2\2\2\u0112\u010d\3\2\2\2\u0112\u0113\3\2"+
		"\2\2\u0113\31\3\2\2\2\u0114\u0115\7\16\2\2\u0115\u011a\7D\2\2\u0116\u0117"+
		"\7\60\2\2\u0117\u0119\7D\2\2\u0118\u0116\3\2\2\2\u0119\u011c\3\2\2\2\u011a"+
		"\u0118\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u011d\3\2\2\2\u011c\u011a\3\2"+
		"\2\2\u011d\u011e\7\24\2\2\u011e\33\3\2\2\2\u011f\u0120\7L\2\2\u0120\u0121"+
		"\7\7\2\2\u0121\u0122\5\36\20\2\u0122\35\3\2\2\2\u0123\u0124\7\16\2\2\u0124"+
		"\u0129\5r:\2\u0125\u0126\7\60\2\2\u0126\u0128\5r:\2\u0127\u0125\3\2\2"+
		"\2\u0128\u012b\3\2\2\2\u0129\u0127\3\2\2\2\u0129\u012a\3\2\2\2\u012a\u012c"+
		"\3\2\2\2\u012b\u0129\3\2\2\2\u012c\u012d\7\24\2\2\u012d\37\3\2\2\2\u012e"+
		"\u012f\7M\2\2\u012f\u0130\7\7\2\2\u0130\u0131\5r:\2\u0131\u0132\7\7\2"+
		"\2\u0132\u0133\5\"\22\2\u0133!\3\2\2\2\u0134\u0135\7\16\2\2\u0135\u013a"+
		"\5\24\13\2\u0136\u0137\7\60\2\2\u0137\u0139\5\24\13\2\u0138\u0136\3\2"+
		"\2\2\u0139\u013c\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b"+
		"\u013d\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u013e\7\24\2\2\u013e#\3\2\2\2"+
		"\u013f\u0142\7J\2\2\u0140\u0142\5\32\16\2\u0141\u013f\3\2\2\2\u0141\u0140"+
		"\3\2\2\2\u0142\u0148\3\2\2\2\u0143\u0146\7\7\2\2\u0144\u0147\5\34\17\2"+
		"\u0145\u0147\5\24\13\2\u0146\u0144\3\2\2\2\u0146\u0145\3\2\2\2\u0147\u0149"+
		"\3\2\2\2\u0148\u0143\3\2\2\2\u0148\u0149\3\2\2\2\u0149%\3\2\2\2\u014a"+
		"\u014b\7+\2\2\u014b\u014c\7\34\2\2\u014c\u014d\5p9\2\u014d\'\3\2\2\2\u014e"+
		"\u014f\7#\2\2\u014f\u0150\7\13\2\2\u0150\u0151\5p9\2\u0151\u0152\7\r\2"+
		"\2\u0152\u0153\5P)\2\u0153)\3\2\2\2\u0154\u0155\7+\2\2\u0155\u0156\7\13"+
		"\2\2\u0156\u0157\5p9\2\u0157+\3\2\2\2\u0158\u015a\7#\2\2\u0159\u015b\7"+
		"F\2\2\u015a\u0159\3\2\2\2\u015a\u015b\3\2\2\2\u015b\u015c\3\2\2\2\u015c"+
		"\u015d\7\4\2\2\u015d\u015e\5p9\2\u015e\u015f\7&\2\2\u015f\u0160\5p9\2"+
		"\u0160\u0161\7\f\2\2\u0161\u0166\5\66\34\2\u0162\u0163\7\7\2\2\u0163\u0165"+
		"\5\66\34\2\u0164\u0162\3\2\2\2\u0165\u0168\3\2\2\2\u0166\u0164\3\2\2\2"+
		"\u0166\u0167\3\2\2\2\u0167\u0169\3\2\2\2\u0168\u0166\3\2\2\2\u0169\u016a"+
		"\7\32\2\2\u016a-\3\2\2\2\u016b\u016c\7+\2\2\u016c\u016d\7\4\2\2\u016d"+
		"\u016e\5p9\2\u016e/\3\2\2\2\u016f\u0170\7\17\2\2\u0170\u0171\t\2\2\2\u0171"+
		"\u0172\7\27\2\2\u0172\u0175\5p9\2\u0173\u0174\7)\2\2\u0174\u0176\5\62"+
		"\32\2\u0175\u0173\3\2\2\2\u0175\u0176\3\2\2\2\u0176\u0177\3\2\2\2\u0177"+
		"\u0178\7-\2\2\u0178\u0179\5\64\33\2\u0179\61\3\2\2\2\u017a\u017b\13\2"+
		"\2\2\u017b\63\3\2\2\2\u017c\u017e\13\2\2\2\u017d\u017c\3\2\2\2\u017e\u0181"+
		"\3\2\2\2\u017f\u017d\3\2\2\2\u017f\u0180\3\2\2\2\u0180\u0182\3\2\2\2\u0181"+
		"\u017f\3\2\2\2\u0182\u0183\7\2\2\3\u0183\65\3\2\2\2\u0184\u0186\5r:\2"+
		"\u0185\u0187\7?\2\2\u0186\u0185\3\2\2\2\u0186\u0187\3\2\2\2\u0187\67\3"+
		"\2\2\2\u0188\u0189\5r:\2\u0189\u018b\5@!\2\u018a\u018c\5<\37\2\u018b\u018a"+
		"\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u018e\3\2\2\2\u018d\u018f\5> \2\u018e"+
		"\u018d\3\2\2\2\u018e\u018f\3\2\2\2\u018f9\3\2\2\2\u0190\u0191\7\20\2\2"+
		"\u0191\u0192\7\"\2\2\u0192\u0193\7\f\2\2\u0193\u0198\5r:\2\u0194\u0195"+
		"\7\7\2\2\u0195\u0197\5r:\2\u0196\u0194\3\2\2\2\u0197\u019a\3\2\2\2\u0198"+
		"\u0196\3\2\2\2\u0198\u0199\3\2\2\2\u0199\u019b\3\2\2\2\u019a\u0198\3\2"+
		"\2\2\u019b\u019c\7\32\2\2\u019c;\3\2\2\2\u019d\u019e\7=\2\2\u019e\u019f"+
		"\7>\2\2\u019f=\3\2\2\2\u01a0\u01a1\7\20\2\2\u01a1\u01a2\7\"\2\2\u01a2"+
		"?\3\2\2\2\u01a3\u01a9\5B\"\2\u01a4\u01a9\5D#\2\u01a5\u01a9\5F$\2\u01a6"+
		"\u01a9\5H%\2\u01a7\u01a9\5J&\2\u01a8\u01a3\3\2\2\2\u01a8\u01a4\3\2\2\2"+
		"\u01a8\u01a5\3\2\2\2\u01a8\u01a6\3\2\2\2\u01a8\u01a7\3\2\2\2\u01a9A\3"+
		"\2\2\2\u01aa\u01ab\t\3\2\2\u01ab\u01ac\7\f\2\2\u01ac\u01ad\7D\2\2\u01ad"+
		"\u01ae\7\32\2\2\u01aeC\3\2\2\2\u01af\u01b0\7\62\2\2\u01b0E\3\2\2\2\u01b1"+
		"\u01b2\7\22\2\2\u01b2G\3\2\2\2\u01b3\u01b4\7N\2\2\u01b4I\3\2\2\2\u01b5"+
		"\u01b6\t\4\2\2\u01b6K\3\2\2\2\u01b7\u01b8\7\f\2\2\u01b8\u01bd\5r:\2\u01b9"+
		"\u01ba\7\7\2\2\u01ba\u01bc\5r:\2\u01bb\u01b9\3\2\2\2\u01bc\u01bf\3\2\2"+
		"\2\u01bd\u01bb\3\2\2\2\u01bd\u01be\3\2\2\2\u01be\u01c0\3\2\2\2\u01bf\u01bd"+
		"\3\2\2\2\u01c0\u01c1\7\32\2\2\u01c1M\3\2\2\2\u01c2\u01ce\7O\2\2\u01c3"+
		"\u01c4\7\f\2\2\u01c4\u01c9\5r:\2\u01c5\u01c6\7\7\2\2\u01c6\u01c8\5r:\2"+
		"\u01c7\u01c5\3\2\2\2\u01c8\u01cb\3\2\2\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca"+
		"\3\2\2\2\u01ca\u01cc\3\2\2\2\u01cb\u01c9\3\2\2\2\u01cc\u01cd\7\32\2\2"+
		"\u01cd\u01cf\3\2\2\2\u01ce\u01c3\3\2\2\2\u01ce\u01cf\3\2\2\2\u01cf\u01d0"+
		"\3\2\2\2\u01d0\u01d1\7\r\2\2\u01d1\u01d2\7\f\2\2\u01d2\u01d3\5P)\2\u01d3"+
		"\u01d4\7\32\2\2\u01d4O\3\2\2\2\u01d5\u01db\5T+\2\u01d6\u01d7\7\f\2\2\u01d7"+
		"\u01d8\5P)\2\u01d8\u01d9\7\32\2\2\u01d9\u01db\3\2\2\2\u01da\u01d5\3\2"+
		"\2\2\u01da\u01d6\3\2\2\2\u01db\u01df\3\2\2\2\u01dc\u01de\5R*\2\u01dd\u01dc"+
		"\3\2\2\2\u01de\u01e1\3\2\2\2\u01df\u01dd\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0"+
		"\u01e3\3\2\2\2\u01e1\u01df\3\2\2\2\u01e2\u01e4\5h\65\2\u01e3\u01e2\3\2"+
		"\2\2\u01e3\u01e4\3\2\2\2\u01e4\u01e6\3\2\2\2\u01e5\u01e7\5n8\2\u01e6\u01e5"+
		"\3\2\2\2\u01e6\u01e7\3\2\2\2\u01e7Q\3\2\2\2\u01e8\u01ee\7B\2\2\u01e9\u01ef"+
		"\5T+\2\u01ea\u01eb\7\f\2\2\u01eb\u01ec\5P)\2\u01ec\u01ed\7\32\2\2\u01ed"+
		"\u01ef\3\2\2\2\u01ee\u01e9\3\2\2\2\u01ee\u01ea\3\2\2\2\u01efS\3\2\2\2"+
		"\u01f0\u01f1\5V,\2\u01f1\u01f3\5\\/\2\u01f2\u01f4\5b\62\2\u01f3\u01f2"+
		"\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4\u01f6\3\2\2\2\u01f5\u01f7\5d\63\2\u01f6"+
		"\u01f5\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7\u01f9\3\2\2\2\u01f8\u01fa\5f"+
		"\64\2\u01f9\u01f8\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\u01fc\3\2\2\2\u01fb"+
		"\u01fd\5h\65\2\u01fc\u01fb\3\2\2\2\u01fc\u01fd\3\2\2\2\u01fd\u01ff\3\2"+
		"\2\2\u01fe\u0200\5n8\2\u01ff\u01fe\3\2\2\2\u01ff\u0200\3\2\2\2\u0200U"+
		"\3\2\2\2\u0201\u0203\7\61\2\2\u0202\u0204\5X-\2\u0203\u0202\3\2\2\2\u0203"+
		"\u0204\3\2\2\2\u0204\u020e\3\2\2\2\u0205\u020f\7\64\2\2\u0206\u020b\5"+
		"Z.\2\u0207\u0208\7\7\2\2\u0208\u020a\5Z.\2\u0209\u0207\3\2\2\2\u020a\u020d"+
		"\3\2\2\2\u020b\u0209\3\2\2\2\u020b\u020c\3\2\2\2\u020c\u020f\3\2\2\2\u020d"+
		"\u020b\3\2\2\2\u020e\u0205\3\2\2\2\u020e\u0206\3\2\2\2\u020fW\3\2\2\2"+
		"\u0210\u0211\t\5\2\2\u0211Y\3\2\2\2\u0212\u0214\5r:\2\u0213\u0215\7\r"+
		"\2\2\u0214\u0213\3\2\2\2\u0214\u0215\3\2\2\2\u0215\u0217\3\2\2\2\u0216"+
		"\u0218\7O\2\2\u0217\u0216\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u0221\3\2"+
		"\2\2\u0219\u021b\5~@\2\u021a\u021c\7\r\2\2\u021b\u021a\3\2\2\2\u021b\u021c"+
		"\3\2\2\2\u021c\u021e\3\2\2\2\u021d\u021f\7O\2\2\u021e\u021d\3\2\2\2\u021e"+
		"\u021f\3\2\2\2\u021f\u0221\3\2\2\2\u0220\u0212\3\2\2\2\u0220\u0219\3\2"+
		"\2\2\u0221[\3\2\2\2\u0222\u0223\7-\2\2\u0223\u0228\5^\60\2\u0224\u0225"+
		"\7\7\2\2\u0225\u0227\5^\60\2\u0226\u0224\3\2\2\2\u0227\u022a\3\2\2\2\u0228"+
		"\u0226\3\2\2\2\u0228\u0229\3\2\2\2\u0229]\3\2\2\2\u022a\u0228\3\2\2\2"+
		"\u022b\u022c\b\60\1\2\u022c\u022d\7\f\2\2\u022d\u022f\5^\60\2\u022e\u0230"+
		"\7@\2\2\u022f\u022e\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u0231\3\2\2\2\u0231"+
		"\u0232\7\25\2\2\u0232\u0233\5^\60\2\u0233\u0234\7&\2\2\u0234\u0235\5t"+
		";\2\u0235\u0237\7\32\2\2\u0236\u0238\5l\67\2\u0237\u0236\3\2\2\2\u0237"+
		"\u0238\3\2\2\2\u0238\u0249\3\2\2\2\u0239\u023a\7\f\2\2\u023a\u023b\5^"+
		"\60\2\u023b\u023c\7A\2\2\u023c\u023d\5^\60\2\u023d\u023f\7\32\2\2\u023e"+
		"\u0240\5l\67\2\u023f\u023e\3\2\2\2\u023f\u0240\3\2\2\2\u0240\u0249\3\2"+
		"\2\2\u0241\u0242\7\f\2\2\u0242\u0243\5P)\2\u0243\u0245\7\32\2\2\u0244"+
		"\u0246\5l\67\2\u0245\u0244\3\2\2\2\u0245\u0246\3\2\2\2\u0246\u0249\3\2"+
		"\2\2\u0247\u0249\5`\61\2\u0248\u022b\3\2\2\2\u0248\u0239\3\2\2\2\u0248"+
		"\u0241\3\2\2\2\u0248\u0247\3\2\2\2\u0249\u025d\3\2\2\2\u024a\u024c\f\b"+
		"\2\2\u024b\u024d\7@\2\2\u024c\u024b\3\2\2\2\u024c\u024d\3\2\2\2\u024d"+
		"\u024e\3\2\2\2\u024e\u024f\7\25\2\2\u024f\u0250\5^\60\2\u0250\u0251\7"+
		"&\2\2\u0251\u0253\5t;\2\u0252\u0254\5l\67\2\u0253\u0252\3\2\2\2\u0253"+
		"\u0254\3\2\2\2\u0254\u025c\3\2\2\2\u0255\u0256\f\7\2\2\u0256\u0257\7A"+
		"\2\2\u0257\u0259\5^\60\2\u0258\u025a\5l\67\2\u0259\u0258\3\2\2\2\u0259"+
		"\u025a\3\2\2\2\u025a\u025c\3\2\2\2\u025b\u024a\3\2\2\2\u025b\u0255\3\2"+
		"\2\2\u025c\u025f\3\2\2\2\u025d\u025b\3\2\2\2\u025d\u025e\3\2\2\2\u025e"+
		"_\3\2\2\2\u025f\u025d\3\2\2\2\u0260\u0262\5p9\2\u0261\u0263\5l\67\2\u0262"+
		"\u0261\3\2\2\2\u0262\u0263\3\2\2\2\u0263a\3\2\2\2\u0264\u0265\7\t\2\2"+
		"\u0265\u0266\5t;\2\u0266c\3\2\2\2\u0267\u0268\7\3\2\2\u0268\u0269\7\5"+
		"\2\2\u0269\u026e\5r:\2\u026a\u026b\7\7\2\2\u026b\u026d\5r:\2\u026c\u026a"+
		"\3\2\2\2\u026d\u0270\3\2\2\2\u026e\u026c\3\2\2\2\u026e\u026f\3\2\2\2\u026f"+
		"e\3\2\2\2\u0270\u026e\3\2\2\2\u0271\u0272\7\n\2\2\u0272\u0273\5t;\2\u0273"+
		"g\3\2\2\2\u0274\u0275\7\26\2\2\u0275\u0276\7\5\2\2\u0276\u027b\5j\66\2"+
		"\u0277\u0278\7\7\2\2\u0278\u027a\5j\66\2\u0279\u0277\3\2\2\2\u027a\u027d"+
		"\3\2\2\2\u027b\u0279\3\2\2\2\u027b\u027c\3\2\2\2\u027ci\3\2\2\2\u027d"+
		"\u027b\3\2\2\2\u027e\u0280\7D\2\2\u027f\u0281\7?\2\2\u0280\u027f\3\2\2"+
		"\2\u0280\u0281\3\2\2\2\u0281\u0287\3\2\2\2\u0282\u0284\5r:\2\u0283\u0285"+
		"\7?\2\2\u0284\u0283\3\2\2\2\u0284\u0285\3\2\2\2\u0285\u0287\3\2\2\2\u0286"+
		"\u027e\3\2\2\2\u0286\u0282\3\2\2\2\u0287k\3\2\2\2\u0288\u028a\7\r\2\2"+
		"\u0289\u0288\3\2\2\2\u0289\u028a\3\2\2\2\u028a\u028b\3\2\2\2\u028b\u028c"+
		"\7O\2\2\u028cm\3\2\2\2\u028d\u028e\7$\2\2\u028e\u0290\7\23\2\2\u028f\u0291"+
		"\7D\2\2\u0290\u028f\3\2\2\2\u0290\u0291\3\2\2\2\u0291\u0292\3\2\2\2\u0292"+
		"\u0293\t\6\2\2\u0293\u0294\7!\2\2\u0294o\3\2\2\2\u0295\u029a\7O\2\2\u0296"+
		"\u0297\7O\2\2\u0297\u0298\7\33\2\2\u0298\u029a\7O\2\2\u0299\u0295\3\2"+
		"\2\2\u0299\u0296\3\2\2\2\u029aq\3\2\2\2\u029b\u02a0\7O\2\2\u029c\u029d"+
		"\7O\2\2\u029d\u029e\7\33\2\2\u029e\u02a0\7O\2\2\u029f\u029b\3\2\2\2\u029f"+
		"\u029c\3\2\2\2\u02a0s\3\2\2\2\u02a1\u02a5\5x=\2\u02a2\u02a4\5v<\2\u02a3"+
		"\u02a2\3\2\2\2\u02a4\u02a7\3\2\2\2\u02a5\u02a3\3\2\2\2\u02a5\u02a6\3\2"+
		"\2\2\u02a6u\3\2\2\2\u02a7\u02a5\3\2\2\2\u02a8\u02a9\t\7\2\2\u02a9\u02aa"+
		"\5x=\2\u02aaw\3\2\2\2\u02ab\u02ad\7=\2\2\u02ac\u02ab\3\2\2\2\u02ac\u02ad"+
		"\3\2\2\2\u02ad\u02b3\3\2\2\2\u02ae\u02b4\5z>\2\u02af\u02b0\7\f\2\2\u02b0"+
		"\u02b1\5t;\2\u02b1\u02b2\7\32\2\2\u02b2\u02b4\3\2\2\2\u02b3\u02ae\3\2"+
		"\2\2\u02b3\u02af\3\2\2\2\u02b4y\3\2\2\2\u02b5\u02b6\5~@\2\u02b6\u02b7"+
		"\5|?\2\u02b7\u02b8\5~@\2\u02b8\u02c2\3\2\2\2\u02b9\u02ba\5~@\2\u02ba\u02bb"+
		"\7:\2\2\u02bb\u02c2\3\2\2\2\u02bc\u02bd\7\6\2\2\u02bd\u02be\7\f\2\2\u02be"+
		"\u02bf\5T+\2\u02bf\u02c0\7\32\2\2\u02c0\u02c2\3\2\2\2\u02c1\u02b5\3\2"+
		"\2\2\u02c1\u02b9\3\2\2\2\u02c1\u02bc\3\2\2\2\u02c2{\3\2\2\2\u02c3\u02c4"+
		"\t\b\2\2\u02c4}\3\2\2\2\u02c5\u02c6\b@\1\2\u02c6\u02c7\5\u0080A\2\u02c7"+
		"\u02c8\7\f\2\2\u02c8\u02cd\5~@\2\u02c9\u02ca\7\7\2\2\u02ca\u02cc\5~@\2"+
		"\u02cb\u02c9\3\2\2\2\u02cc\u02cf\3\2\2\2\u02cd\u02cb\3\2\2\2\u02cd\u02ce"+
		"\3\2\2\2\u02ce\u02d0\3\2\2\2\u02cf\u02cd\3\2\2\2\u02d0\u02d1\7\32\2\2"+
		"\u02d1\u02f2\3\2\2\2\u02d2\u02d3\7\65\2\2\u02d3\u02d4\7\f\2\2\u02d4\u02d5"+
		"\7C\2\2\u02d5\u02d6\5~@\2\u02d6\u02d7\7\32\2\2\u02d7\u02f2\3\2\2\2\u02d8"+
		"\u02d9\7\f\2\2\u02d9\u02dc\5~@\2\u02da\u02db\7\7\2\2\u02db\u02dd\5~@\2"+
		"\u02dc\u02da\3\2\2\2\u02dd\u02de\3\2\2\2\u02de\u02dc\3\2\2\2\u02de\u02df"+
		"\3\2\2\2\u02df\u02e0\3\2\2\2\u02e0\u02e1\7\32\2\2\u02e1\u02f2\3\2\2\2"+
		"\u02e2\u02e3\7\65\2\2\u02e3\u02e4\7\f\2\2\u02e4\u02e5\7\64\2\2\u02e5\u02f2"+
		"\7\32\2\2\u02e6\u02f2\5\u0082B\2\u02e7\u02f2\5r:\2\u02e8\u02e9\7\f\2\2"+
		"\u02e9\u02ea\5T+\2\u02ea\u02eb\7\32\2\2\u02eb\u02f2\3\2\2\2\u02ec\u02ed"+
		"\7\f\2\2\u02ed\u02ee\5~@\2\u02ee\u02ef\7\32\2\2\u02ef\u02f2\3\2\2\2\u02f0"+
		"\u02f2\7>\2\2\u02f1\u02c5\3\2\2\2\u02f1\u02d2\3\2\2\2\u02f1\u02d8\3\2"+
		"\2\2\u02f1\u02e2\3\2\2\2\u02f1\u02e6\3\2\2\2\u02f1\u02e7\3\2\2\2\u02f1"+
		"\u02e8\3\2\2\2\u02f1\u02ec\3\2\2\2\u02f1\u02f0\3\2\2\2\u02f2\u02fe\3\2"+
		"\2\2\u02f3\u02f4\f\16\2\2\u02f4\u02f5\t\t\2\2\u02f5\u02fd\5~@\17\u02f6"+
		"\u02f7\f\r\2\2\u02f7\u02f8\t\n\2\2\u02f8\u02fd\5~@\16\u02f9\u02fa\f\f"+
		"\2\2\u02fa\u02fb\7\66\2\2\u02fb\u02fd\5~@\r\u02fc\u02f3\3\2\2\2\u02fc"+
		"\u02f6\3\2\2\2\u02fc\u02f9\3\2\2\2\u02fd\u0300\3\2\2\2\u02fe\u02fc\3\2"+
		"\2\2\u02fe\u02ff\3\2\2\2\u02ff\177\3\2\2\2\u0300\u02fe\3\2\2\2\u0301\u0302"+
		"\t\13\2\2\u0302\u0081\3\2\2\2\u0303\u0305\7\67\2\2\u0304\u0303\3\2\2\2"+
		"\u0304\u0305\3\2\2\2\u0305\u0306\3\2\2\2\u0306\u0309\7D\2\2\u0307\u0308"+
		"\7\33\2\2\u0308\u030a\7D\2\2\u0309\u0307\3\2\2\2\u0309\u030a\3\2\2\2\u030a"+
		"\u030d\3\2\2\2\u030b\u030d\7\63\2\2\u030c\u0304\3\2\2\2\u030c\u030b\3"+
		"\2\2\2\u030d\u0083\3\2\2\2]\u0095\u0098\u009b\u00a5\u00af\u00b4\u00bb"+
		"\u00c0\u00c6\u00d1\u00d6\u00da\u00e1\u00e9\u00f0\u00f2\u00fa\u0102\u0107"+
		"\u010b\u0110\u0112\u011a\u0129\u013a\u0141\u0146\u0148\u015a\u0166\u0175"+
		"\u017f\u0186\u018b\u018e\u0198\u01a8\u01bd\u01c9\u01ce\u01da\u01df\u01e3"+
		"\u01e6\u01ee\u01f3\u01f6\u01f9\u01fc\u01ff\u0203\u020b\u020e\u0214\u0217"+
		"\u021b\u021e\u0220\u0228\u022f\u0237\u023f\u0245\u0248\u024c\u0253\u0259"+
		"\u025b\u025d\u0262\u026e\u027b\u0280\u0284\u0286\u0289\u0290\u0299\u029f"+
		"\u02a5\u02ac\u02b3\u02c1\u02cd\u02de\u02f1\u02fc\u02fe\u0304\u0309\u030c";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}