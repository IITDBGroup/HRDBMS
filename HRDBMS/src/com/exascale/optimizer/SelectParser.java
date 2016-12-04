// Generated from src/com/exascale/optimizer/Select.g4 by ANTLR 4.5.3
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
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, T__30=31, 
		T__31=32, T__32=33, T__33=34, T__34=35, T__35=36, T__36=37, T__37=38, 
		T__38=39, T__39=40, T__40=41, T__41=42, T__42=43, T__43=44, T__44=45, 
		T__45=46, T__46=47, T__47=48, T__48=49, T__49=50, T__50=51, T__51=52, 
		T__52=53, STRING=54, STAR=55, COUNT=56, CONCAT=57, NEGATIVE=58, EQUALS=59, 
		OPERATOR=60, NULLOPERATOR=61, AND=62, OR=63, NOT=64, NULL=65, DIRECTION=66, 
		JOINTYPE=67, CROSSJOIN=68, TABLECOMBINATION=69, COLUMN=70, DISTINCT=71, 
		INTEGER=72, WS=73, UNIQUE=74, REPLACE=75, RESUME=76, NONE=77, ALL=78, 
		ANYTEXT=79, HASH=80, RANGE=81, DATE=82, COLORDER=83, ORGANIZATION=84, 
		IDENTIFIER=85, ANY=86;
	public static final int
		RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_valuesList = 3, 
		RULE_update = 4, RULE_setClause = 5, RULE_delete = 6, RULE_createTable = 7, 
		RULE_organization = 8, RULE_colOrder = 9, RULE_groupExp = 10, RULE_realGroupExp = 11, 
		RULE_groupDef = 12, RULE_rangeExp = 13, RULE_nodeExp = 14, RULE_realNodeExp = 15, 
		RULE_integerSet = 16, RULE_hashExp = 17, RULE_columnSet = 18, RULE_rangeType = 19, 
		RULE_rangeSet = 20, RULE_deviceExp = 21, RULE_dropTable = 22, RULE_createView = 23, 
		RULE_dropView = 24, RULE_createIndex = 25, RULE_dropIndex = 26, RULE_load = 27, 
		RULE_any = 28, RULE_remainder = 29, RULE_indexDef = 30, RULE_colDef = 31, 
		RULE_primaryKey = 32, RULE_notNull = 33, RULE_primary = 34, RULE_dataType = 35, 
		RULE_char2 = 36, RULE_int2 = 37, RULE_long2 = 38, RULE_date2 = 39, RULE_float2 = 40, 
		RULE_colList = 41, RULE_commonTableExpression = 42, RULE_fullSelect = 43, 
		RULE_connectedSelect = 44, RULE_subSelect = 45, RULE_selectClause = 46, 
		RULE_selecthow = 47, RULE_selectListEntry = 48, RULE_fromClause = 49, 
		RULE_tableReference = 50, RULE_singleTable = 51, RULE_whereClause = 52, 
		RULE_groupBy = 53, RULE_havingClause = 54, RULE_orderBy = 55, RULE_sortKey = 56, 
		RULE_correlationClause = 57, RULE_fetchFirst = 58, RULE_tableName = 59, 
		RULE_columnName = 60, RULE_searchCondition = 61, RULE_connectedSearchClause = 62, 
		RULE_searchClause = 63, RULE_predicate = 64, RULE_operator = 65, RULE_expression = 66, 
		RULE_caseCase = 67, RULE_identifier = 68, RULE_literal = 69;
	public static final String[] ruleNames = {
		"select", "runstats", "insert", "valuesList", "update", "setClause", "delete", 
		"createTable", "organization", "colOrder", "groupExp", "realGroupExp", 
		"groupDef", "rangeExp", "nodeExp", "realNodeExp", "integerSet", "hashExp", 
		"columnSet", "rangeType", "rangeSet", "deviceExp", "dropTable", "createView", 
		"dropView", "createIndex", "dropIndex", "load", "any", "remainder", "indexDef", 
		"colDef", "primaryKey", "notNull", "primary", "dataType", "char2", "int2", 
		"long2", "date2", "float2", "colList", "commonTableExpression", "fullSelect", 
		"connectedSelect", "subSelect", "selectClause", "selecthow", "selectListEntry", 
		"fromClause", "tableReference", "singleTable", "whereClause", "groupBy", 
		"havingClause", "orderBy", "sortKey", "correlationClause", "fetchFirst", 
		"tableName", "columnName", "searchCondition", "connectedSearchClause", 
		"searchClause", "predicate", "operator", "expression", "caseCase", "identifier", 
		"literal"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'WITH'", "','", "'RUNSTATS'", "'ON'", "'INSERT'", "'INTO'", "'FROM'", 
		"'VALUES'", "'('", "')'", "'UPDATE'", "'SET'", "'DELETE'", "'CREATE'", 
		"'TABLE'", "'{'", "'|'", "'}'", "'DROP'", "'VIEW'", "'AS'", "'INDEX'", 
		"'LOAD'", "'DELIMITER'", "'PRIMARY'", "'KEY'", "'CHAR'", "'VARCHAR'", 
		"'INTEGER'", "'BIGINT'", "'FLOAT'", "'DOUBLE'", "'SELECT'", "'JOIN'", 
		"'WHERE'", "'GROUP'", "'BY'", "'HAVING'", "'ORDER'", "'FETCH'", "'FIRST'", 
		"'ROW'", "'ROWS'", "'ONLY'", "'.'", "'EXISTS'", "'/'", "'+'", "'CASE'", 
		"'ELSE'", "'END'", "'WHEN'", "'THEN'", null, "'*'", "'COUNT'", "'||'", 
		"'-'", "'='", null, null, "'AND'", "'OR'", "'NOT'", "'NULL'", null, null, 
		"'CROSS JOIN'", null, "'COLUMN'", "'DISTINCT'", null, null, "'UNIQUE'", 
		"'REPLACE'", "'RESUME'", "'NONE'", "'ALL'", "'ANY'", "'HASH'", "'RANGE'", 
		"'DATE'", "'COLORDER'", "'ORGANIZATION'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, "STRING", "STAR", "COUNT", "CONCAT", 
		"NEGATIVE", "EQUALS", "OPERATOR", "NULLOPERATOR", "AND", "OR", "NOT", 
		"NULL", "DIRECTION", "JOINTYPE", "CROSSJOIN", "TABLECOMBINATION", "COLUMN", 
		"DISTINCT", "INTEGER", "WS", "UNIQUE", "REPLACE", "RESUME", "NONE", "ALL", 
		"ANYTEXT", "HASH", "RANGE", "DATE", "COLORDER", "ORGANIZATION", "IDENTIFIER", 
		"ANY"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Select.g4"; }

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
		public InsertContext insert() {
			return getRuleContext(InsertContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SelectParser.EOF, 0); }
		public UpdateContext update() {
			return getRuleContext(UpdateContext.class,0);
		}
		public DeleteContext delete() {
			return getRuleContext(DeleteContext.class,0);
		}
		public CreateTableContext createTable() {
			return getRuleContext(CreateTableContext.class,0);
		}
		public CreateIndexContext createIndex() {
			return getRuleContext(CreateIndexContext.class,0);
		}
		public CreateViewContext createView() {
			return getRuleContext(CreateViewContext.class,0);
		}
		public DropTableContext dropTable() {
			return getRuleContext(DropTableContext.class,0);
		}
		public DropIndexContext dropIndex() {
			return getRuleContext(DropIndexContext.class,0);
		}
		public DropViewContext dropView() {
			return getRuleContext(DropViewContext.class,0);
		}
		public LoadContext load() {
			return getRuleContext(LoadContext.class,0);
		}
		public RunstatsContext runstats() {
			return getRuleContext(RunstatsContext.class,0);
		}
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public List<CommonTableExpressionContext> commonTableExpression() {
			return getRuleContexts(CommonTableExpressionContext.class);
		}
		public CommonTableExpressionContext commonTableExpression(int i) {
			return getRuleContext(CommonTableExpressionContext.class,i);
		}
		public SelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select; }
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
			setState(188);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(140);
				insert();
				setState(141);
				match(EOF);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(143);
				update();
				setState(144);
				match(EOF);
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(146);
				delete();
				setState(147);
				match(EOF);
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(149);
				createTable();
				setState(150);
				match(EOF);
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(152);
				createIndex();
				setState(153);
				match(EOF);
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(155);
				createView();
				setState(156);
				match(EOF);
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				{
				setState(158);
				dropTable();
				setState(159);
				match(EOF);
				}
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				{
				setState(161);
				dropIndex();
				setState(162);
				match(EOF);
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				{
				setState(164);
				dropView();
				setState(165);
				match(EOF);
				}
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				{
				setState(167);
				load();
				setState(168);
				match(EOF);
				}
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				{
				setState(170);
				runstats();
				setState(171);
				match(EOF);
				}
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				{
				{
				setState(182);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(173);
					match(T__0);
					setState(174);
					commonTableExpression();
					setState(179);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(175);
						match(T__1);
						setState(176);
						commonTableExpression();
						}
						}
						setState(181);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(184);
				fullSelect();
				}
				setState(186);
				match(EOF);
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
			setState(190);
			match(T__2);
			setState(191);
			match(T__3);
			setState(192);
			tableName();
			}
		}
		catch (RecognitionException re) {
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
		public List<ValuesListContext> valuesList() {
			return getRuleContexts(ValuesListContext.class);
		}
		public ValuesListContext valuesList(int i) {
			return getRuleContext(ValuesListContext.class,i);
		}
		public InsertContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert; }
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
			setState(194);
			match(T__4);
			setState(195);
			match(T__5);
			setState(196);
			tableName();
			setState(210);
			switch (_input.LA(1)) {
			case T__6:
			case T__8:
			case T__32:
				{
				{
				setState(198);
				_la = _input.LA(1);
				if (_la==T__6) {
					{
					setState(197);
					match(T__6);
					}
				}

				setState(200);
				fullSelect();
				}
				}
				break;
			case T__7:
				{
				{
				setState(201);
				match(T__7);
				setState(202);
				valuesList();
				setState(207);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(203);
					match(T__1);
					setState(204);
					valuesList();
					}
					}
					setState(209);
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
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ValuesListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valuesList; }
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
			setState(212);
			match(T__8);
			setState(213);
			expression(0);
			setState(218);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(214);
				match(T__1);
				setState(215);
				expression(0);
				}
				}
				setState(220);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(221);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public List<SetClauseContext> setClause() {
			return getRuleContexts(SetClauseContext.class);
		}
		public SetClauseContext setClause(int i) {
			return getRuleContext(SetClauseContext.class,i);
		}
		public UpdateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update; }
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
			setState(223);
			match(T__10);
			setState(224);
			tableName();
			setState(226); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(225);
				setClause();
				}
				}
				setState(228); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==T__11 );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetClauseContext extends ParserRuleContext {
		public TerminalNode EQUALS() { return getToken(SelectParser.EQUALS, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public ColListContext colList() {
			return getRuleContext(ColListContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setClause; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSetClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetClauseContext setClause() throws RecognitionException {
		SetClauseContext _localctx = new SetClauseContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_setClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(230);
			match(T__11);
			setState(233);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				setState(231);
				columnName();
				}
				break;
			case T__8:
				{
				setState(232);
				colList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(235);
			match(EQUALS);
			setState(236);
			expression(0);
			setState(238);
			_la = _input.LA(1);
			if (_la==T__34) {
				{
				setState(237);
				whereClause();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDelete(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeleteContext delete() throws RecognitionException {
		DeleteContext _localctx = new DeleteContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_delete);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(240);
			match(T__12);
			setState(241);
			match(T__6);
			setState(242);
			tableName();
			setState(244);
			_la = _input.LA(1);
			if (_la==T__34) {
				{
				setState(243);
				whereClause();
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
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public List<ColDefContext> colDef() {
			return getRuleContexts(ColDefContext.class);
		}
		public ColDefContext colDef(int i) {
			return getRuleContext(ColDefContext.class,i);
		}
		public NodeExpContext nodeExp() {
			return getRuleContext(NodeExpContext.class,0);
		}
		public DeviceExpContext deviceExp() {
			return getRuleContext(DeviceExpContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SelectParser.COLUMN, 0); }
		public PrimaryKeyContext primaryKey() {
			return getRuleContext(PrimaryKeyContext.class,0);
		}
		public ColOrderContext colOrder() {
			return getRuleContext(ColOrderContext.class,0);
		}
		public OrganizationContext organization() {
			return getRuleContext(OrganizationContext.class,0);
		}
		public GroupExpContext groupExp() {
			return getRuleContext(GroupExpContext.class,0);
		}
		public CreateTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTable; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableContext createTable() throws RecognitionException {
		CreateTableContext _localctx = new CreateTableContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_createTable);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(246);
			match(T__13);
			setState(248);
			_la = _input.LA(1);
			if (_la==COLUMN) {
				{
				setState(247);
				match(COLUMN);
				}
			}

			setState(250);
			match(T__14);
			setState(251);
			tableName();
			setState(252);
			match(T__8);
			setState(253);
			colDef();
			setState(258);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(254);
					match(T__1);
					setState(255);
					colDef();
					}
					} 
				}
				setState(260);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
			}
			setState(263);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(261);
				match(T__1);
				setState(262);
				primaryKey();
				}
			}

			setState(265);
			match(T__9);
			setState(267);
			_la = _input.LA(1);
			if (_la==COLORDER) {
				{
				setState(266);
				colOrder();
				}
			}

			setState(270);
			_la = _input.LA(1);
			if (_la==ORGANIZATION) {
				{
				setState(269);
				organization();
				}
			}

			setState(273);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
			case 1:
				{
				setState(272);
				groupExp();
				}
				break;
			}
			setState(275);
			nodeExp();
			setState(276);
			deviceExp();
			}
		}
		catch (RecognitionException re) {
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
		public TerminalNode ORGANIZATION() { return getToken(SelectParser.ORGANIZATION, 0); }
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public OrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_organization; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrganizationContext organization() throws RecognitionException {
		OrganizationContext _localctx = new OrganizationContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_organization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(278);
			match(ORGANIZATION);
			setState(279);
			match(T__8);
			setState(280);
			match(INTEGER);
			setState(285);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(281);
				match(T__1);
				setState(282);
				match(INTEGER);
				}
				}
				setState(287);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(288);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public TerminalNode COLORDER() { return getToken(SelectParser.COLORDER, 0); }
		public List<TerminalNode> INTEGER() { return getTokens(SelectParser.INTEGER); }
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public ColOrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colOrder; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColOrder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColOrderContext colOrder() throws RecognitionException {
		ColOrderContext _localctx = new ColOrderContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_colOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(290);
			match(COLORDER);
			setState(291);
			match(T__8);
			setState(292);
			match(INTEGER);
			setState(297);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(293);
				match(T__1);
				setState(294);
				match(INTEGER);
				}
				}
				setState(299);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(300);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGroupExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupExpContext groupExp() throws RecognitionException {
		GroupExpContext _localctx = new GroupExpContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_groupExp);
		try {
			setState(304);
			switch (_input.LA(1)) {
			case NONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(302);
				match(NONE);
				}
				break;
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(303);
				realGroupExp();
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
		public List<GroupDefContext> groupDef() {
			return getRuleContexts(GroupDefContext.class);
		}
		public GroupDefContext groupDef(int i) {
			return getRuleContext(GroupDefContext.class,i);
		}
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public RealGroupExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_realGroupExp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRealGroupExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RealGroupExpContext realGroupExp() throws RecognitionException {
		RealGroupExpContext _localctx = new RealGroupExpContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_realGroupExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(306);
			match(T__15);
			setState(307);
			groupDef();
			setState(312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__16) {
				{
				{
				setState(308);
				match(T__16);
				setState(309);
				groupDef();
				}
				}
				setState(314);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(315);
			match(T__17);
			setState(321);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(316);
				match(T__1);
				setState(319);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(317);
					hashExp();
					}
					break;
				case RANGE:
					{
					setState(318);
					rangeType();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGroupDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupDefContext groupDef() throws RecognitionException {
		GroupDefContext _localctx = new GroupDefContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_groupDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(323);
			match(T__15);
			setState(324);
			match(INTEGER);
			setState(329);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__16) {
				{
				{
				setState(325);
				match(T__16);
				setState(326);
				match(INTEGER);
				}
				}
				setState(331);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(332);
			match(T__17);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRangeExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeExpContext rangeExp() throws RecognitionException {
		RangeExpContext _localctx = new RangeExpContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_rangeExp);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(337);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(334);
					matchWildcard();
					}
					} 
				}
				setState(339);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNodeExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeExpContext nodeExp() throws RecognitionException {
		NodeExpContext _localctx = new NodeExpContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_nodeExp);
		try {
			setState(342);
			switch (_input.LA(1)) {
			case ANYTEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(340);
				match(ANYTEXT);
				}
				break;
			case T__15:
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(341);
				realNodeExp();
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
		public IntegerSetContext integerSet() {
			return getRuleContext(IntegerSetContext.class,0);
		}
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public RangeTypeContext rangeType() {
			return getRuleContext(RangeTypeContext.class,0);
		}
		public RealNodeExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_realNodeExp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRealNodeExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RealNodeExpContext realNodeExp() throws RecognitionException {
		RealNodeExpContext _localctx = new RealNodeExpContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_realNodeExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(346);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(344);
				match(ALL);
				}
				break;
			case T__15:
				{
				setState(345);
				integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(353);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(348);
				match(T__1);
				setState(351);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(349);
					hashExp();
					}
					break;
				case RANGE:
					{
					setState(350);
					rangeType();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIntegerSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntegerSetContext integerSet() throws RecognitionException {
		IntegerSetContext _localctx = new IntegerSetContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_integerSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(355);
			match(T__15);
			setState(356);
			match(INTEGER);
			setState(361);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__16) {
				{
				{
				setState(357);
				match(T__16);
				setState(358);
				match(INTEGER);
				}
				}
				setState(363);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(364);
			match(T__17);
			}
		}
		catch (RecognitionException re) {
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
		public TerminalNode HASH() { return getToken(SelectParser.HASH, 0); }
		public ColumnSetContext columnSet() {
			return getRuleContext(ColumnSetContext.class,0);
		}
		public HashExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hashExp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitHashExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HashExpContext hashExp() throws RecognitionException {
		HashExpContext _localctx = new HashExpContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_hashExp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(366);
			match(HASH);
			setState(367);
			match(T__1);
			setState(368);
			columnSet();
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColumnSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnSetContext columnSet() throws RecognitionException {
		ColumnSetContext _localctx = new ColumnSetContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_columnSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(370);
			match(T__15);
			setState(371);
			columnName();
			setState(376);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__16) {
				{
				{
				setState(372);
				match(T__16);
				setState(373);
				columnName();
				}
				}
				setState(378);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(379);
			match(T__17);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRangeType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeTypeContext rangeType() throws RecognitionException {
		RangeTypeContext _localctx = new RangeTypeContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_rangeType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(381);
			match(RANGE);
			setState(382);
			match(T__1);
			setState(383);
			columnName();
			setState(384);
			match(T__1);
			setState(385);
			rangeSet();
			}
		}
		catch (RecognitionException re) {
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
		public List<RangeExpContext> rangeExp() {
			return getRuleContexts(RangeExpContext.class);
		}
		public RangeExpContext rangeExp(int i) {
			return getRuleContext(RangeExpContext.class,i);
		}
		public RangeSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeSet; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRangeSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeSetContext rangeSet() throws RecognitionException {
		RangeSetContext _localctx = new RangeSetContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_rangeSet);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(387);
			match(T__15);
			setState(388);
			rangeExp();
			setState(393);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__16) {
				{
				{
				setState(389);
				match(T__16);
				setState(390);
				rangeExp();
				}
				}
				setState(395);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(396);
			match(T__17);
			}
		}
		catch (RecognitionException re) {
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
		public IntegerSetContext integerSet() {
			return getRuleContext(IntegerSetContext.class,0);
		}
		public HashExpContext hashExp() {
			return getRuleContext(HashExpContext.class,0);
		}
		public RangeExpContext rangeExp() {
			return getRuleContext(RangeExpContext.class,0);
		}
		public DeviceExpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deviceExp; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDeviceExp(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeviceExpContext deviceExp() throws RecognitionException {
		DeviceExpContext _localctx = new DeviceExpContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_deviceExp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(400);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(398);
				match(ALL);
				}
				break;
			case T__15:
				{
				setState(399);
				integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(407);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(402);
				match(T__1);
				setState(405);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
				case 1:
					{
					setState(403);
					hashExp();
					}
					break;
				case 2:
					{
					setState(404);
					rangeExp();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropTableContext dropTable() throws RecognitionException {
		DropTableContext _localctx = new DropTableContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_dropTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(409);
			match(T__18);
			setState(410);
			match(T__14);
			setState(411);
			tableName();
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateViewContext createView() throws RecognitionException {
		CreateViewContext _localctx = new CreateViewContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_createView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(413);
			match(T__13);
			setState(414);
			match(T__19);
			setState(415);
			tableName();
			setState(416);
			match(T__20);
			setState(417);
			fullSelect();
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDropView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropViewContext dropView() throws RecognitionException {
		DropViewContext _localctx = new DropViewContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_dropView);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(419);
			match(T__18);
			setState(420);
			match(T__19);
			setState(421);
			tableName();
			}
		}
		catch (RecognitionException re) {
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
		public List<TableNameContext> tableName() {
			return getRuleContexts(TableNameContext.class);
		}
		public TableNameContext tableName(int i) {
			return getRuleContext(TableNameContext.class,i);
		}
		public List<IndexDefContext> indexDef() {
			return getRuleContexts(IndexDefContext.class);
		}
		public IndexDefContext indexDef(int i) {
			return getRuleContext(IndexDefContext.class,i);
		}
		public TerminalNode UNIQUE() { return getToken(SelectParser.UNIQUE, 0); }
		public CreateIndexContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createIndex; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCreateIndex(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateIndexContext createIndex() throws RecognitionException {
		CreateIndexContext _localctx = new CreateIndexContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_createIndex);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(423);
			match(T__13);
			setState(425);
			_la = _input.LA(1);
			if (_la==UNIQUE) {
				{
				setState(424);
				match(UNIQUE);
				}
			}

			setState(427);
			match(T__21);
			setState(428);
			tableName();
			setState(429);
			match(T__3);
			setState(430);
			tableName();
			setState(431);
			match(T__8);
			setState(432);
			indexDef();
			setState(437);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(433);
				match(T__1);
				setState(434);
				indexDef();
				}
				}
				setState(439);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(440);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDropIndex(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropIndexContext dropIndex() throws RecognitionException {
		DropIndexContext _localctx = new DropIndexContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_dropIndex);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(442);
			match(T__18);
			setState(443);
			match(T__21);
			setState(444);
			tableName();
			}
		}
		catch (RecognitionException re) {
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
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public RemainderContext remainder() {
			return getRuleContext(RemainderContext.class,0);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitLoad(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LoadContext load() throws RecognitionException {
		LoadContext _localctx = new LoadContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_load);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(446);
			match(T__22);
			setState(447);
			_la = _input.LA(1);
			if ( !(_la==REPLACE || _la==RESUME) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(448);
			match(T__5);
			setState(449);
			tableName();
			setState(452);
			_la = _input.LA(1);
			if (_la==T__23) {
				{
				setState(450);
				match(T__23);
				setState(451);
				any();
				}
			}

			setState(454);
			match(T__6);
			setState(455);
			remainder();
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitAny(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnyContext any() throws RecognitionException {
		AnyContext _localctx = new AnyContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_any);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(457);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitRemainder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RemainderContext remainder() throws RecognitionException {
		RemainderContext _localctx = new RemainderContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_remainder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(462);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << T__29) | (1L << T__30) | (1L << T__31) | (1L << T__32) | (1L << T__33) | (1L << T__34) | (1L << T__35) | (1L << T__36) | (1L << T__37) | (1L << T__38) | (1L << T__39) | (1L << T__40) | (1L << T__41) | (1L << T__42) | (1L << T__43) | (1L << T__44) | (1L << T__45) | (1L << T__46) | (1L << T__47) | (1L << T__48) | (1L << T__49) | (1L << T__50) | (1L << T__51) | (1L << T__52) | (1L << STRING) | (1L << STAR) | (1L << COUNT) | (1L << CONCAT) | (1L << NEGATIVE) | (1L << EQUALS) | (1L << OPERATOR) | (1L << NULLOPERATOR) | (1L << AND) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (COLUMN - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (COLORDER - 64)) | (1L << (ORGANIZATION - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0)) {
				{
				{
				setState(459);
				matchWildcard();
				}
				}
				setState(464);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(465);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIndexDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IndexDefContext indexDef() throws RecognitionException {
		IndexDefContext _localctx = new IndexDefContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_indexDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(467);
			columnName();
			setState(469);
			_la = _input.LA(1);
			if (_la==DIRECTION) {
				{
				setState(468);
				match(DIRECTION);
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
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public NotNullContext notNull() {
			return getRuleContext(NotNullContext.class,0);
		}
		public PrimaryContext primary() {
			return getRuleContext(PrimaryContext.class,0);
		}
		public ColDefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colDef; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColDefContext colDef() throws RecognitionException {
		ColDefContext _localctx = new ColDefContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_colDef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(471);
			columnName();
			setState(472);
			dataType();
			setState(474);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(473);
				notNull();
				}
			}

			setState(477);
			_la = _input.LA(1);
			if (_la==T__24) {
				{
				setState(476);
				primary();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitPrimaryKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryKeyContext primaryKey() throws RecognitionException {
		PrimaryKeyContext _localctx = new PrimaryKeyContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_primaryKey);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(479);
			match(T__24);
			setState(480);
			match(T__25);
			setState(481);
			match(T__8);
			setState(482);
			columnName();
			setState(487);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(483);
				match(T__1);
				setState(484);
				columnName();
				}
				}
				setState(489);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(490);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNotNull(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NotNullContext notNull() throws RecognitionException {
		NotNullContext _localctx = new NotNullContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_notNull);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(492);
			match(NOT);
			setState(493);
			match(NULL);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitPrimary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryContext primary() throws RecognitionException {
		PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_primary);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(495);
			match(T__24);
			setState(496);
			match(T__25);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_dataType);
		try {
			setState(503);
			switch (_input.LA(1)) {
			case T__26:
			case T__27:
				enterOuterAlt(_localctx, 1);
				{
				setState(498);
				char2();
				}
				break;
			case T__28:
				enterOuterAlt(_localctx, 2);
				{
				setState(499);
				int2();
				}
				break;
			case T__29:
				enterOuterAlt(_localctx, 3);
				{
				setState(500);
				long2();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 4);
				{
				setState(501);
				date2();
				}
				break;
			case T__30:
			case T__31:
				enterOuterAlt(_localctx, 5);
				{
				setState(502);
				float2();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitChar2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Char2Context char2() throws RecognitionException {
		Char2Context _localctx = new Char2Context(_ctx, getState());
		enterRule(_localctx, 72, RULE_char2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(505);
			_la = _input.LA(1);
			if ( !(_la==T__26 || _la==T__27) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(506);
			match(T__8);
			setState(507);
			match(INTEGER);
			setState(508);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitInt2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Int2Context int2() throws RecognitionException {
		Int2Context _localctx = new Int2Context(_ctx, getState());
		enterRule(_localctx, 74, RULE_int2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(510);
			match(T__28);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitLong2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Long2Context long2() throws RecognitionException {
		Long2Context _localctx = new Long2Context(_ctx, getState());
		enterRule(_localctx, 76, RULE_long2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(512);
			match(T__29);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitDate2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Date2Context date2() throws RecognitionException {
		Date2Context _localctx = new Date2Context(_ctx, getState());
		enterRule(_localctx, 78, RULE_date2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(514);
			match(DATE);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFloat2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Float2Context float2() throws RecognitionException {
		Float2Context _localctx = new Float2Context(_ctx, getState());
		enterRule(_localctx, 80, RULE_float2);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(516);
			_la = _input.LA(1);
			if ( !(_la==T__30 || _la==T__31) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColListContext colList() throws RecognitionException {
		ColListContext _localctx = new ColListContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_colList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(518);
			match(T__8);
			setState(519);
			columnName();
			setState(524);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(520);
				match(T__1);
				setState(521);
				columnName();
				}
				}
				setState(526);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(527);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public TerminalNode IDENTIFIER() { return getToken(SelectParser.IDENTIFIER, 0); }
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public List<ColumnNameContext> columnName() {
			return getRuleContexts(ColumnNameContext.class);
		}
		public ColumnNameContext columnName(int i) {
			return getRuleContext(ColumnNameContext.class,i);
		}
		public CommonTableExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commonTableExpression; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCommonTableExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommonTableExpressionContext commonTableExpression() throws RecognitionException {
		CommonTableExpressionContext _localctx = new CommonTableExpressionContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_commonTableExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(529);
			match(IDENTIFIER);
			setState(541);
			_la = _input.LA(1);
			if (_la==T__8) {
				{
				setState(530);
				match(T__8);
				setState(531);
				columnName();
				setState(536);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(532);
					match(T__1);
					setState(533);
					columnName();
					}
					}
					setState(538);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(539);
				match(T__9);
				}
			}

			setState(543);
			match(T__20);
			setState(544);
			match(T__8);
			setState(545);
			fullSelect();
			setState(546);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
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
		public FullSelectContext fullSelect() {
			return getRuleContext(FullSelectContext.class,0);
		}
		public List<ConnectedSelectContext> connectedSelect() {
			return getRuleContexts(ConnectedSelectContext.class);
		}
		public ConnectedSelectContext connectedSelect(int i) {
			return getRuleContext(ConnectedSelectContext.class,i);
		}
		public OrderByContext orderBy() {
			return getRuleContext(OrderByContext.class,0);
		}
		public FetchFirstContext fetchFirst() {
			return getRuleContext(FetchFirstContext.class,0);
		}
		public FullSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullSelect; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFullSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FullSelectContext fullSelect() throws RecognitionException {
		FullSelectContext _localctx = new FullSelectContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_fullSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(553);
			switch (_input.LA(1)) {
			case T__32:
				{
				setState(548);
				subSelect();
				}
				break;
			case T__8:
				{
				setState(549);
				match(T__8);
				setState(550);
				fullSelect();
				setState(551);
				match(T__9);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(558);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==TABLECOMBINATION) {
				{
				{
				setState(555);
				connectedSelect();
				}
				}
				setState(560);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(562);
			_la = _input.LA(1);
			if (_la==T__38) {
				{
				setState(561);
				orderBy();
				}
			}

			setState(565);
			_la = _input.LA(1);
			if (_la==T__39) {
				{
				setState(564);
				fetchFirst();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitConnectedSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConnectedSelectContext connectedSelect() throws RecognitionException {
		ConnectedSelectContext _localctx = new ConnectedSelectContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_connectedSelect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(567);
			match(TABLECOMBINATION);
			setState(573);
			switch (_input.LA(1)) {
			case T__32:
				{
				setState(568);
				subSelect();
				}
				break;
			case T__8:
				{
				setState(569);
				match(T__8);
				setState(570);
				fullSelect();
				setState(571);
				match(T__9);
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
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public OrderByContext orderBy() {
			return getRuleContext(OrderByContext.class,0);
		}
		public FetchFirstContext fetchFirst() {
			return getRuleContext(FetchFirstContext.class,0);
		}
		public SubSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subSelect; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSubSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubSelectContext subSelect() throws RecognitionException {
		SubSelectContext _localctx = new SubSelectContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_subSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(575);
			selectClause();
			setState(576);
			fromClause();
			setState(578);
			_la = _input.LA(1);
			if (_la==T__34) {
				{
				setState(577);
				whereClause();
				}
			}

			setState(581);
			_la = _input.LA(1);
			if (_la==T__35) {
				{
				setState(580);
				groupBy();
				}
			}

			setState(584);
			_la = _input.LA(1);
			if (_la==T__37) {
				{
				setState(583);
				havingClause();
				}
			}

			setState(587);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				{
				setState(586);
				orderBy();
				}
				break;
			}
			setState(590);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,56,_ctx) ) {
			case 1:
				{
				setState(589);
				fetchFirst();
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
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
		public SelecthowContext selecthow() {
			return getRuleContext(SelecthowContext.class,0);
		}
		public List<SelectListEntryContext> selectListEntry() {
			return getRuleContexts(SelectListEntryContext.class);
		}
		public SelectListEntryContext selectListEntry(int i) {
			return getRuleContext(SelectListEntryContext.class,i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(592);
			match(T__32);
			setState(594);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==ALL) {
				{
				setState(593);
				selecthow();
				}
			}

			setState(605);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(596);
				match(STAR);
				}
				break;
			case T__8:
			case T__48:
			case STRING:
			case COUNT:
			case NEGATIVE:
			case NULL:
			case INTEGER:
			case DATE:
			case IDENTIFIER:
				{
				{
				setState(597);
				selectListEntry();
				setState(602);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(598);
					match(T__1);
					setState(599);
					selectListEntry();
					}
					}
					setState(604);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelecthow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelecthowContext selecthow() throws RecognitionException {
		SelecthowContext _localctx = new SelecthowContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_selecthow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(607);
			_la = _input.LA(1);
			if ( !(_la==DISTINCT || _la==ALL) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSelectExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectListEntryContext selectListEntry() throws RecognitionException {
		SelectListEntryContext _localctx = new SelectListEntryContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_selectListEntry);
		int _la;
		try {
			setState(623);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
			case 1:
				_localctx = new SelectColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(609);
				columnName();
				setState(611);
				_la = _input.LA(1);
				if (_la==T__20) {
					{
					setState(610);
					match(T__20);
					}
				}

				setState(614);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(613);
					match(IDENTIFIER);
					}
				}

				}
				break;
			case 2:
				_localctx = new SelectExpressionContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(616);
				expression(0);
				setState(618);
				_la = _input.LA(1);
				if (_la==T__20) {
					{
					setState(617);
					match(T__20);
					}
				}

				setState(621);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(620);
					match(IDENTIFIER);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(625);
			match(T__6);
			setState(626);
			tableReference(0);
			setState(631);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(627);
				match(T__1);
				setState(628);
				tableReference(0);
				}
				}
				setState(633);
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
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public TerminalNode JOINTYPE() { return getToken(SelectParser.JOINTYPE, 0); }
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public JoinPContext(TableReferenceContext ctx) { copyFrom(ctx); }
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNestedTable(this);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIsSingleTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class JoinContext extends TableReferenceContext {
		public List<TableReferenceContext> tableReference() {
			return getRuleContexts(TableReferenceContext.class);
		}
		public TableReferenceContext tableReference(int i) {
			return getRuleContext(TableReferenceContext.class,i);
		}
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public TerminalNode JOINTYPE() { return getToken(SelectParser.JOINTYPE, 0); }
		public CorrelationClauseContext correlationClause() {
			return getRuleContext(CorrelationClauseContext.class,0);
		}
		public JoinContext(TableReferenceContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitJoin(this);
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
		int _startState = 100;
		enterRecursionRule(_localctx, 100, RULE_tableReference, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(663);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
			case 1:
				{
				_localctx = new JoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(635);
				match(T__8);
				setState(636);
				tableReference(0);
				setState(638);
				_la = _input.LA(1);
				if (_la==JOINTYPE) {
					{
					setState(637);
					match(JOINTYPE);
					}
				}

				setState(640);
				match(T__33);
				setState(641);
				tableReference(0);
				setState(642);
				match(T__3);
				setState(643);
				searchCondition();
				setState(644);
				match(T__9);
				setState(646);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
				case 1:
					{
					setState(645);
					correlationClause();
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
				setState(648);
				match(T__8);
				setState(649);
				tableReference(0);
				setState(650);
				match(CROSSJOIN);
				setState(651);
				tableReference(0);
				setState(652);
				match(T__9);
				setState(654);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
				case 1:
					{
					setState(653);
					correlationClause();
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
				setState(656);
				match(T__8);
				setState(657);
				fullSelect();
				setState(658);
				match(T__9);
				setState(660);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
				case 1:
					{
					setState(659);
					correlationClause();
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
				setState(662);
				singleTable();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(684);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,75,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(682);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
					case 1:
						{
						_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(665);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(667);
						_la = _input.LA(1);
						if (_la==JOINTYPE) {
							{
							setState(666);
							match(JOINTYPE);
							}
						}

						setState(669);
						match(T__33);
						setState(670);
						tableReference(0);
						setState(671);
						match(T__3);
						setState(672);
						searchCondition();
						setState(674);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
						case 1:
							{
							setState(673);
							correlationClause();
							}
							break;
						}
						}
						break;
					case 2:
						{
						_localctx = new CrossJoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(676);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(677);
						match(CROSSJOIN);
						setState(678);
						tableReference(0);
						setState(680);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
						case 1:
							{
							setState(679);
							correlationClause();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(686);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSingleTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableContext singleTable() throws RecognitionException {
		SingleTableContext _localctx = new SingleTableContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_singleTable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(687);
			tableName();
			setState(689);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
			case 1:
				{
				setState(688);
				correlationClause();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(691);
			match(T__34);
			setState(692);
			searchCondition();
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitGroupBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByContext groupBy() throws RecognitionException {
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_groupBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(694);
			match(T__35);
			setState(695);
			match(T__36);
			setState(696);
			columnName();
			setState(701);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(697);
				match(T__1);
				setState(698);
				columnName();
				}
				}
				setState(703);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitHavingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(704);
			match(T__37);
			setState(705);
			searchCondition();
			}
		}
		catch (RecognitionException re) {
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
		public List<SortKeyContext> sortKey() {
			return getRuleContexts(SortKeyContext.class);
		}
		public SortKeyContext sortKey(int i) {
			return getRuleContext(SortKeyContext.class,i);
		}
		public OrderByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderBy; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitOrderBy(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderByContext orderBy() throws RecognitionException {
		OrderByContext _localctx = new OrderByContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_orderBy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(707);
			match(T__38);
			setState(708);
			match(T__36);
			setState(709);
			sortKey();
			setState(714);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(710);
				match(T__1);
				setState(711);
				sortKey();
				}
				}
				setState(716);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSortKeyInt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortKeyContext sortKey() throws RecognitionException {
		SortKeyContext _localctx = new SortKeyContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_sortKey);
		int _la;
		try {
			setState(725);
			switch (_input.LA(1)) {
			case INTEGER:
				_localctx = new SortKeyIntContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(717);
				match(INTEGER);
				setState(719);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(718);
					match(DIRECTION);
					}
				}

				}
				break;
			case IDENTIFIER:
				_localctx = new SortKeyColContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(721);
				columnName();
				setState(723);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(722);
					match(DIRECTION);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCorrelationClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CorrelationClauseContext correlationClause() throws RecognitionException {
		CorrelationClauseContext _localctx = new CorrelationClauseContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_correlationClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(728);
			_la = _input.LA(1);
			if (_la==T__20) {
				{
				setState(727);
				match(T__20);
				}
			}

			setState(730);
			match(IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitFetchFirst(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FetchFirstContext fetchFirst() throws RecognitionException {
		FetchFirstContext _localctx = new FetchFirstContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_fetchFirst);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(732);
			match(T__39);
			setState(733);
			match(T__40);
			setState(735);
			_la = _input.LA(1);
			if (_la==INTEGER) {
				{
				setState(734);
				match(INTEGER);
				}
			}

			setState(737);
			_la = _input.LA(1);
			if ( !(_la==T__41 || _la==T__42) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(738);
			match(T__43);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitTable1Part(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Table2PartContext extends TableNameContext {
		public List<TerminalNode> IDENTIFIER() { return getTokens(SelectParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SelectParser.IDENTIFIER, i);
		}
		public Table2PartContext(TableNameContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitTable2Part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableNameContext tableName() throws RecognitionException {
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_tableName);
		try {
			setState(744);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
			case 1:
				_localctx = new Table1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(740);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new Table2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(741);
				match(IDENTIFIER);
				setState(742);
				match(T__44);
				setState(743);
				match(IDENTIFIER);
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
		public List<TerminalNode> IDENTIFIER() { return getTokens(SelectParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SelectParser.IDENTIFIER, i);
		}
		public Col2PartContext(ColumnNameContext ctx) { copyFrom(ctx); }
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCol1Part(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameContext columnName() throws RecognitionException {
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_columnName);
		try {
			setState(750);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
			case 1:
				_localctx = new Col1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(746);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new Col2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(747);
				match(IDENTIFIER);
				setState(748);
				match(T__44);
				setState(749);
				match(IDENTIFIER);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSearchCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SearchConditionContext searchCondition() throws RecognitionException {
		SearchConditionContext _localctx = new SearchConditionContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_searchCondition);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(752);
			searchClause();
			setState(756);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,86,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(753);
					connectedSearchClause();
					}
					} 
				}
				setState(758);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitConnectedSearchClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConnectedSearchClauseContext connectedSearchClause() throws RecognitionException {
		ConnectedSearchClauseContext _localctx = new ConnectedSearchClauseContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_connectedSearchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(759);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OR) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(760);
			searchClause();
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitSearchClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SearchClauseContext searchClause() throws RecognitionException {
		SearchClauseContext _localctx = new SearchClauseContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_searchClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(763);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(762);
				match(NOT);
				}
			}

			setState(770);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
			case 1:
				{
				setState(765);
				predicate();
				}
				break;
			case 2:
				{
				{
				setState(766);
				match(T__8);
				setState(767);
				searchCondition();
				setState(768);
				match(T__9);
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NormalPredicateContext extends PredicateContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public OperatorContext operator() {
			return getRuleContext(OperatorContext.class,0);
		}
		public NormalPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitNormalPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_predicate);
		try {
			setState(784);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
			case 1:
				_localctx = new NormalPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(772);
				expression(0);
				setState(773);
				operator();
				setState(774);
				expression(0);
				}
				}
				break;
			case 2:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(776);
				expression(0);
				setState(777);
				match(NULLOPERATOR);
				}
				}
				break;
			case 3:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(779);
				match(T__45);
				setState(780);
				match(T__8);
				setState(781);
				subSelect();
				setState(782);
				match(T__9);
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
		public TerminalNode OPERATOR() { return getToken(SelectParser.OPERATOR, 0); }
		public TerminalNode EQUALS() { return getToken(SelectParser.EQUALS, 0); }
		public OperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operator; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OperatorContext operator() throws RecognitionException {
		OperatorContext _localctx = new OperatorContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_operator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(786);
			_la = _input.LA(1);
			if ( !(_la==EQUALS || _la==OPERATOR) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitExpSelect(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MulDivContext extends ExpressionContext {
		public Token op;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
		public MulDivContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitMulDiv(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddSubContext extends ExpressionContext {
		public Token op;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NEGATIVE() { return getToken(SelectParser.NEGATIVE, 0); }
		public AddSubContext(ExpressionContext ctx) { copyFrom(ctx); }
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCaseExp(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NullExpContext extends ExpressionContext {
		public TerminalNode NULL() { return getToken(SelectParser.NULL, 0); }
		public NullExpContext(ExpressionContext ctx) { copyFrom(ctx); }
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitPExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConcatContext extends ExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode CONCAT() { return getToken(SelectParser.CONCAT, 0); }
		public ConcatContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitConcat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionContext extends ExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public FunctionContext(ExpressionContext ctx) { copyFrom(ctx); }
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitColLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ListContext extends ExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ListContext(ExpressionContext ctx) { copyFrom(ctx); }
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
		int _startState = 132;
		enterRecursionRule(_localctx, 132, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(846);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
			case 1:
				{
				_localctx = new FunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(789);
				identifier();
				setState(790);
				match(T__8);
				setState(791);
				expression(0);
				setState(796);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(792);
					match(T__1);
					setState(793);
					expression(0);
					}
					}
					setState(798);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(799);
				match(T__9);
				}
				break;
			case 2:
				{
				_localctx = new CountDistinctContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(801);
				match(COUNT);
				setState(802);
				match(T__8);
				setState(803);
				match(DISTINCT);
				setState(804);
				expression(0);
				setState(805);
				match(T__9);
				}
				break;
			case 3:
				{
				_localctx = new ListContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(807);
				match(T__8);
				setState(808);
				expression(0);
				setState(811); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(809);
					match(T__1);
					setState(810);
					expression(0);
					}
					}
					setState(813); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__1 );
				setState(815);
				match(T__9);
				}
				break;
			case 4:
				{
				_localctx = new CountStarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(817);
				match(COUNT);
				setState(818);
				match(T__8);
				setState(819);
				match(STAR);
				setState(820);
				match(T__9);
				}
				break;
			case 5:
				{
				_localctx = new IsLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(821);
				literal();
				}
				break;
			case 6:
				{
				_localctx = new ColLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(822);
				columnName();
				}
				break;
			case 7:
				{
				_localctx = new ExpSelectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(823);
				match(T__8);
				setState(824);
				subSelect();
				setState(825);
				match(T__9);
				}
				break;
			case 8:
				{
				_localctx = new PExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(827);
				match(T__8);
				setState(828);
				expression(0);
				setState(829);
				match(T__9);
				}
				break;
			case 9:
				{
				_localctx = new NullExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(831);
				match(NULL);
				}
				break;
			case 10:
				{
				_localctx = new CaseExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(832);
				match(T__48);
				setState(833);
				caseCase();
				setState(837);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__51) {
					{
					{
					setState(834);
					caseCase();
					}
					}
					setState(839);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(840);
				match(T__49);
				setState(841);
				expression(0);
				setState(842);
				match(T__50);
				setState(844);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
				case 1:
					{
					setState(843);
					match(T__48);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(859);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(857);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
					case 1:
						{
						_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(848);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(849);
						((MulDivContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__46 || _la==STAR) ) {
							((MulDivContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(850);
						expression(14);
						}
						break;
					case 2:
						{
						_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(851);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(852);
						((AddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__47 || _la==NEGATIVE) ) {
							((AddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(853);
						expression(13);
						}
						break;
					case 3:
						{
						_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(854);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(855);
						match(CONCAT);
						setState(856);
						expression(12);
						}
						break;
					}
					} 
				}
				setState(861);
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
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CaseCaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseCase; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitCaseCase(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseCaseContext caseCase() throws RecognitionException {
		CaseCaseContext _localctx = new CaseCaseContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_caseCase);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(862);
			match(T__51);
			setState(863);
			searchCondition();
			setState(864);
			match(T__52);
			setState(865);
			expression(0);
			}
		}
		catch (RecognitionException re) {
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(867);
			_la = _input.LA(1);
			if ( !(((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & ((1L << (COUNT - 56)) | (1L << (DATE - 56)) | (1L << (IDENTIFIER - 56)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
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
		public TerminalNode INTEGER(int i) {
			return getToken(SelectParser.INTEGER, i);
		}
		public TerminalNode NEGATIVE() { return getToken(SelectParser.NEGATIVE, 0); }
		public NumericLiteralContext(LiteralContext ctx) { copyFrom(ctx); }
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
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SelectVisitor ) return ((SelectVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_literal);
		int _la;
		try {
			setState(878);
			switch (_input.LA(1)) {
			case NEGATIVE:
			case INTEGER:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(870);
				_la = _input.LA(1);
				if (_la==NEGATIVE) {
					{
					setState(869);
					match(NEGATIVE);
					}
				}

				setState(872);
				match(INTEGER);
				setState(875);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(873);
					match(T__44);
					setState(874);
					match(INTEGER);
					}
					break;
				}
				}
				break;
			case STRING:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(877);
				match(STRING);
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
		case 50:
			return tableReference_sempred((TableReferenceContext)_localctx, predIndex);
		case 66:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean tableReference_sempred(TableReferenceContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 6);
		case 1:
			return precpred(_ctx, 5);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 13);
		case 3:
			return precpred(_ctx, 12);
		case 4:
			return precpred(_ctx, 11);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3X\u0373\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\3\2\3\2\3"+
		"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\7"+
		"\2\u00b4\n\2\f\2\16\2\u00b7\13\2\5\2\u00b9\n\2\3\2\3\2\3\2\3\2\5\2\u00bf"+
		"\n\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4\u00c9\n\4\3\4\3\4\3\4\3\4\3\4"+
		"\7\4\u00d0\n\4\f\4\16\4\u00d3\13\4\5\4\u00d5\n\4\3\5\3\5\3\5\3\5\7\5\u00db"+
		"\n\5\f\5\16\5\u00de\13\5\3\5\3\5\3\6\3\6\3\6\6\6\u00e5\n\6\r\6\16\6\u00e6"+
		"\3\7\3\7\3\7\5\7\u00ec\n\7\3\7\3\7\3\7\5\7\u00f1\n\7\3\b\3\b\3\b\3\b\5"+
		"\b\u00f7\n\b\3\t\3\t\5\t\u00fb\n\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u0103\n"+
		"\t\f\t\16\t\u0106\13\t\3\t\3\t\5\t\u010a\n\t\3\t\3\t\5\t\u010e\n\t\3\t"+
		"\5\t\u0111\n\t\3\t\5\t\u0114\n\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\7\n\u011e"+
		"\n\n\f\n\16\n\u0121\13\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\7\13\u012a\n"+
		"\13\f\13\16\13\u012d\13\13\3\13\3\13\3\f\3\f\5\f\u0133\n\f\3\r\3\r\3\r"+
		"\3\r\7\r\u0139\n\r\f\r\16\r\u013c\13\r\3\r\3\r\3\r\3\r\5\r\u0142\n\r\5"+
		"\r\u0144\n\r\3\16\3\16\3\16\3\16\7\16\u014a\n\16\f\16\16\16\u014d\13\16"+
		"\3\16\3\16\3\17\7\17\u0152\n\17\f\17\16\17\u0155\13\17\3\20\3\20\5\20"+
		"\u0159\n\20\3\21\3\21\5\21\u015d\n\21\3\21\3\21\3\21\5\21\u0162\n\21\5"+
		"\21\u0164\n\21\3\22\3\22\3\22\3\22\7\22\u016a\n\22\f\22\16\22\u016d\13"+
		"\22\3\22\3\22\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\7\24\u0179\n\24"+
		"\f\24\16\24\u017c\13\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3"+
		"\26\3\26\3\26\7\26\u018a\n\26\f\26\16\26\u018d\13\26\3\26\3\26\3\27\3"+
		"\27\5\27\u0193\n\27\3\27\3\27\3\27\5\27\u0198\n\27\5\27\u019a\n\27\3\30"+
		"\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\33"+
		"\3\33\5\33\u01ac\n\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\7\33\u01b6"+
		"\n\33\f\33\16\33\u01b9\13\33\3\33\3\33\3\34\3\34\3\34\3\34\3\35\3\35\3"+
		"\35\3\35\3\35\3\35\5\35\u01c7\n\35\3\35\3\35\3\35\3\36\3\36\3\37\7\37"+
		"\u01cf\n\37\f\37\16\37\u01d2\13\37\3\37\3\37\3 \3 \5 \u01d8\n \3!\3!\3"+
		"!\5!\u01dd\n!\3!\5!\u01e0\n!\3\"\3\"\3\"\3\"\3\"\3\"\7\"\u01e8\n\"\f\""+
		"\16\"\u01eb\13\"\3\"\3\"\3#\3#\3#\3$\3$\3$\3%\3%\3%\3%\3%\5%\u01fa\n%"+
		"\3&\3&\3&\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3+\3+\7+\u020d\n+\f+\16"+
		"+\u0210\13+\3+\3+\3,\3,\3,\3,\3,\7,\u0219\n,\f,\16,\u021c\13,\3,\3,\5"+
		",\u0220\n,\3,\3,\3,\3,\3,\3-\3-\3-\3-\3-\5-\u022c\n-\3-\7-\u022f\n-\f"+
		"-\16-\u0232\13-\3-\5-\u0235\n-\3-\5-\u0238\n-\3.\3.\3.\3.\3.\3.\5.\u0240"+
		"\n.\3/\3/\3/\5/\u0245\n/\3/\5/\u0248\n/\3/\5/\u024b\n/\3/\5/\u024e\n/"+
		"\3/\5/\u0251\n/\3\60\3\60\5\60\u0255\n\60\3\60\3\60\3\60\3\60\7\60\u025b"+
		"\n\60\f\60\16\60\u025e\13\60\5\60\u0260\n\60\3\61\3\61\3\62\3\62\5\62"+
		"\u0266\n\62\3\62\5\62\u0269\n\62\3\62\3\62\5\62\u026d\n\62\3\62\5\62\u0270"+
		"\n\62\5\62\u0272\n\62\3\63\3\63\3\63\3\63\7\63\u0278\n\63\f\63\16\63\u027b"+
		"\13\63\3\64\3\64\3\64\3\64\5\64\u0281\n\64\3\64\3\64\3\64\3\64\3\64\3"+
		"\64\5\64\u0289\n\64\3\64\3\64\3\64\3\64\3\64\3\64\5\64\u0291\n\64\3\64"+
		"\3\64\3\64\3\64\5\64\u0297\n\64\3\64\5\64\u029a\n\64\3\64\3\64\5\64\u029e"+
		"\n\64\3\64\3\64\3\64\3\64\3\64\5\64\u02a5\n\64\3\64\3\64\3\64\3\64\5\64"+
		"\u02ab\n\64\7\64\u02ad\n\64\f\64\16\64\u02b0\13\64\3\65\3\65\5\65\u02b4"+
		"\n\65\3\66\3\66\3\66\3\67\3\67\3\67\3\67\3\67\7\67\u02be\n\67\f\67\16"+
		"\67\u02c1\13\67\38\38\38\39\39\39\39\39\79\u02cb\n9\f9\169\u02ce\139\3"+
		":\3:\5:\u02d2\n:\3:\3:\5:\u02d6\n:\5:\u02d8\n:\3;\5;\u02db\n;\3;\3;\3"+
		"<\3<\3<\5<\u02e2\n<\3<\3<\3<\3=\3=\3=\3=\5=\u02eb\n=\3>\3>\3>\3>\5>\u02f1"+
		"\n>\3?\3?\7?\u02f5\n?\f?\16?\u02f8\13?\3@\3@\3@\3A\5A\u02fe\nA\3A\3A\3"+
		"A\3A\3A\5A\u0305\nA\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\5B\u0313\nB\3"+
		"C\3C\3D\3D\3D\3D\3D\3D\7D\u031d\nD\fD\16D\u0320\13D\3D\3D\3D\3D\3D\3D"+
		"\3D\3D\3D\3D\3D\3D\6D\u032e\nD\rD\16D\u032f\3D\3D\3D\3D\3D\3D\3D\3D\3"+
		"D\3D\3D\3D\3D\3D\3D\3D\3D\3D\3D\3D\7D\u0346\nD\fD\16D\u0349\13D\3D\3D"+
		"\3D\3D\5D\u034f\nD\5D\u0351\nD\3D\3D\3D\3D\3D\3D\3D\3D\3D\7D\u035c\nD"+
		"\fD\16D\u035f\13D\3E\3E\3E\3E\3E\3F\3F\3G\5G\u0369\nG\3G\3G\3G\5G\u036e"+
		"\nG\3G\5G\u0371\nG\3G\3\u0153\4f\u0086H\2\4\6\b\n\f\16\20\22\24\26\30"+
		"\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080"+
		"\u0082\u0084\u0086\u0088\u008a\u008c\2\f\3\2MN\3\2\35\36\3\2!\"\4\2II"+
		"PP\3\2,-\3\2@A\3\2=>\4\2\61\6199\4\2\62\62<<\5\2::TTWW\u03a9\2\u00be\3"+
		"\2\2\2\4\u00c0\3\2\2\2\6\u00c4\3\2\2\2\b\u00d6\3\2\2\2\n\u00e1\3\2\2\2"+
		"\f\u00e8\3\2\2\2\16\u00f2\3\2\2\2\20\u00f8\3\2\2\2\22\u0118\3\2\2\2\24"+
		"\u0124\3\2\2\2\26\u0132\3\2\2\2\30\u0134\3\2\2\2\32\u0145\3\2\2\2\34\u0153"+
		"\3\2\2\2\36\u0158\3\2\2\2 \u015c\3\2\2\2\"\u0165\3\2\2\2$\u0170\3\2\2"+
		"\2&\u0174\3\2\2\2(\u017f\3\2\2\2*\u0185\3\2\2\2,\u0192\3\2\2\2.\u019b"+
		"\3\2\2\2\60\u019f\3\2\2\2\62\u01a5\3\2\2\2\64\u01a9\3\2\2\2\66\u01bc\3"+
		"\2\2\28\u01c0\3\2\2\2:\u01cb\3\2\2\2<\u01d0\3\2\2\2>\u01d5\3\2\2\2@\u01d9"+
		"\3\2\2\2B\u01e1\3\2\2\2D\u01ee\3\2\2\2F\u01f1\3\2\2\2H\u01f9\3\2\2\2J"+
		"\u01fb\3\2\2\2L\u0200\3\2\2\2N\u0202\3\2\2\2P\u0204\3\2\2\2R\u0206\3\2"+
		"\2\2T\u0208\3\2\2\2V\u0213\3\2\2\2X\u022b\3\2\2\2Z\u0239\3\2\2\2\\\u0241"+
		"\3\2\2\2^\u0252\3\2\2\2`\u0261\3\2\2\2b\u0271\3\2\2\2d\u0273\3\2\2\2f"+
		"\u0299\3\2\2\2h\u02b1\3\2\2\2j\u02b5\3\2\2\2l\u02b8\3\2\2\2n\u02c2\3\2"+
		"\2\2p\u02c5\3\2\2\2r\u02d7\3\2\2\2t\u02da\3\2\2\2v\u02de\3\2\2\2x\u02ea"+
		"\3\2\2\2z\u02f0\3\2\2\2|\u02f2\3\2\2\2~\u02f9\3\2\2\2\u0080\u02fd\3\2"+
		"\2\2\u0082\u0312\3\2\2\2\u0084\u0314\3\2\2\2\u0086\u0350\3\2\2\2\u0088"+
		"\u0360\3\2\2\2\u008a\u0365\3\2\2\2\u008c\u0370\3\2\2\2\u008e\u008f\5\6"+
		"\4\2\u008f\u0090\7\2\2\3\u0090\u00bf\3\2\2\2\u0091\u0092\5\n\6\2\u0092"+
		"\u0093\7\2\2\3\u0093\u00bf\3\2\2\2\u0094\u0095\5\16\b\2\u0095\u0096\7"+
		"\2\2\3\u0096\u00bf\3\2\2\2\u0097\u0098\5\20\t\2\u0098\u0099\7\2\2\3\u0099"+
		"\u00bf\3\2\2\2\u009a\u009b\5\64\33\2\u009b\u009c\7\2\2\3\u009c\u00bf\3"+
		"\2\2\2\u009d\u009e\5\60\31\2\u009e\u009f\7\2\2\3\u009f\u00bf\3\2\2\2\u00a0"+
		"\u00a1\5.\30\2\u00a1\u00a2\7\2\2\3\u00a2\u00bf\3\2\2\2\u00a3\u00a4\5\66"+
		"\34\2\u00a4\u00a5\7\2\2\3\u00a5\u00bf\3\2\2\2\u00a6\u00a7\5\62\32\2\u00a7"+
		"\u00a8\7\2\2\3\u00a8\u00bf\3\2\2\2\u00a9\u00aa\58\35\2\u00aa\u00ab\7\2"+
		"\2\3\u00ab\u00bf\3\2\2\2\u00ac\u00ad\5\4\3\2\u00ad\u00ae\7\2\2\3\u00ae"+
		"\u00bf\3\2\2\2\u00af\u00b0\7\3\2\2\u00b0\u00b5\5V,\2\u00b1\u00b2\7\4\2"+
		"\2\u00b2\u00b4\5V,\2\u00b3\u00b1\3\2\2\2\u00b4\u00b7\3\2\2\2\u00b5\u00b3"+
		"\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00b9\3\2\2\2\u00b7\u00b5\3\2\2\2\u00b8"+
		"\u00af\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bb\5X"+
		"-\2\u00bb\u00bc\3\2\2\2\u00bc\u00bd\7\2\2\3\u00bd\u00bf\3\2\2\2\u00be"+
		"\u008e\3\2\2\2\u00be\u0091\3\2\2\2\u00be\u0094\3\2\2\2\u00be\u0097\3\2"+
		"\2\2\u00be\u009a\3\2\2\2\u00be\u009d\3\2\2\2\u00be\u00a0\3\2\2\2\u00be"+
		"\u00a3\3\2\2\2\u00be\u00a6\3\2\2\2\u00be\u00a9\3\2\2\2\u00be\u00ac\3\2"+
		"\2\2\u00be\u00b8\3\2\2\2\u00bf\3\3\2\2\2\u00c0\u00c1\7\5\2\2\u00c1\u00c2"+
		"\7\6\2\2\u00c2\u00c3\5x=\2\u00c3\5\3\2\2\2\u00c4\u00c5\7\7\2\2\u00c5\u00c6"+
		"\7\b\2\2\u00c6\u00d4\5x=\2\u00c7\u00c9\7\t\2\2\u00c8\u00c7\3\2\2\2\u00c8"+
		"\u00c9\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00d5\5X-\2\u00cb\u00cc\7\n\2"+
		"\2\u00cc\u00d1\5\b\5\2\u00cd\u00ce\7\4\2\2\u00ce\u00d0\5\b\5\2\u00cf\u00cd"+
		"\3\2\2\2\u00d0\u00d3\3\2\2\2\u00d1\u00cf\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2"+
		"\u00d5\3\2\2\2\u00d3\u00d1\3\2\2\2\u00d4\u00c8\3\2\2\2\u00d4\u00cb\3\2"+
		"\2\2\u00d5\7\3\2\2\2\u00d6\u00d7\7\13\2\2\u00d7\u00dc\5\u0086D\2\u00d8"+
		"\u00d9\7\4\2\2\u00d9\u00db\5\u0086D\2\u00da\u00d8\3\2\2\2\u00db\u00de"+
		"\3\2\2\2\u00dc\u00da\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00df\3\2\2\2\u00de"+
		"\u00dc\3\2\2\2\u00df\u00e0\7\f\2\2\u00e0\t\3\2\2\2\u00e1\u00e2\7\r\2\2"+
		"\u00e2\u00e4\5x=\2\u00e3\u00e5\5\f\7\2\u00e4\u00e3\3\2\2\2\u00e5\u00e6"+
		"\3\2\2\2\u00e6\u00e4\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7\13\3\2\2\2\u00e8"+
		"\u00eb\7\16\2\2\u00e9\u00ec\5z>\2\u00ea\u00ec\5T+\2\u00eb\u00e9\3\2\2"+
		"\2\u00eb\u00ea\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\7=\2\2\u00ee\u00f0"+
		"\5\u0086D\2\u00ef\u00f1\5j\66\2\u00f0\u00ef\3\2\2\2\u00f0\u00f1\3\2\2"+
		"\2\u00f1\r\3\2\2\2\u00f2\u00f3\7\17\2\2\u00f3\u00f4\7\t\2\2\u00f4\u00f6"+
		"\5x=\2\u00f5\u00f7\5j\66\2\u00f6\u00f5\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7"+
		"\17\3\2\2\2\u00f8\u00fa\7\20\2\2\u00f9\u00fb\7H\2\2\u00fa\u00f9\3\2\2"+
		"\2\u00fa\u00fb\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\u00fd\7\21\2\2\u00fd"+
		"\u00fe\5x=\2\u00fe\u00ff\7\13\2\2\u00ff\u0104\5@!\2\u0100\u0101\7\4\2"+
		"\2\u0101\u0103\5@!\2\u0102\u0100\3\2\2\2\u0103\u0106\3\2\2\2\u0104\u0102"+
		"\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0109\3\2\2\2\u0106\u0104\3\2\2\2\u0107"+
		"\u0108\7\4\2\2\u0108\u010a\5B\"\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2"+
		"\2\2\u010a\u010b\3\2\2\2\u010b\u010d\7\f\2\2\u010c\u010e\5\24\13\2\u010d"+
		"\u010c\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0110\3\2\2\2\u010f\u0111\5\22"+
		"\n\2\u0110\u010f\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0113\3\2\2\2\u0112"+
		"\u0114\5\26\f\2\u0113\u0112\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u0115\3"+
		"\2\2\2\u0115\u0116\5\36\20\2\u0116\u0117\5,\27\2\u0117\21\3\2\2\2\u0118"+
		"\u0119\7V\2\2\u0119\u011a\7\13\2\2\u011a\u011f\7J\2\2\u011b\u011c\7\4"+
		"\2\2\u011c\u011e\7J\2\2\u011d\u011b\3\2\2\2\u011e\u0121\3\2\2\2\u011f"+
		"\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120\u0122\3\2\2\2\u0121\u011f\3\2"+
		"\2\2\u0122\u0123\7\f\2\2\u0123\23\3\2\2\2\u0124\u0125\7U\2\2\u0125\u0126"+
		"\7\13\2\2\u0126\u012b\7J\2\2\u0127\u0128\7\4\2\2\u0128\u012a\7J\2\2\u0129"+
		"\u0127\3\2\2\2\u012a\u012d\3\2\2\2\u012b\u0129\3\2\2\2\u012b\u012c\3\2"+
		"\2\2\u012c\u012e\3\2\2\2\u012d\u012b\3\2\2\2\u012e\u012f\7\f\2\2\u012f"+
		"\25\3\2\2\2\u0130\u0133\7O\2\2\u0131\u0133\5\30\r\2\u0132\u0130\3\2\2"+
		"\2\u0132\u0131\3\2\2\2\u0133\27\3\2\2\2\u0134\u0135\7\22\2\2\u0135\u013a"+
		"\5\32\16\2\u0136\u0137\7\23\2\2\u0137\u0139\5\32\16\2\u0138\u0136\3\2"+
		"\2\2\u0139\u013c\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b"+
		"\u013d\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u0143\7\24\2\2\u013e\u0141\7"+
		"\4\2\2\u013f\u0142\5$\23\2\u0140\u0142\5(\25\2\u0141\u013f\3\2\2\2\u0141"+
		"\u0140\3\2\2\2\u0142\u0144\3\2\2\2\u0143\u013e\3\2\2\2\u0143\u0144\3\2"+
		"\2\2\u0144\31\3\2\2\2\u0145\u0146\7\22\2\2\u0146\u014b\7J\2\2\u0147\u0148"+
		"\7\23\2\2\u0148\u014a\7J\2\2\u0149\u0147\3\2\2\2\u014a\u014d\3\2\2\2\u014b"+
		"\u0149\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014e\3\2\2\2\u014d\u014b\3\2"+
		"\2\2\u014e\u014f\7\24\2\2\u014f\33\3\2\2\2\u0150\u0152\13\2\2\2\u0151"+
		"\u0150\3\2\2\2\u0152\u0155\3\2\2\2\u0153\u0154\3\2\2\2\u0153\u0151\3\2"+
		"\2\2\u0154\35\3\2\2\2\u0155\u0153\3\2\2\2\u0156\u0159\7Q\2\2\u0157\u0159"+
		"\5 \21\2\u0158\u0156\3\2\2\2\u0158\u0157\3\2\2\2\u0159\37\3\2\2\2\u015a"+
		"\u015d\7P\2\2\u015b\u015d\5\"\22\2\u015c\u015a\3\2\2\2\u015c\u015b\3\2"+
		"\2\2\u015d\u0163\3\2\2\2\u015e\u0161\7\4\2\2\u015f\u0162\5$\23\2\u0160"+
		"\u0162\5(\25\2\u0161\u015f\3\2\2\2\u0161\u0160\3\2\2\2\u0162\u0164\3\2"+
		"\2\2\u0163\u015e\3\2\2\2\u0163\u0164\3\2\2\2\u0164!\3\2\2\2\u0165\u0166"+
		"\7\22\2\2\u0166\u016b\7J\2\2\u0167\u0168\7\23\2\2\u0168\u016a\7J\2\2\u0169"+
		"\u0167\3\2\2\2\u016a\u016d\3\2\2\2\u016b\u0169\3\2\2\2\u016b\u016c\3\2"+
		"\2\2\u016c\u016e\3\2\2\2\u016d\u016b\3\2\2\2\u016e\u016f\7\24\2\2\u016f"+
		"#\3\2\2\2\u0170\u0171\7R\2\2\u0171\u0172\7\4\2\2\u0172\u0173\5&\24\2\u0173"+
		"%\3\2\2\2\u0174\u0175\7\22\2\2\u0175\u017a\5z>\2\u0176\u0177\7\23\2\2"+
		"\u0177\u0179\5z>\2\u0178\u0176\3\2\2\2\u0179\u017c\3\2\2\2\u017a\u0178"+
		"\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u017d\3\2\2\2\u017c\u017a\3\2\2\2\u017d"+
		"\u017e\7\24\2\2\u017e\'\3\2\2\2\u017f\u0180\7S\2\2\u0180\u0181\7\4\2\2"+
		"\u0181\u0182\5z>\2\u0182\u0183\7\4\2\2\u0183\u0184\5*\26\2\u0184)\3\2"+
		"\2\2\u0185\u0186\7\22\2\2\u0186\u018b\5\34\17\2\u0187\u0188\7\23\2\2\u0188"+
		"\u018a\5\34\17\2\u0189\u0187\3\2\2\2\u018a\u018d\3\2\2\2\u018b\u0189\3"+
		"\2\2\2\u018b\u018c\3\2\2\2\u018c\u018e\3\2\2\2\u018d\u018b\3\2\2\2\u018e"+
		"\u018f\7\24\2\2\u018f+\3\2\2\2\u0190\u0193\7P\2\2\u0191\u0193\5\"\22\2"+
		"\u0192\u0190\3\2\2\2\u0192\u0191\3\2\2\2\u0193\u0199\3\2\2\2\u0194\u0197"+
		"\7\4\2\2\u0195\u0198\5$\23\2\u0196\u0198\5\34\17\2\u0197\u0195\3\2\2\2"+
		"\u0197\u0196\3\2\2\2\u0198\u019a\3\2\2\2\u0199\u0194\3\2\2\2\u0199\u019a"+
		"\3\2\2\2\u019a-\3\2\2\2\u019b\u019c\7\25\2\2\u019c\u019d\7\21\2\2\u019d"+
		"\u019e\5x=\2\u019e/\3\2\2\2\u019f\u01a0\7\20\2\2\u01a0\u01a1\7\26\2\2"+
		"\u01a1\u01a2\5x=\2\u01a2\u01a3\7\27\2\2\u01a3\u01a4\5X-\2\u01a4\61\3\2"+
		"\2\2\u01a5\u01a6\7\25\2\2\u01a6\u01a7\7\26\2\2\u01a7\u01a8\5x=\2\u01a8"+
		"\63\3\2\2\2\u01a9\u01ab\7\20\2\2\u01aa\u01ac\7L\2\2\u01ab\u01aa\3\2\2"+
		"\2\u01ab\u01ac\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01ae\7\30\2\2\u01ae"+
		"\u01af\5x=\2\u01af\u01b0\7\6\2\2\u01b0\u01b1\5x=\2\u01b1\u01b2\7\13\2"+
		"\2\u01b2\u01b7\5> \2\u01b3\u01b4\7\4\2\2\u01b4\u01b6\5> \2\u01b5\u01b3"+
		"\3\2\2\2\u01b6\u01b9\3\2\2\2\u01b7\u01b5\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8"+
		"\u01ba\3\2\2\2\u01b9\u01b7\3\2\2\2\u01ba\u01bb\7\f\2\2\u01bb\65\3\2\2"+
		"\2\u01bc\u01bd\7\25\2\2\u01bd\u01be\7\30\2\2\u01be\u01bf\5x=\2\u01bf\67"+
		"\3\2\2\2\u01c0\u01c1\7\31\2\2\u01c1\u01c2\t\2\2\2\u01c2\u01c3\7\b\2\2"+
		"\u01c3\u01c6\5x=\2\u01c4\u01c5\7\32\2\2\u01c5\u01c7\5:\36\2\u01c6\u01c4"+
		"\3\2\2\2\u01c6\u01c7\3\2\2\2\u01c7\u01c8\3\2\2\2\u01c8\u01c9\7\t\2\2\u01c9"+
		"\u01ca\5<\37\2\u01ca9\3\2\2\2\u01cb\u01cc\13\2\2\2\u01cc;\3\2\2\2\u01cd"+
		"\u01cf\13\2\2\2\u01ce\u01cd\3\2\2\2\u01cf\u01d2\3\2\2\2\u01d0\u01ce\3"+
		"\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d3\3\2\2\2\u01d2\u01d0\3\2\2\2\u01d3"+
		"\u01d4\7\2\2\3\u01d4=\3\2\2\2\u01d5\u01d7\5z>\2\u01d6\u01d8\7D\2\2\u01d7"+
		"\u01d6\3\2\2\2\u01d7\u01d8\3\2\2\2\u01d8?\3\2\2\2\u01d9\u01da\5z>\2\u01da"+
		"\u01dc\5H%\2\u01db\u01dd\5D#\2\u01dc\u01db\3\2\2\2\u01dc\u01dd\3\2\2\2"+
		"\u01dd\u01df\3\2\2\2\u01de\u01e0\5F$\2\u01df\u01de\3\2\2\2\u01df\u01e0"+
		"\3\2\2\2\u01e0A\3\2\2\2\u01e1\u01e2\7\33\2\2\u01e2\u01e3\7\34\2\2\u01e3"+
		"\u01e4\7\13\2\2\u01e4\u01e9\5z>\2\u01e5\u01e6\7\4\2\2\u01e6\u01e8\5z>"+
		"\2\u01e7\u01e5\3\2\2\2\u01e8\u01eb\3\2\2\2\u01e9\u01e7\3\2\2\2\u01e9\u01ea"+
		"\3\2\2\2\u01ea\u01ec\3\2\2\2\u01eb\u01e9\3\2\2\2\u01ec\u01ed\7\f\2\2\u01ed"+
		"C\3\2\2\2\u01ee\u01ef\7B\2\2\u01ef\u01f0\7C\2\2\u01f0E\3\2\2\2\u01f1\u01f2"+
		"\7\33\2\2\u01f2\u01f3\7\34\2\2\u01f3G\3\2\2\2\u01f4\u01fa\5J&\2\u01f5"+
		"\u01fa\5L\'\2\u01f6\u01fa\5N(\2\u01f7\u01fa\5P)\2\u01f8\u01fa\5R*\2\u01f9"+
		"\u01f4\3\2\2\2\u01f9\u01f5\3\2\2\2\u01f9\u01f6\3\2\2\2\u01f9\u01f7\3\2"+
		"\2\2\u01f9\u01f8\3\2\2\2\u01faI\3\2\2\2\u01fb\u01fc\t\3\2\2\u01fc\u01fd"+
		"\7\13\2\2\u01fd\u01fe\7J\2\2\u01fe\u01ff\7\f\2\2\u01ffK\3\2\2\2\u0200"+
		"\u0201\7\37\2\2\u0201M\3\2\2\2\u0202\u0203\7 \2\2\u0203O\3\2\2\2\u0204"+
		"\u0205\7T\2\2\u0205Q\3\2\2\2\u0206\u0207\t\4\2\2\u0207S\3\2\2\2\u0208"+
		"\u0209\7\13\2\2\u0209\u020e\5z>\2\u020a\u020b\7\4\2\2\u020b\u020d\5z>"+
		"\2\u020c\u020a\3\2\2\2\u020d\u0210\3\2\2\2\u020e\u020c\3\2\2\2\u020e\u020f"+
		"\3\2\2\2\u020f\u0211\3\2\2\2\u0210\u020e\3\2\2\2\u0211\u0212\7\f\2\2\u0212"+
		"U\3\2\2\2\u0213\u021f\7W\2\2\u0214\u0215\7\13\2\2\u0215\u021a\5z>\2\u0216"+
		"\u0217\7\4\2\2\u0217\u0219\5z>\2\u0218\u0216\3\2\2\2\u0219\u021c\3\2\2"+
		"\2\u021a\u0218\3\2\2\2\u021a\u021b\3\2\2\2\u021b\u021d\3\2\2\2\u021c\u021a"+
		"\3\2\2\2\u021d\u021e\7\f\2\2\u021e\u0220\3\2\2\2\u021f\u0214\3\2\2\2\u021f"+
		"\u0220\3\2\2\2\u0220\u0221\3\2\2\2\u0221\u0222\7\27\2\2\u0222\u0223\7"+
		"\13\2\2\u0223\u0224\5X-\2\u0224\u0225\7\f\2\2\u0225W\3\2\2\2\u0226\u022c"+
		"\5\\/\2\u0227\u0228\7\13\2\2\u0228\u0229\5X-\2\u0229\u022a\7\f\2\2\u022a"+
		"\u022c\3\2\2\2\u022b\u0226\3\2\2\2\u022b\u0227\3\2\2\2\u022c\u0230\3\2"+
		"\2\2\u022d\u022f\5Z.\2\u022e\u022d\3\2\2\2\u022f\u0232\3\2\2\2\u0230\u022e"+
		"\3\2\2\2\u0230\u0231\3\2\2\2\u0231\u0234\3\2\2\2\u0232\u0230\3\2\2\2\u0233"+
		"\u0235\5p9\2\u0234\u0233\3\2\2\2\u0234\u0235\3\2\2\2\u0235\u0237\3\2\2"+
		"\2\u0236\u0238\5v<\2\u0237\u0236\3\2\2\2\u0237\u0238\3\2\2\2\u0238Y\3"+
		"\2\2\2\u0239\u023f\7G\2\2\u023a\u0240\5\\/\2\u023b\u023c\7\13\2\2\u023c"+
		"\u023d\5X-\2\u023d\u023e\7\f\2\2\u023e\u0240\3\2\2\2\u023f\u023a\3\2\2"+
		"\2\u023f\u023b\3\2\2\2\u0240[\3\2\2\2\u0241\u0242\5^\60\2\u0242\u0244"+
		"\5d\63\2\u0243\u0245\5j\66\2\u0244\u0243\3\2\2\2\u0244\u0245\3\2\2\2\u0245"+
		"\u0247\3\2\2\2\u0246\u0248\5l\67\2\u0247\u0246\3\2\2\2\u0247\u0248\3\2"+
		"\2\2\u0248\u024a\3\2\2\2\u0249\u024b\5n8\2\u024a\u0249\3\2\2\2\u024a\u024b"+
		"\3\2\2\2\u024b\u024d\3\2\2\2\u024c\u024e\5p9\2\u024d\u024c\3\2\2\2\u024d"+
		"\u024e\3\2\2\2\u024e\u0250\3\2\2\2\u024f\u0251\5v<\2\u0250\u024f\3\2\2"+
		"\2\u0250\u0251\3\2\2\2\u0251]\3\2\2\2\u0252\u0254\7#\2\2\u0253\u0255\5"+
		"`\61\2\u0254\u0253\3\2\2\2\u0254\u0255\3\2\2\2\u0255\u025f\3\2\2\2\u0256"+
		"\u0260\79\2\2\u0257\u025c\5b\62\2\u0258\u0259\7\4\2\2\u0259\u025b\5b\62"+
		"\2\u025a\u0258\3\2\2\2\u025b\u025e\3\2\2\2\u025c\u025a\3\2\2\2\u025c\u025d"+
		"\3\2\2\2\u025d\u0260\3\2\2\2\u025e\u025c\3\2\2\2\u025f\u0256\3\2\2\2\u025f"+
		"\u0257\3\2\2\2\u0260_\3\2\2\2\u0261\u0262\t\5\2\2\u0262a\3\2\2\2\u0263"+
		"\u0265\5z>\2\u0264\u0266\7\27\2\2\u0265\u0264\3\2\2\2\u0265\u0266\3\2"+
		"\2\2\u0266\u0268\3\2\2\2\u0267\u0269\7W\2\2\u0268\u0267\3\2\2\2\u0268"+
		"\u0269\3\2\2\2\u0269\u0272\3\2\2\2\u026a\u026c\5\u0086D\2\u026b\u026d"+
		"\7\27\2\2\u026c\u026b\3\2\2\2\u026c\u026d\3\2\2\2\u026d\u026f\3\2\2\2"+
		"\u026e\u0270\7W\2\2\u026f\u026e\3\2\2\2\u026f\u0270\3\2\2\2\u0270\u0272"+
		"\3\2\2\2\u0271\u0263\3\2\2\2\u0271\u026a\3\2\2\2\u0272c\3\2\2\2\u0273"+
		"\u0274\7\t\2\2\u0274\u0279\5f\64\2\u0275\u0276\7\4\2\2\u0276\u0278\5f"+
		"\64\2\u0277\u0275\3\2\2\2\u0278\u027b\3\2\2\2\u0279\u0277\3\2\2\2\u0279"+
		"\u027a\3\2\2\2\u027ae\3\2\2\2\u027b\u0279\3\2\2\2\u027c\u027d\b\64\1\2"+
		"\u027d\u027e\7\13\2\2\u027e\u0280\5f\64\2\u027f\u0281\7E\2\2\u0280\u027f"+
		"\3\2\2\2\u0280\u0281\3\2\2\2\u0281\u0282\3\2\2\2\u0282\u0283\7$\2\2\u0283"+
		"\u0284\5f\64\2\u0284\u0285\7\6\2\2\u0285\u0286\5|?\2\u0286\u0288\7\f\2"+
		"\2\u0287\u0289\5t;\2\u0288\u0287\3\2\2\2\u0288\u0289\3\2\2\2\u0289\u029a"+
		"\3\2\2\2\u028a\u028b\7\13\2\2\u028b\u028c\5f\64\2\u028c\u028d\7F\2\2\u028d"+
		"\u028e\5f\64\2\u028e\u0290\7\f\2\2\u028f\u0291\5t;\2\u0290\u028f\3\2\2"+
		"\2\u0290\u0291\3\2\2\2\u0291\u029a\3\2\2\2\u0292\u0293\7\13\2\2\u0293"+
		"\u0294\5X-\2\u0294\u0296\7\f\2\2\u0295\u0297\5t;\2\u0296\u0295\3\2\2\2"+
		"\u0296\u0297\3\2\2\2\u0297\u029a\3\2\2\2\u0298\u029a\5h\65\2\u0299\u027c"+
		"\3\2\2\2\u0299\u028a\3\2\2\2\u0299\u0292\3\2\2\2\u0299\u0298\3\2\2\2\u029a"+
		"\u02ae\3\2\2\2\u029b\u029d\f\b\2\2\u029c\u029e\7E\2\2\u029d\u029c\3\2"+
		"\2\2\u029d\u029e\3\2\2\2\u029e\u029f\3\2\2\2\u029f\u02a0\7$\2\2\u02a0"+
		"\u02a1\5f\64\2\u02a1\u02a2\7\6\2\2\u02a2\u02a4\5|?\2\u02a3\u02a5\5t;\2"+
		"\u02a4\u02a3\3\2\2\2\u02a4\u02a5\3\2\2\2\u02a5\u02ad\3\2\2\2\u02a6\u02a7"+
		"\f\7\2\2\u02a7\u02a8\7F\2\2\u02a8\u02aa\5f\64\2\u02a9\u02ab\5t;\2\u02aa"+
		"\u02a9\3\2\2\2\u02aa\u02ab\3\2\2\2\u02ab\u02ad\3\2\2\2\u02ac\u029b\3\2"+
		"\2\2\u02ac\u02a6\3\2\2\2\u02ad\u02b0\3\2\2\2\u02ae\u02ac\3\2\2\2\u02ae"+
		"\u02af\3\2\2\2\u02afg\3\2\2\2\u02b0\u02ae\3\2\2\2\u02b1\u02b3\5x=\2\u02b2"+
		"\u02b4\5t;\2\u02b3\u02b2\3\2\2\2\u02b3\u02b4\3\2\2\2\u02b4i\3\2\2\2\u02b5"+
		"\u02b6\7%\2\2\u02b6\u02b7\5|?\2\u02b7k\3\2\2\2\u02b8\u02b9\7&\2\2\u02b9"+
		"\u02ba\7\'\2\2\u02ba\u02bf\5z>\2\u02bb\u02bc\7\4\2\2\u02bc\u02be\5z>\2"+
		"\u02bd\u02bb\3\2\2\2\u02be\u02c1\3\2\2\2\u02bf\u02bd\3\2\2\2\u02bf\u02c0"+
		"\3\2\2\2\u02c0m\3\2\2\2\u02c1\u02bf\3\2\2\2\u02c2\u02c3\7(\2\2\u02c3\u02c4"+
		"\5|?\2\u02c4o\3\2\2\2\u02c5\u02c6\7)\2\2\u02c6\u02c7\7\'\2\2\u02c7\u02cc"+
		"\5r:\2\u02c8\u02c9\7\4\2\2\u02c9\u02cb\5r:\2\u02ca\u02c8\3\2\2\2\u02cb"+
		"\u02ce\3\2\2\2\u02cc\u02ca\3\2\2\2\u02cc\u02cd\3\2\2\2\u02cdq\3\2\2\2"+
		"\u02ce\u02cc\3\2\2\2\u02cf\u02d1\7J\2\2\u02d0\u02d2\7D\2\2\u02d1\u02d0"+
		"\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d2\u02d8\3\2\2\2\u02d3\u02d5\5z>\2\u02d4"+
		"\u02d6\7D\2\2\u02d5\u02d4\3\2\2\2\u02d5\u02d6\3\2\2\2\u02d6\u02d8\3\2"+
		"\2\2\u02d7\u02cf\3\2\2\2\u02d7\u02d3\3\2\2\2\u02d8s\3\2\2\2\u02d9\u02db"+
		"\7\27\2\2\u02da\u02d9\3\2\2\2\u02da\u02db\3\2\2\2\u02db\u02dc\3\2\2\2"+
		"\u02dc\u02dd\7W\2\2\u02ddu\3\2\2\2\u02de\u02df\7*\2\2\u02df\u02e1\7+\2"+
		"\2\u02e0\u02e2\7J\2\2\u02e1\u02e0\3\2\2\2\u02e1\u02e2\3\2\2\2\u02e2\u02e3"+
		"\3\2\2\2\u02e3\u02e4\t\6\2\2\u02e4\u02e5\7.\2\2\u02e5w\3\2\2\2\u02e6\u02eb"+
		"\7W\2\2\u02e7\u02e8\7W\2\2\u02e8\u02e9\7/\2\2\u02e9\u02eb\7W\2\2\u02ea"+
		"\u02e6\3\2\2\2\u02ea\u02e7\3\2\2\2\u02eby\3\2\2\2\u02ec\u02f1\7W\2\2\u02ed"+
		"\u02ee\7W\2\2\u02ee\u02ef\7/\2\2\u02ef\u02f1\7W\2\2\u02f0\u02ec\3\2\2"+
		"\2\u02f0\u02ed\3\2\2\2\u02f1{\3\2\2\2\u02f2\u02f6\5\u0080A\2\u02f3\u02f5"+
		"\5~@\2\u02f4\u02f3\3\2\2\2\u02f5\u02f8\3\2\2\2\u02f6\u02f4\3\2\2\2\u02f6"+
		"\u02f7\3\2\2\2\u02f7}\3\2\2\2\u02f8\u02f6\3\2\2\2\u02f9\u02fa\t\7\2\2"+
		"\u02fa\u02fb\5\u0080A\2\u02fb\177\3\2\2\2\u02fc\u02fe\7B\2\2\u02fd\u02fc"+
		"\3\2\2\2\u02fd\u02fe\3\2\2\2\u02fe\u0304\3\2\2\2\u02ff\u0305\5\u0082B"+
		"\2\u0300\u0301\7\13\2\2\u0301\u0302\5|?\2\u0302\u0303\7\f\2\2\u0303\u0305"+
		"\3\2\2\2\u0304\u02ff\3\2\2\2\u0304\u0300\3\2\2\2\u0305\u0081\3\2\2\2\u0306"+
		"\u0307\5\u0086D\2\u0307\u0308\5\u0084C\2\u0308\u0309\5\u0086D\2\u0309"+
		"\u0313\3\2\2\2\u030a\u030b\5\u0086D\2\u030b\u030c\7?\2\2\u030c\u0313\3"+
		"\2\2\2\u030d\u030e\7\60\2\2\u030e\u030f\7\13\2\2\u030f\u0310\5\\/\2\u0310"+
		"\u0311\7\f\2\2\u0311\u0313\3\2\2\2\u0312\u0306\3\2\2\2\u0312\u030a\3\2"+
		"\2\2\u0312\u030d\3\2\2\2\u0313\u0083\3\2\2\2\u0314\u0315\t\b\2\2\u0315"+
		"\u0085\3\2\2\2\u0316\u0317\bD\1\2\u0317\u0318\5\u008aF\2\u0318\u0319\7"+
		"\13\2\2\u0319\u031e\5\u0086D\2\u031a\u031b\7\4\2\2\u031b\u031d\5\u0086"+
		"D\2\u031c\u031a\3\2\2\2\u031d\u0320\3\2\2\2\u031e\u031c\3\2\2\2\u031e"+
		"\u031f\3\2\2\2\u031f\u0321\3\2\2\2\u0320\u031e\3\2\2\2\u0321\u0322\7\f"+
		"\2\2\u0322\u0351\3\2\2\2\u0323\u0324\7:\2\2\u0324\u0325\7\13\2\2\u0325"+
		"\u0326\7I\2\2\u0326\u0327\5\u0086D\2\u0327\u0328\7\f\2\2\u0328\u0351\3"+
		"\2\2\2\u0329\u032a\7\13\2\2\u032a\u032d\5\u0086D\2\u032b\u032c\7\4\2\2"+
		"\u032c\u032e\5\u0086D\2\u032d\u032b\3\2\2\2\u032e\u032f\3\2\2\2\u032f"+
		"\u032d\3\2\2\2\u032f\u0330\3\2\2\2\u0330\u0331\3\2\2\2\u0331\u0332\7\f"+
		"\2\2\u0332\u0351\3\2\2\2\u0333\u0334\7:\2\2\u0334\u0335\7\13\2\2\u0335"+
		"\u0336\79\2\2\u0336\u0351\7\f\2\2\u0337\u0351\5\u008cG\2\u0338\u0351\5"+
		"z>\2\u0339\u033a\7\13\2\2\u033a\u033b\5\\/\2\u033b\u033c\7\f\2\2\u033c"+
		"\u0351\3\2\2\2\u033d\u033e\7\13\2\2\u033e\u033f\5\u0086D\2\u033f\u0340"+
		"\7\f\2\2\u0340\u0351\3\2\2\2\u0341\u0351\7C\2\2\u0342\u0343\7\63\2\2\u0343"+
		"\u0347\5\u0088E\2\u0344\u0346\5\u0088E\2\u0345\u0344\3\2\2\2\u0346\u0349"+
		"\3\2\2\2\u0347\u0345\3\2\2\2\u0347\u0348\3\2\2\2\u0348\u034a\3\2\2\2\u0349"+
		"\u0347\3\2\2\2\u034a\u034b\7\64\2\2\u034b\u034c\5\u0086D\2\u034c\u034e"+
		"\7\65\2\2\u034d\u034f\7\63\2\2\u034e\u034d\3\2\2\2\u034e\u034f\3\2\2\2"+
		"\u034f\u0351\3\2\2\2\u0350\u0316\3\2\2\2\u0350\u0323\3\2\2\2\u0350\u0329"+
		"\3\2\2\2\u0350\u0333\3\2\2\2\u0350\u0337\3\2\2\2\u0350\u0338\3\2\2\2\u0350"+
		"\u0339\3\2\2\2\u0350\u033d\3\2\2\2\u0350\u0341\3\2\2\2\u0350\u0342\3\2"+
		"\2\2\u0351\u035d\3\2\2\2\u0352\u0353\f\17\2\2\u0353\u0354\t\t\2\2\u0354"+
		"\u035c\5\u0086D\20\u0355\u0356\f\16\2\2\u0356\u0357\t\n\2\2\u0357\u035c"+
		"\5\u0086D\17\u0358\u0359\f\r\2\2\u0359\u035a\7;\2\2\u035a\u035c\5\u0086"+
		"D\16\u035b\u0352\3\2\2\2\u035b\u0355\3\2\2\2\u035b\u0358\3\2\2\2\u035c"+
		"\u035f\3\2\2\2\u035d\u035b\3\2\2\2\u035d\u035e\3\2\2\2\u035e\u0087\3\2"+
		"\2\2\u035f\u035d\3\2\2\2\u0360\u0361\7\66\2\2\u0361\u0362\5|?\2\u0362"+
		"\u0363\7\67\2\2\u0363\u0364\5\u0086D\2\u0364\u0089\3\2\2\2\u0365\u0366"+
		"\t\13\2\2\u0366\u008b\3\2\2\2\u0367\u0369\7<\2\2\u0368\u0367\3\2\2\2\u0368"+
		"\u0369\3\2\2\2\u0369\u036a\3\2\2\2\u036a\u036d\7J\2\2\u036b\u036c\7/\2"+
		"\2\u036c\u036e\7J\2\2\u036d\u036b\3\2\2\2\u036d\u036e\3\2\2\2\u036e\u0371"+
		"\3\2\2\2\u036f\u0371\78\2\2\u0370\u0368\3\2\2\2\u0370\u036f\3\2\2\2\u0371"+
		"\u008d\3\2\2\2f\u00b5\u00b8\u00be\u00c8\u00d1\u00d4\u00dc\u00e6\u00eb"+
		"\u00f0\u00f6\u00fa\u0104\u0109\u010d\u0110\u0113\u011f\u012b\u0132\u013a"+
		"\u0141\u0143\u014b\u0153\u0158\u015c\u0161\u0163\u016b\u017a\u018b\u0192"+
		"\u0197\u0199\u01ab\u01b7\u01c6\u01d0\u01d7\u01dc\u01df\u01e9\u01f9\u020e"+
		"\u021a\u021f\u022b\u0230\u0234\u0237\u023f\u0244\u0247\u024a\u024d\u0250"+
		"\u0254\u025c\u025f\u0265\u0268\u026c\u026f\u0271\u0279\u0280\u0288\u0290"+
		"\u0296\u0299\u029d\u02a4\u02aa\u02ac\u02ae\u02b3\u02bf\u02cc\u02d1\u02d5"+
		"\u02d7\u02da\u02e1\u02ea\u02f0\u02f6\u02fd\u0304\u0312\u031e\u032f\u0347"+
		"\u034e\u0350\u035b\u035d\u0368\u036d\u0370";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}