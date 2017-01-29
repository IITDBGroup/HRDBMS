// Generated from /home/michael/ms-project/Bitbucket/hrdbms/HRDBMS/src/com/exascale/optimizer/Select.g4 by ANTLR 4.6
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
	static { RuntimeMetaData.checkVersion("4.6", RuntimeMetaData.VERSION); }

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
		T__52=53, T__53=54, T__54=55, T__55=56, T__56=57, T__57=58, T__58=59, 
		T__59=60, T__60=61, T__61=62, T__62=63, T__63=64, T__64=65, T__65=66, 
		STRING=67, STAR=68, COUNT=69, CONCAT=70, NEGATIVE=71, EQUALS=72, OPERATOR=73, 
		NULLOPERATOR=74, AND=75, OR=76, NOT=77, NULL=78, DIRECTION=79, JOINTYPE=80, 
		CROSSJOIN=81, TABLECOMBINATION=82, COLUMN=83, DISTINCT=84, INTEGER=85, 
		WS=86, UNIQUE=87, REPLACE=88, RESUME=89, NONE=90, ALL=91, ANYTEXT=92, 
		HASH=93, RANGE=94, DATE=95, COLORDER=96, ORGANIZATION=97, IDENTIFIER=98, 
		JAVACLASSNAMEIDENTIFIER=99, FILEPATHIDENTIFIER=100, ANY=101;
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

	private static final String[] _LITERAL_NAMES = {
		null, "'WITH'", "','", "'RUNSTATS'", "'ON'", "'INSERT'", "'INTO'", "'FROM'", 
		"'VALUES'", "'('", "')'", "'UPDATE'", "'SET'", "'DELETE'", "'CREATE'", 
		"'TABLE'", "'EXTERNAL'", "'IMPORT'", "'FIELDS'", "'DELIMITED'", "'BY'", 
		"'ROWS'", "'FILE'", "'PATH'", "'USING'", "'PARAMETERS'", "'.'", "'.java'", 
		"':'", "'LOCAL'", "'HDFS'", "'S3'", "'{'", "'|'", "'}'", "'DROP'", "'VIEW'", 
		"'AS'", "'INDEX'", "'LOAD'", "'DELIMITER'", "'PRIMARY'", "'KEY'", "'CHAR'", 
		"'VARCHAR'", "'INTEGER'", "'BIGINT'", "'FLOAT'", "'DOUBLE'", "'SELECT'", 
		"'JOIN'", "'WHERE'", "'GROUP'", "'HAVING'", "'ORDER'", "'FETCH'", "'FIRST'", 
		"'ROW'", "'ONLY'", "'EXISTS'", "'/'", "'+'", "'CASE'", "'ELSE'", "'END'", 
		"'WHEN'", "'THEN'", null, "'*'", "'COUNT'", "'||'", "'-'", "'='", null, 
		null, "'AND'", "'OR'", "'NOT'", "'NULL'", null, null, "'CROSS JOIN'", 
		null, "'COLUMN'", "'DISTINCT'", null, null, "'UNIQUE'", "'REPLACE'", "'RESUME'", 
		"'NONE'", "'ALL'", "'ANY'", "'HASH'", "'RANGE'", "'DATE'", "'COLORDER'", 
		"'ORGANIZATION'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, "STRING", "STAR", "COUNT", "CONCAT", 
		"NEGATIVE", "EQUALS", "OPERATOR", "NULLOPERATOR", "AND", "OR", "NOT", 
		"NULL", "DIRECTION", "JOINTYPE", "CROSSJOIN", "TABLECOMBINATION", "COLUMN", 
		"DISTINCT", "INTEGER", "WS", "UNIQUE", "REPLACE", "RESUME", "NONE", "ALL", 
		"ANYTEXT", "HASH", "RANGE", "DATE", "COLORDER", "ORGANIZATION", "IDENTIFIER", 
		"JAVACLASSNAMEIDENTIFIER", "FILEPATHIDENTIFIER", "ANY"
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
		public CreateExternalTableContext createExternalTable() {
			return getRuleContext(CreateExternalTableContext.class,0);
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
			setState(201);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(150);
				insert();
				setState(151);
				match(EOF);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(153);
				update();
				setState(154);
				match(EOF);
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(156);
				delete();
				setState(157);
				match(EOF);
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(159);
				createExternalTable();
				setState(160);
				match(EOF);
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(162);
				createTable();
				setState(163);
				match(EOF);
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(165);
				createIndex();
				setState(166);
				match(EOF);
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				{
				setState(168);
				createView();
				setState(169);
				match(EOF);
				}
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				{
				setState(171);
				dropTable();
				setState(172);
				match(EOF);
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				{
				setState(174);
				dropIndex();
				setState(175);
				match(EOF);
				}
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				{
				setState(177);
				dropView();
				setState(178);
				match(EOF);
				}
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				{
				setState(180);
				load();
				setState(181);
				match(EOF);
				}
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				{
				setState(183);
				runstats();
				setState(184);
				match(EOF);
				}
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				{
				{
				setState(195);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(186);
					match(T__0);
					setState(187);
					commonTableExpression();
					setState(192);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__1) {
						{
						{
						setState(188);
						match(T__1);
						setState(189);
						commonTableExpression();
						}
						}
						setState(194);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(197);
				fullSelect();
				}
				setState(199);
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
			setState(203);
			match(T__2);
			setState(204);
			match(T__3);
			setState(205);
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
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
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
			setState(207);
			match(T__4);
			setState(208);
			match(T__5);
			setState(209);
			tableName();
			setState(226);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__6:
			case T__8:
			case T__48:
				{
				{
				setState(211);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__6) {
					{
					setState(210);
					match(T__6);
					}
				}

				setState(213);
				fullSelect();
				}
				}
				break;
			case T__7:
				{
				{
				setState(214);
				match(T__7);
				setState(215);
				match(T__8);
				setState(216);
				expression(0);
				setState(221);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(217);
					match(T__1);
					setState(218);
					expression(0);
					}
					}
					setState(223);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(224);
				match(T__9);
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
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
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
			setState(228);
			match(T__10);
			setState(229);
			tableName();
			setState(230);
			match(T__11);
			setState(233);
			_errHandler.sync(this);
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
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__50) {
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
			setState(240);
			match(T__12);
			setState(241);
			match(T__6);
			setState(242);
			tableName();
			setState(244);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__50) {
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
			setState(246);
			match(T__13);
			setState(248);
			_errHandler.sync(this);
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
			_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
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
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			}
			setState(263);
			_errHandler.sync(this);
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
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COLORDER) {
				{
				setState(266);
				colOrder();
				}
			}

			setState(270);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORGANIZATION) {
				{
				setState(269);
				organization();
				}
			}

			setState(273);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
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

	public static class CreateExternalTableContext extends ParserRuleContext {
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
		public JavaClassExtTableSpecContext javaClassExtTableSpec() {
			return getRuleContext(JavaClassExtTableSpecContext.class,0);
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
			setState(290);
			match(T__13);
			setState(291);
			match(T__15);
			setState(292);
			match(T__14);
			setState(293);
			tableName();
			setState(294);
			match(T__8);
			setState(295);
			colDef();
			setState(300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(296);
				match(T__1);
				setState(297);
				colDef();
				}
				}
				setState(302);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(303);
			match(T__9);
			setState(306);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__16:
				{
				setState(304);
				generalExtTableSpec();
				}
				break;
			case T__23:
				{
				setState(305);
				javaClassExtTableSpec();
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
		public SourceListContext sourceList() {
			return getRuleContext(SourceListContext.class,0);
		}
		public List<AnythingContext> anything() {
			return getRuleContexts(AnythingContext.class);
		}
		public AnythingContext anything(int i) {
			return getRuleContext(AnythingContext.class,i);
		}
		public TerminalNode FILEPATHIDENTIFIER() { return getToken(SelectParser.FILEPATHIDENTIFIER, 0); }
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
			setState(308);
			match(T__16);
			setState(309);
			match(T__6);
			setState(310);
			sourceList();
			setState(311);
			match(T__17);
			setState(312);
			match(T__18);
			setState(313);
			match(T__19);
			setState(314);
			anything();
			setState(315);
			match(T__20);
			setState(316);
			match(T__18);
			setState(317);
			match(T__19);
			setState(318);
			anything();
			setState(319);
			match(T__21);
			setState(320);
			match(T__22);
			setState(321);
			match(FILEPATHIDENTIFIER);
			}
		}
		catch (RecognitionException re) {
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
		public JavaClassNameContext javaClassName() {
			return getRuleContext(JavaClassNameContext.class,0);
		}
		public KeyValueListContext keyValueList() {
			return getRuleContext(KeyValueListContext.class,0);
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
			setState(323);
			match(T__23);
			setState(324);
			javaClassName();
			setState(325);
			match(T__0);
			setState(326);
			match(T__24);
			setState(327);
			match(T__8);
			setState(328);
			keyValueList();
			setState(329);
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
			setState(331);
			match(JAVACLASSNAMEIDENTIFIER);
			setState(336);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__25) {
				{
				{
				setState(332);
				match(T__25);
				setState(333);
				match(JAVACLASSNAMEIDENTIFIER);
				}
				}
				setState(338);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(339);
			match(T__26);
			}
		}
		catch (RecognitionException re) {
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
			setState(341);
			anything();
			setState(342);
			match(T__27);
			setState(343);
			anything();
			setState(351);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(344);
				match(T__1);
				setState(345);
				anything();
				setState(346);
				match(T__27);
				setState(347);
				anything();
				}
				}
				setState(353);
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
			setState(354);
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
			setState(356);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__28) | (1L << T__29) | (1L << T__30))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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
			setState(358);
			match(COLORDER);
			setState(359);
			match(T__8);
			setState(360);
			match(INTEGER);
			setState(365);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(361);
				match(T__1);
				setState(362);
				match(INTEGER);
				}
				}
				setState(367);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(368);
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
			setState(372);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(370);
				match(NONE);
				}
				break;
			case T__31:
				enterOuterAlt(_localctx, 2);
				{
				setState(371);
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
			setState(374);
			match(T__31);
			setState(375);
			groupDef();
			setState(380);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__32) {
				{
				{
				setState(376);
				match(T__32);
				setState(377);
				groupDef();
				}
				}
				setState(382);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(383);
			match(T__33);
			setState(389);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(384);
				match(T__1);
				setState(387);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(385);
					hashExp();
					}
					break;
				case RANGE:
					{
					setState(386);
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
			setState(391);
			match(T__31);
			setState(392);
			match(INTEGER);
			setState(397);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__32) {
				{
				{
				setState(393);
				match(T__32);
				setState(394);
				match(INTEGER);
				}
				}
				setState(399);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(400);
			match(T__33);
			}
		}
		catch (RecognitionException re) {
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
			setState(405);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,26,_ctx);
			while ( _alt!=1 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(402);
					matchWildcard();
					}
					} 
				}
				setState(407);
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
			setState(410);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ANYTEXT:
				enterOuterAlt(_localctx, 1);
				{
				setState(408);
				match(ANYTEXT);
				}
				break;
			case T__31:
			case ALL:
				enterOuterAlt(_localctx, 2);
				{
				setState(409);
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
			setState(414);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(412);
				match(ALL);
				}
				break;
			case T__31:
				{
				setState(413);
				integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(421);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(416);
				match(T__1);
				setState(419);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case HASH:
					{
					setState(417);
					hashExp();
					}
					break;
				case RANGE:
					{
					setState(418);
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
			setState(423);
			match(T__31);
			setState(424);
			match(INTEGER);
			setState(429);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__32) {
				{
				{
				setState(425);
				match(T__32);
				setState(426);
				match(INTEGER);
				}
				}
				setState(431);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(432);
			match(T__33);
			}
		}
		catch (RecognitionException re) {
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
			setState(434);
			match(HASH);
			setState(435);
			match(T__1);
			setState(436);
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
			setState(438);
			match(T__31);
			setState(439);
			columnName();
			setState(444);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__32) {
				{
				{
				setState(440);
				match(T__32);
				setState(441);
				columnName();
				}
				}
				setState(446);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(447);
			match(T__33);
			}
		}
		catch (RecognitionException re) {
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
			setState(449);
			match(RANGE);
			setState(450);
			match(T__1);
			setState(451);
			columnName();
			setState(452);
			match(T__1);
			setState(453);
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
			setState(455);
			match(T__31);
			setState(456);
			rangeExp();
			setState(461);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__32) {
				{
				{
				setState(457);
				match(T__32);
				setState(458);
				rangeExp();
				}
				}
				setState(463);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(464);
			match(T__33);
			}
		}
		catch (RecognitionException re) {
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
			setState(468);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALL:
				{
				setState(466);
				match(ALL);
				}
				break;
			case T__31:
				{
				setState(467);
				integerSet();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(475);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(470);
				match(T__1);
				setState(473);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
				case 1:
					{
					setState(471);
					hashExp();
					}
					break;
				case 2:
					{
					setState(472);
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
			setState(477);
			match(T__34);
			setState(478);
			match(T__14);
			setState(479);
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
			setState(481);
			match(T__13);
			setState(482);
			match(T__35);
			setState(483);
			tableName();
			setState(484);
			match(T__36);
			setState(485);
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
			setState(487);
			match(T__34);
			setState(488);
			match(T__35);
			setState(489);
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
			setState(491);
			match(T__13);
			setState(493);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==UNIQUE) {
				{
				setState(492);
				match(UNIQUE);
				}
			}

			setState(495);
			match(T__37);
			setState(496);
			tableName();
			setState(497);
			match(T__3);
			setState(498);
			tableName();
			setState(499);
			match(T__8);
			setState(500);
			indexDef();
			setState(505);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(501);
				match(T__1);
				setState(502);
				indexDef();
				}
				}
				setState(507);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
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
			setState(510);
			match(T__34);
			setState(511);
			match(T__37);
			setState(512);
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
			setState(514);
			match(T__38);
			setState(515);
			_la = _input.LA(1);
			if ( !(_la==REPLACE || _la==RESUME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(516);
			match(T__5);
			setState(517);
			tableName();
			setState(520);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__39) {
				{
				setState(518);
				match(T__39);
				setState(519);
				any();
				}
			}

			setState(522);
			match(T__6);
			setState(523);
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
			setState(525);
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
			setState(530);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << T__29) | (1L << T__30) | (1L << T__31) | (1L << T__32) | (1L << T__33) | (1L << T__34) | (1L << T__35) | (1L << T__36) | (1L << T__37) | (1L << T__38) | (1L << T__39) | (1L << T__40) | (1L << T__41) | (1L << T__42) | (1L << T__43) | (1L << T__44) | (1L << T__45) | (1L << T__46) | (1L << T__47) | (1L << T__48) | (1L << T__49) | (1L << T__50) | (1L << T__51) | (1L << T__52) | (1L << T__53) | (1L << T__54) | (1L << T__55) | (1L << T__56) | (1L << T__57) | (1L << T__58) | (1L << T__59) | (1L << T__60) | (1L << T__61) | (1L << T__62))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (T__63 - 64)) | (1L << (T__64 - 64)) | (1L << (T__65 - 64)) | (1L << (STRING - 64)) | (1L << (STAR - 64)) | (1L << (COUNT - 64)) | (1L << (CONCAT - 64)) | (1L << (NEGATIVE - 64)) | (1L << (EQUALS - 64)) | (1L << (OPERATOR - 64)) | (1L << (NULLOPERATOR - 64)) | (1L << (AND - 64)) | (1L << (OR - 64)) | (1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (COLUMN - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (COLORDER - 64)) | (1L << (ORGANIZATION - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (JAVACLASSNAMEIDENTIFIER - 64)) | (1L << (FILEPATHIDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0)) {
				{
				{
				setState(527);
				matchWildcard();
				}
				}
				setState(532);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(533);
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
			setState(535);
			columnName();
			setState(537);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DIRECTION) {
				{
				setState(536);
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
			setState(539);
			columnName();
			setState(540);
			dataType();
			setState(542);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(541);
				notNull();
				}
			}

			setState(545);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__40) {
				{
				setState(544);
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
			setState(547);
			match(T__40);
			setState(548);
			match(T__41);
			setState(549);
			match(T__8);
			setState(550);
			columnName();
			setState(555);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(551);
				match(T__1);
				setState(552);
				columnName();
				}
				}
				setState(557);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(558);
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
			setState(560);
			match(NOT);
			setState(561);
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
			setState(563);
			match(T__40);
			setState(564);
			match(T__41);
			}
		}
		catch (RecognitionException re) {
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
			setState(571);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__42:
			case T__43:
				enterOuterAlt(_localctx, 1);
				{
				setState(566);
				char2();
				}
				break;
			case T__44:
				enterOuterAlt(_localctx, 2);
				{
				setState(567);
				int2();
				}
				break;
			case T__45:
				enterOuterAlt(_localctx, 3);
				{
				setState(568);
				long2();
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 4);
				{
				setState(569);
				date2();
				}
				break;
			case T__46:
			case T__47:
				enterOuterAlt(_localctx, 5);
				{
				setState(570);
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
			setState(573);
			_la = _input.LA(1);
			if ( !(_la==T__42 || _la==T__43) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(574);
			match(T__8);
			setState(575);
			match(INTEGER);
			setState(576);
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
			setState(578);
			match(T__44);
			}
		}
		catch (RecognitionException re) {
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
			setState(580);
			match(T__45);
			}
		}
		catch (RecognitionException re) {
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
			setState(582);
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
			setState(584);
			_la = _input.LA(1);
			if ( !(_la==T__46 || _la==T__47) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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
			setState(586);
			match(T__8);
			setState(587);
			columnName();
			setState(592);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(588);
				match(T__1);
				setState(589);
				columnName();
				}
				}
				setState(594);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(595);
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
			setState(597);
			match(IDENTIFIER);
			setState(609);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__8) {
				{
				setState(598);
				match(T__8);
				setState(599);
				columnName();
				setState(604);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(600);
					match(T__1);
					setState(601);
					columnName();
					}
					}
					setState(606);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(607);
				match(T__9);
				}
			}

			setState(611);
			match(T__36);
			setState(612);
			match(T__8);
			setState(613);
			fullSelect();
			setState(614);
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
			setState(621);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__48:
				{
				setState(616);
				subSelect();
				}
				break;
			case T__8:
				{
				setState(617);
				match(T__8);
				setState(618);
				fullSelect();
				setState(619);
				match(T__9);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(626);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==TABLECOMBINATION) {
				{
				{
				setState(623);
				connectedSelect();
				}
				}
				setState(628);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(630);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__53) {
				{
				setState(629);
				orderBy();
				}
			}

			setState(633);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__54) {
				{
				setState(632);
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
			setState(635);
			match(TABLECOMBINATION);
			setState(641);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__48:
				{
				setState(636);
				subSelect();
				}
				break;
			case T__8:
				{
				setState(637);
				match(T__8);
				setState(638);
				fullSelect();
				setState(639);
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
			setState(643);
			selectClause();
			setState(644);
			fromClause();
			setState(646);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__50) {
				{
				setState(645);
				whereClause();
				}
			}

			setState(649);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__51) {
				{
				setState(648);
				groupBy();
				}
			}

			setState(652);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__52) {
				{
				setState(651);
				havingClause();
				}
			}

			setState(655);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
			case 1:
				{
				setState(654);
				orderBy();
				}
				break;
			}
			setState(658);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
			case 1:
				{
				setState(657);
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
			setState(660);
			match(T__48);
			setState(662);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==ALL) {
				{
				setState(661);
				selecthow();
				}
			}

			setState(673);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(664);
				match(STAR);
				}
				break;
			case T__8:
			case T__61:
			case STRING:
			case COUNT:
			case NEGATIVE:
			case NULL:
			case INTEGER:
			case DATE:
			case IDENTIFIER:
				{
				{
				setState(665);
				selectListEntry();
				setState(670);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(666);
					match(T__1);
					setState(667);
					selectListEntry();
					}
					}
					setState(672);
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
		enterRule(_localctx, 104, RULE_selecthow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(675);
			_la = _input.LA(1);
			if ( !(_la==DISTINCT || _la==ALL) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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
			setState(691);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
			case 1:
				_localctx = new SelectColumnContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(677);
				columnName();
				setState(679);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__36) {
					{
					setState(678);
					match(T__36);
					}
				}

				setState(682);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(681);
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
				setState(684);
				expression(0);
				setState(686);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__36) {
					{
					setState(685);
					match(T__36);
					}
				}

				setState(689);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(688);
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
			setState(693);
			match(T__6);
			setState(694);
			tableReference(0);
			setState(699);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(695);
				match(T__1);
				setState(696);
				tableReference(0);
				}
				}
				setState(701);
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
			setState(731);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				{
				_localctx = new JoinPContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(703);
				match(T__8);
				setState(704);
				tableReference(0);
				setState(706);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==JOINTYPE) {
					{
					setState(705);
					match(JOINTYPE);
					}
				}

				setState(708);
				match(T__49);
				setState(709);
				tableReference(0);
				setState(710);
				match(T__3);
				setState(711);
				searchCondition();
				setState(712);
				match(T__9);
				setState(714);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
				case 1:
					{
					setState(713);
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
				setState(716);
				match(T__8);
				setState(717);
				tableReference(0);
				setState(718);
				match(CROSSJOIN);
				setState(719);
				tableReference(0);
				setState(720);
				match(T__9);
				setState(722);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(721);
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
				setState(724);
				match(T__8);
				setState(725);
				fullSelect();
				setState(726);
				match(T__9);
				setState(728);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
				case 1:
					{
					setState(727);
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
				setState(730);
				singleTable();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(752);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,77,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(750);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
					case 1:
						{
						_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
						setState(733);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(735);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==JOINTYPE) {
							{
							setState(734);
							match(JOINTYPE);
							}
						}

						setState(737);
						match(T__49);
						setState(738);
						tableReference(0);
						setState(739);
						match(T__3);
						setState(740);
						searchCondition();
						setState(742);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
						case 1:
							{
							setState(741);
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
						setState(744);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(745);
						match(CROSSJOIN);
						setState(746);
						tableReference(0);
						setState(748);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
						case 1:
							{
							setState(747);
							correlationClause();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(754);
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
			setState(755);
			tableName();
			setState(757);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
			case 1:
				{
				setState(756);
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
			setState(759);
			match(T__50);
			setState(760);
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
			setState(762);
			match(T__51);
			setState(763);
			match(T__19);
			setState(764);
			columnName();
			setState(769);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(765);
				match(T__1);
				setState(766);
				columnName();
				}
				}
				setState(771);
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
			setState(772);
			match(T__52);
			setState(773);
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
			setState(775);
			match(T__53);
			setState(776);
			match(T__19);
			setState(777);
			sortKey();
			setState(782);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(778);
				match(T__1);
				setState(779);
				sortKey();
				}
				}
				setState(784);
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
			setState(793);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER:
				_localctx = new SortKeyIntContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(785);
				match(INTEGER);
				setState(787);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(786);
					match(DIRECTION);
					}
				}

				}
				break;
			case IDENTIFIER:
				_localctx = new SortKeyColContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(789);
				columnName();
				setState(791);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DIRECTION) {
					{
					setState(790);
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
			setState(796);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__36) {
				{
				setState(795);
				match(T__36);
				}
			}

			setState(798);
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
			setState(800);
			match(T__54);
			setState(801);
			match(T__55);
			setState(803);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INTEGER) {
				{
				setState(802);
				match(INTEGER);
				}
			}

			setState(805);
			_la = _input.LA(1);
			if ( !(_la==T__20 || _la==T__56) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(806);
			match(T__57);
			}
		}
		catch (RecognitionException re) {
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
		public List<TerminalNode> IDENTIFIER() { return getTokens(SelectParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(SelectParser.IDENTIFIER, i);
		}
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
			setState(812);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
			case 1:
				_localctx = new Table1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(808);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new Table2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(809);
				match(IDENTIFIER);
				setState(810);
				match(T__25);
				setState(811);
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
			setState(818);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
			case 1:
				_localctx = new Col1PartContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(814);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new Col2PartContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(815);
				match(IDENTIFIER);
				setState(816);
				match(T__25);
				setState(817);
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
			setState(820);
			searchClause();
			setState(824);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(821);
					connectedSearchClause();
					}
					} 
				}
				setState(826);
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
			setState(827);
			_la = _input.LA(1);
			if ( !(_la==AND || _la==OR) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(828);
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
			setState(831);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(830);
				match(NOT);
				}
			}

			setState(838);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
			case 1:
				{
				setState(833);
				predicate();
				}
				break;
			case 2:
				{
				{
				setState(834);
				match(T__8);
				setState(835);
				searchCondition();
				setState(836);
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
			setState(852);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
			case 1:
				_localctx = new NormalPredicateContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(840);
				expression(0);
				setState(841);
				operator();
				setState(842);
				expression(0);
				}
				}
				break;
			case 2:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(844);
				expression(0);
				setState(845);
				match(NULLOPERATOR);
				}
				}
				break;
			case 3:
				_localctx = new ExistsPredicateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(847);
				match(T__58);
				setState(848);
				match(T__8);
				setState(849);
				subSelect();
				setState(850);
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
			setState(854);
			_la = _input.LA(1);
			if ( !(_la==EQUALS || _la==OPERATOR) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode STAR() { return getToken(SelectParser.STAR, 0); }
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
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
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
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode CONCAT() { return getToken(SelectParser.CONCAT, 0); }
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
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
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
			setState(914);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
			case 1:
				{
				_localctx = new FunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(857);
				identifier();
				setState(858);
				match(T__8);
				setState(859);
				expression(0);
				setState(864);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(860);
					match(T__1);
					setState(861);
					expression(0);
					}
					}
					setState(866);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(867);
				match(T__9);
				}
				break;
			case 2:
				{
				_localctx = new CountDistinctContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(869);
				match(COUNT);
				setState(870);
				match(T__8);
				setState(871);
				match(DISTINCT);
				setState(872);
				expression(0);
				setState(873);
				match(T__9);
				}
				break;
			case 3:
				{
				_localctx = new ListContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(875);
				match(T__8);
				setState(876);
				expression(0);
				setState(879); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(877);
					match(T__1);
					setState(878);
					expression(0);
					}
					}
					setState(881); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__1 );
				setState(883);
				match(T__9);
				}
				break;
			case 4:
				{
				_localctx = new CountStarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(885);
				match(COUNT);
				setState(886);
				match(T__8);
				setState(887);
				match(STAR);
				setState(888);
				match(T__9);
				}
				break;
			case 5:
				{
				_localctx = new IsLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(889);
				literal();
				}
				break;
			case 6:
				{
				_localctx = new ColLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(890);
				columnName();
				}
				break;
			case 7:
				{
				_localctx = new ExpSelectContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(891);
				match(T__8);
				setState(892);
				subSelect();
				setState(893);
				match(T__9);
				}
				break;
			case 8:
				{
				_localctx = new PExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(895);
				match(T__8);
				setState(896);
				expression(0);
				setState(897);
				match(T__9);
				}
				break;
			case 9:
				{
				_localctx = new NullExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(899);
				match(NULL);
				}
				break;
			case 10:
				{
				_localctx = new CaseExpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(900);
				match(T__61);
				setState(901);
				caseCase();
				setState(905);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__64) {
					{
					{
					setState(902);
					caseCase();
					}
					}
					setState(907);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(908);
				match(T__62);
				setState(909);
				expression(0);
				setState(910);
				match(T__63);
				setState(912);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
				case 1:
					{
					setState(911);
					match(T__61);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(927);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
			while ( _alt!=2 && _alt!= ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(925);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
					case 1:
						{
						_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(916);
						if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
						setState(917);
						((MulDivContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__59 || _la==STAR) ) {
							((MulDivContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(918);
						expression(14);
						}
						break;
					case 2:
						{
						_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(919);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(920);
						((AddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==T__60 || _la==NEGATIVE) ) {
							((AddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(921);
						expression(13);
						}
						break;
					case 3:
						{
						_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(922);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(923);
						match(CONCAT);
						setState(924);
						expression(12);
						}
						break;
					}
					} 
				}
				setState(929);
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
			setState(930);
			match(T__64);
			setState(931);
			searchCondition();
			setState(932);
			match(T__65);
			setState(933);
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
			setState(935);
			_la = _input.LA(1);
			if ( !(((((_la - 69)) & ~0x3f) == 0 && ((1L << (_la - 69)) & ((1L << (COUNT - 69)) | (1L << (DATE - 69)) | (1L << (IDENTIFIER - 69)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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
			setState(946);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NEGATIVE:
			case INTEGER:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(938);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NEGATIVE) {
					{
					setState(937);
					match(NEGATIVE);
					}
				}

				setState(940);
				match(INTEGER);
				setState(943);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
				case 1:
					{
					setState(941);
					match(T__25);
					setState(942);
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
				setState(945);
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
		case 55:
			return tableReference_sempred((TableReferenceContext)_localctx, predIndex);
		case 71:
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3g\u03b7\4\2\t\2\4"+
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
		"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\7\2\u00c1\n\2\f\2\16\2\u00c4"+
		"\13\2\5\2\u00c6\n\2\3\2\3\2\3\2\3\2\5\2\u00cc\n\2\3\3\3\3\3\3\3\3\3\4"+
		"\3\4\3\4\3\4\5\4\u00d6\n\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4\u00de\n\4\f\4\16"+
		"\4\u00e1\13\4\3\4\3\4\5\4\u00e5\n\4\3\5\3\5\3\5\3\5\3\5\5\5\u00ec\n\5"+
		"\3\5\3\5\3\5\5\5\u00f1\n\5\3\6\3\6\3\6\3\6\5\6\u00f7\n\6\3\7\3\7\5\7\u00fb"+
		"\n\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7\u0103\n\7\f\7\16\7\u0106\13\7\3\7\3\7"+
		"\5\7\u010a\n\7\3\7\3\7\5\7\u010e\n\7\3\7\5\7\u0111\n\7\3\7\5\7\u0114\n"+
		"\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\7\b\u011e\n\b\f\b\16\b\u0121\13\b\3"+
		"\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u012d\n\t\f\t\16\t\u0130\13"+
		"\t\3\t\3\t\3\t\5\t\u0135\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f"+
		"\7\f\u0151\n\f\f\f\16\f\u0154\13\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r"+
		"\3\r\7\r\u0160\n\r\f\r\16\r\u0163\13\r\3\16\3\16\3\17\3\17\3\20\3\20\3"+
		"\20\3\20\3\20\7\20\u016e\n\20\f\20\16\20\u0171\13\20\3\20\3\20\3\21\3"+
		"\21\5\21\u0177\n\21\3\22\3\22\3\22\3\22\7\22\u017d\n\22\f\22\16\22\u0180"+
		"\13\22\3\22\3\22\3\22\3\22\5\22\u0186\n\22\5\22\u0188\n\22\3\23\3\23\3"+
		"\23\3\23\7\23\u018e\n\23\f\23\16\23\u0191\13\23\3\23\3\23\3\24\7\24\u0196"+
		"\n\24\f\24\16\24\u0199\13\24\3\25\3\25\5\25\u019d\n\25\3\26\3\26\5\26"+
		"\u01a1\n\26\3\26\3\26\3\26\5\26\u01a6\n\26\5\26\u01a8\n\26\3\27\3\27\3"+
		"\27\3\27\7\27\u01ae\n\27\f\27\16\27\u01b1\13\27\3\27\3\27\3\30\3\30\3"+
		"\30\3\30\3\31\3\31\3\31\3\31\7\31\u01bd\n\31\f\31\16\31\u01c0\13\31\3"+
		"\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\7\33\u01ce"+
		"\n\33\f\33\16\33\u01d1\13\33\3\33\3\33\3\34\3\34\5\34\u01d7\n\34\3\34"+
		"\3\34\3\34\5\34\u01dc\n\34\5\34\u01de\n\34\3\35\3\35\3\35\3\35\3\36\3"+
		"\36\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3 \3 \5 \u01f0\n \3 \3 \3"+
		" \3 \3 \3 \3 \3 \7 \u01fa\n \f \16 \u01fd\13 \3 \3 \3!\3!\3!\3!\3\"\3"+
		"\"\3\"\3\"\3\"\3\"\5\"\u020b\n\"\3\"\3\"\3\"\3#\3#\3$\7$\u0213\n$\f$\16"+
		"$\u0216\13$\3$\3$\3%\3%\5%\u021c\n%\3&\3&\3&\5&\u0221\n&\3&\5&\u0224\n"+
		"&\3\'\3\'\3\'\3\'\3\'\3\'\7\'\u022c\n\'\f\'\16\'\u022f\13\'\3\'\3\'\3"+
		"(\3(\3(\3)\3)\3)\3*\3*\3*\3*\3*\5*\u023e\n*\3+\3+\3+\3+\3+\3,\3,\3-\3"+
		"-\3.\3.\3/\3/\3\60\3\60\3\60\3\60\7\60\u0251\n\60\f\60\16\60\u0254\13"+
		"\60\3\60\3\60\3\61\3\61\3\61\3\61\3\61\7\61\u025d\n\61\f\61\16\61\u0260"+
		"\13\61\3\61\3\61\5\61\u0264\n\61\3\61\3\61\3\61\3\61\3\61\3\62\3\62\3"+
		"\62\3\62\3\62\5\62\u0270\n\62\3\62\7\62\u0273\n\62\f\62\16\62\u0276\13"+
		"\62\3\62\5\62\u0279\n\62\3\62\5\62\u027c\n\62\3\63\3\63\3\63\3\63\3\63"+
		"\3\63\5\63\u0284\n\63\3\64\3\64\3\64\5\64\u0289\n\64\3\64\5\64\u028c\n"+
		"\64\3\64\5\64\u028f\n\64\3\64\5\64\u0292\n\64\3\64\5\64\u0295\n\64\3\65"+
		"\3\65\5\65\u0299\n\65\3\65\3\65\3\65\3\65\7\65\u029f\n\65\f\65\16\65\u02a2"+
		"\13\65\5\65\u02a4\n\65\3\66\3\66\3\67\3\67\5\67\u02aa\n\67\3\67\5\67\u02ad"+
		"\n\67\3\67\3\67\5\67\u02b1\n\67\3\67\5\67\u02b4\n\67\5\67\u02b6\n\67\3"+
		"8\38\38\38\78\u02bc\n8\f8\168\u02bf\138\39\39\39\39\59\u02c5\n9\39\39"+
		"\39\39\39\39\59\u02cd\n9\39\39\39\39\39\39\59\u02d5\n9\39\39\39\39\59"+
		"\u02db\n9\39\59\u02de\n9\39\39\59\u02e2\n9\39\39\39\39\39\59\u02e9\n9"+
		"\39\39\39\39\59\u02ef\n9\79\u02f1\n9\f9\169\u02f4\139\3:\3:\5:\u02f8\n"+
		":\3;\3;\3;\3<\3<\3<\3<\3<\7<\u0302\n<\f<\16<\u0305\13<\3=\3=\3=\3>\3>"+
		"\3>\3>\3>\7>\u030f\n>\f>\16>\u0312\13>\3?\3?\5?\u0316\n?\3?\3?\5?\u031a"+
		"\n?\5?\u031c\n?\3@\5@\u031f\n@\3@\3@\3A\3A\3A\5A\u0326\nA\3A\3A\3A\3B"+
		"\3B\3B\3B\5B\u032f\nB\3C\3C\3C\3C\5C\u0335\nC\3D\3D\7D\u0339\nD\fD\16"+
		"D\u033c\13D\3E\3E\3E\3F\5F\u0342\nF\3F\3F\3F\3F\3F\5F\u0349\nF\3G\3G\3"+
		"G\3G\3G\3G\3G\3G\3G\3G\3G\3G\5G\u0357\nG\3H\3H\3I\3I\3I\3I\3I\3I\7I\u0361"+
		"\nI\fI\16I\u0364\13I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\6I\u0372\nI\r"+
		"I\16I\u0373\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3I\3"+
		"I\7I\u038a\nI\fI\16I\u038d\13I\3I\3I\3I\3I\5I\u0393\nI\5I\u0395\nI\3I"+
		"\3I\3I\3I\3I\3I\3I\3I\3I\7I\u03a0\nI\fI\16I\u03a3\13I\3J\3J\3J\3J\3J\3"+
		"K\3K\3L\5L\u03ad\nL\3L\3L\3L\5L\u03b2\nL\3L\5L\u03b5\nL\3L\3\u0197\4p"+
		"\u0090M\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:"+
		"<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a"+
		"\u008c\u008e\u0090\u0092\u0094\u0096\2\r\3\2\37!\3\2Z[\3\2-.\3\2\61\62"+
		"\4\2VV]]\4\2\27\27;;\3\2MN\3\2JK\4\2>>FF\4\2??II\5\2GGaadd\u03eb\2\u00cb"+
		"\3\2\2\2\4\u00cd\3\2\2\2\6\u00d1\3\2\2\2\b\u00e6\3\2\2\2\n\u00f2\3\2\2"+
		"\2\f\u00f8\3\2\2\2\16\u0118\3\2\2\2\20\u0124\3\2\2\2\22\u0136\3\2\2\2"+
		"\24\u0145\3\2\2\2\26\u014d\3\2\2\2\30\u0157\3\2\2\2\32\u0164\3\2\2\2\34"+
		"\u0166\3\2\2\2\36\u0168\3\2\2\2 \u0176\3\2\2\2\"\u0178\3\2\2\2$\u0189"+
		"\3\2\2\2&\u0197\3\2\2\2(\u019c\3\2\2\2*\u01a0\3\2\2\2,\u01a9\3\2\2\2."+
		"\u01b4\3\2\2\2\60\u01b8\3\2\2\2\62\u01c3\3\2\2\2\64\u01c9\3\2\2\2\66\u01d6"+
		"\3\2\2\28\u01df\3\2\2\2:\u01e3\3\2\2\2<\u01e9\3\2\2\2>\u01ed\3\2\2\2@"+
		"\u0200\3\2\2\2B\u0204\3\2\2\2D\u020f\3\2\2\2F\u0214\3\2\2\2H\u0219\3\2"+
		"\2\2J\u021d\3\2\2\2L\u0225\3\2\2\2N\u0232\3\2\2\2P\u0235\3\2\2\2R\u023d"+
		"\3\2\2\2T\u023f\3\2\2\2V\u0244\3\2\2\2X\u0246\3\2\2\2Z\u0248\3\2\2\2\\"+
		"\u024a\3\2\2\2^\u024c\3\2\2\2`\u0257\3\2\2\2b\u026f\3\2\2\2d\u027d\3\2"+
		"\2\2f\u0285\3\2\2\2h\u0296\3\2\2\2j\u02a5\3\2\2\2l\u02b5\3\2\2\2n\u02b7"+
		"\3\2\2\2p\u02dd\3\2\2\2r\u02f5\3\2\2\2t\u02f9\3\2\2\2v\u02fc\3\2\2\2x"+
		"\u0306\3\2\2\2z\u0309\3\2\2\2|\u031b\3\2\2\2~\u031e\3\2\2\2\u0080\u0322"+
		"\3\2\2\2\u0082\u032e\3\2\2\2\u0084\u0334\3\2\2\2\u0086\u0336\3\2\2\2\u0088"+
		"\u033d\3\2\2\2\u008a\u0341\3\2\2\2\u008c\u0356\3\2\2\2\u008e\u0358\3\2"+
		"\2\2\u0090\u0394\3\2\2\2\u0092\u03a4\3\2\2\2\u0094\u03a9\3\2\2\2\u0096"+
		"\u03b4\3\2\2\2\u0098\u0099\5\6\4\2\u0099\u009a\7\2\2\3\u009a\u00cc\3\2"+
		"\2\2\u009b\u009c\5\b\5\2\u009c\u009d\7\2\2\3\u009d\u00cc\3\2\2\2\u009e"+
		"\u009f\5\n\6\2\u009f\u00a0\7\2\2\3\u00a0\u00cc\3\2\2\2\u00a1\u00a2\5\20"+
		"\t\2\u00a2\u00a3\7\2\2\3\u00a3\u00cc\3\2\2\2\u00a4\u00a5\5\f\7\2\u00a5"+
		"\u00a6\7\2\2\3\u00a6\u00cc\3\2\2\2\u00a7\u00a8\5> \2\u00a8\u00a9\7\2\2"+
		"\3\u00a9\u00cc\3\2\2\2\u00aa\u00ab\5:\36\2\u00ab\u00ac\7\2\2\3\u00ac\u00cc"+
		"\3\2\2\2\u00ad\u00ae\58\35\2\u00ae\u00af\7\2\2\3\u00af\u00cc\3\2\2\2\u00b0"+
		"\u00b1\5@!\2\u00b1\u00b2\7\2\2\3\u00b2\u00cc\3\2\2\2\u00b3\u00b4\5<\37"+
		"\2\u00b4\u00b5\7\2\2\3\u00b5\u00cc\3\2\2\2\u00b6\u00b7\5B\"\2\u00b7\u00b8"+
		"\7\2\2\3\u00b8\u00cc\3\2\2\2\u00b9\u00ba\5\4\3\2\u00ba\u00bb\7\2\2\3\u00bb"+
		"\u00cc\3\2\2\2\u00bc\u00bd\7\3\2\2\u00bd\u00c2\5`\61\2\u00be\u00bf\7\4"+
		"\2\2\u00bf\u00c1\5`\61\2\u00c0\u00be\3\2\2\2\u00c1\u00c4\3\2\2\2\u00c2"+
		"\u00c0\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c2\3\2"+
		"\2\2\u00c5\u00bc\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7"+
		"\u00c8\5b\62\2\u00c8\u00c9\3\2\2\2\u00c9\u00ca\7\2\2\3\u00ca\u00cc\3\2"+
		"\2\2\u00cb\u0098\3\2\2\2\u00cb\u009b\3\2\2\2\u00cb\u009e\3\2\2\2\u00cb"+
		"\u00a1\3\2\2\2\u00cb\u00a4\3\2\2\2\u00cb\u00a7\3\2\2\2\u00cb\u00aa\3\2"+
		"\2\2\u00cb\u00ad\3\2\2\2\u00cb\u00b0\3\2\2\2\u00cb\u00b3\3\2\2\2\u00cb"+
		"\u00b6\3\2\2\2\u00cb\u00b9\3\2\2\2\u00cb\u00c5\3\2\2\2\u00cc\3\3\2\2\2"+
		"\u00cd\u00ce\7\5\2\2\u00ce\u00cf\7\6\2\2\u00cf\u00d0\5\u0082B\2\u00d0"+
		"\5\3\2\2\2\u00d1\u00d2\7\7\2\2\u00d2\u00d3\7\b\2\2\u00d3\u00e4\5\u0082"+
		"B\2\u00d4\u00d6\7\t\2\2\u00d5\u00d4\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6"+
		"\u00d7\3\2\2\2\u00d7\u00e5\5b\62\2\u00d8\u00d9\7\n\2\2\u00d9\u00da\7\13"+
		"\2\2\u00da\u00df\5\u0090I\2\u00db\u00dc\7\4\2\2\u00dc\u00de\5\u0090I\2"+
		"\u00dd\u00db\3\2\2\2\u00de\u00e1\3\2\2\2\u00df\u00dd\3\2\2\2\u00df\u00e0"+
		"\3\2\2\2\u00e0\u00e2\3\2\2\2\u00e1\u00df\3\2\2\2\u00e2\u00e3\7\f\2\2\u00e3"+
		"\u00e5\3\2\2\2\u00e4\u00d5\3\2\2\2\u00e4\u00d8\3\2\2\2\u00e5\7\3\2\2\2"+
		"\u00e6\u00e7\7\r\2\2\u00e7\u00e8\5\u0082B\2\u00e8\u00eb\7\16\2\2\u00e9"+
		"\u00ec\5\u0084C\2\u00ea\u00ec\5^\60\2\u00eb\u00e9\3\2\2\2\u00eb\u00ea"+
		"\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\7J\2\2\u00ee\u00f0\5\u0090I\2"+
		"\u00ef\u00f1\5t;\2\u00f0\u00ef\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\t\3\2"+
		"\2\2\u00f2\u00f3\7\17\2\2\u00f3\u00f4\7\t\2\2\u00f4\u00f6\5\u0082B\2\u00f5"+
		"\u00f7\5t;\2\u00f6\u00f5\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7\13\3\2\2\2"+
		"\u00f8\u00fa\7\20\2\2\u00f9\u00fb\7U\2\2\u00fa\u00f9\3\2\2\2\u00fa\u00fb"+
		"\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\u00fd\7\21\2\2\u00fd\u00fe\5\u0082"+
		"B\2\u00fe\u00ff\7\13\2\2\u00ff\u0104\5J&\2\u0100\u0101\7\4\2\2\u0101\u0103"+
		"\5J&\2\u0102\u0100\3\2\2\2\u0103\u0106\3\2\2\2\u0104\u0102\3\2\2\2\u0104"+
		"\u0105\3\2\2\2\u0105\u0109\3\2\2\2\u0106\u0104\3\2\2\2\u0107\u0108\7\4"+
		"\2\2\u0108\u010a\5L\'\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2\2\2\u010a"+
		"\u010b\3\2\2\2\u010b\u010d\7\f\2\2\u010c\u010e\5\36\20\2\u010d\u010c\3"+
		"\2\2\2\u010d\u010e\3\2\2\2\u010e\u0110\3\2\2\2\u010f\u0111\5\16\b\2\u0110"+
		"\u010f\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u0113\3\2\2\2\u0112\u0114\5 "+
		"\21\2\u0113\u0112\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u0115\3\2\2\2\u0115"+
		"\u0116\5(\25\2\u0116\u0117\5\66\34\2\u0117\r\3\2\2\2\u0118\u0119\7c\2"+
		"\2\u0119\u011a\7\13\2\2\u011a\u011f\7W\2\2\u011b\u011c\7\4\2\2\u011c\u011e"+
		"\7W\2\2\u011d\u011b\3\2\2\2\u011e\u0121\3\2\2\2\u011f\u011d\3\2\2\2\u011f"+
		"\u0120\3\2\2\2\u0120\u0122\3\2\2\2\u0121\u011f\3\2\2\2\u0122\u0123\7\f"+
		"\2\2\u0123\17\3\2\2\2\u0124\u0125\7\20\2\2\u0125\u0126\7\22\2\2\u0126"+
		"\u0127\7\21\2\2\u0127\u0128\5\u0082B\2\u0128\u0129\7\13\2\2\u0129\u012e"+
		"\5J&\2\u012a\u012b\7\4\2\2\u012b\u012d\5J&\2\u012c\u012a\3\2\2\2\u012d"+
		"\u0130\3\2\2\2\u012e\u012c\3\2\2\2\u012e\u012f\3\2\2\2\u012f\u0131\3\2"+
		"\2\2\u0130\u012e\3\2\2\2\u0131\u0134\7\f\2\2\u0132\u0135\5\22\n\2\u0133"+
		"\u0135\5\24\13\2\u0134\u0132\3\2\2\2\u0134\u0133\3\2\2\2\u0135\21\3\2"+
		"\2\2\u0136\u0137\7\23\2\2\u0137\u0138\7\t\2\2\u0138\u0139\5\34\17\2\u0139"+
		"\u013a\7\24\2\2\u013a\u013b\7\25\2\2\u013b\u013c\7\26\2\2\u013c\u013d"+
		"\5\32\16\2\u013d\u013e\7\27\2\2\u013e\u013f\7\25\2\2\u013f\u0140\7\26"+
		"\2\2\u0140\u0141\5\32\16\2\u0141\u0142\7\30\2\2\u0142\u0143\7\31\2\2\u0143"+
		"\u0144\7f\2\2\u0144\23\3\2\2\2\u0145\u0146\7\32\2\2\u0146\u0147\5\26\f"+
		"\2\u0147\u0148\7\3\2\2\u0148\u0149\7\33\2\2\u0149\u014a\7\13\2\2\u014a"+
		"\u014b\5\30\r\2\u014b\u014c\7\f\2\2\u014c\25\3\2\2\2\u014d\u0152\7e\2"+
		"\2\u014e\u014f\7\34\2\2\u014f\u0151\7e\2\2\u0150\u014e\3\2\2\2\u0151\u0154"+
		"\3\2\2\2\u0152\u0150\3\2\2\2\u0152\u0153\3\2\2\2\u0153\u0155\3\2\2\2\u0154"+
		"\u0152\3\2\2\2\u0155\u0156\7\35\2\2\u0156\27\3\2\2\2\u0157\u0158\5\32"+
		"\16\2\u0158\u0159\7\36\2\2\u0159\u0161\5\32\16\2\u015a\u015b\7\4\2\2\u015b"+
		"\u015c\5\32\16\2\u015c\u015d\7\36\2\2\u015d\u015e\5\32\16\2\u015e\u0160"+
		"\3\2\2\2\u015f\u015a\3\2\2\2\u0160\u0163\3\2\2\2\u0161\u015f\3\2\2\2\u0161"+
		"\u0162\3\2\2\2\u0162\31\3\2\2\2\u0163\u0161\3\2\2\2\u0164\u0165\13\2\2"+
		"\2\u0165\33\3\2\2\2\u0166\u0167\t\2\2\2\u0167\35\3\2\2\2\u0168\u0169\7"+
		"b\2\2\u0169\u016a\7\13\2\2\u016a\u016f\7W\2\2\u016b\u016c\7\4\2\2\u016c"+
		"\u016e\7W\2\2\u016d\u016b\3\2\2\2\u016e\u0171\3\2\2\2\u016f\u016d\3\2"+
		"\2\2\u016f\u0170\3\2\2\2\u0170\u0172\3\2\2\2\u0171\u016f\3\2\2\2\u0172"+
		"\u0173\7\f\2\2\u0173\37\3\2\2\2\u0174\u0177\7\\\2\2\u0175\u0177\5\"\22"+
		"\2\u0176\u0174\3\2\2\2\u0176\u0175\3\2\2\2\u0177!\3\2\2\2\u0178\u0179"+
		"\7\"\2\2\u0179\u017e\5$\23\2\u017a\u017b\7#\2\2\u017b\u017d\5$\23\2\u017c"+
		"\u017a\3\2\2\2\u017d\u0180\3\2\2\2\u017e\u017c\3\2\2\2\u017e\u017f\3\2"+
		"\2\2\u017f\u0181\3\2\2\2\u0180\u017e\3\2\2\2\u0181\u0187\7$\2\2\u0182"+
		"\u0185\7\4\2\2\u0183\u0186\5.\30\2\u0184\u0186\5\62\32\2\u0185\u0183\3"+
		"\2\2\2\u0185\u0184\3\2\2\2\u0186\u0188\3\2\2\2\u0187\u0182\3\2\2\2\u0187"+
		"\u0188\3\2\2\2\u0188#\3\2\2\2\u0189\u018a\7\"\2\2\u018a\u018f\7W\2\2\u018b"+
		"\u018c\7#\2\2\u018c\u018e\7W\2\2\u018d\u018b\3\2\2\2\u018e\u0191\3\2\2"+
		"\2\u018f\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0192\3\2\2\2\u0191\u018f"+
		"\3\2\2\2\u0192\u0193\7$\2\2\u0193%\3\2\2\2\u0194\u0196\13\2\2\2\u0195"+
		"\u0194\3\2\2\2\u0196\u0199\3\2\2\2\u0197\u0198\3\2\2\2\u0197\u0195\3\2"+
		"\2\2\u0198\'\3\2\2\2\u0199\u0197\3\2\2\2\u019a\u019d\7^\2\2\u019b\u019d"+
		"\5*\26\2\u019c\u019a\3\2\2\2\u019c\u019b\3\2\2\2\u019d)\3\2\2\2\u019e"+
		"\u01a1\7]\2\2\u019f\u01a1\5,\27\2\u01a0\u019e\3\2\2\2\u01a0\u019f\3\2"+
		"\2\2\u01a1\u01a7\3\2\2\2\u01a2\u01a5\7\4\2\2\u01a3\u01a6\5.\30\2\u01a4"+
		"\u01a6\5\62\32\2\u01a5\u01a3\3\2\2\2\u01a5\u01a4\3\2\2\2\u01a6\u01a8\3"+
		"\2\2\2\u01a7\u01a2\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8+\3\2\2\2\u01a9\u01aa"+
		"\7\"\2\2\u01aa\u01af\7W\2\2\u01ab\u01ac\7#\2\2\u01ac\u01ae\7W\2\2\u01ad"+
		"\u01ab\3\2\2\2\u01ae\u01b1\3\2\2\2\u01af\u01ad\3\2\2\2\u01af\u01b0\3\2"+
		"\2\2\u01b0\u01b2\3\2\2\2\u01b1\u01af\3\2\2\2\u01b2\u01b3\7$\2\2\u01b3"+
		"-\3\2\2\2\u01b4\u01b5\7_\2\2\u01b5\u01b6\7\4\2\2\u01b6\u01b7\5\60\31\2"+
		"\u01b7/\3\2\2\2\u01b8\u01b9\7\"\2\2\u01b9\u01be\5\u0084C\2\u01ba\u01bb"+
		"\7#\2\2\u01bb\u01bd\5\u0084C\2\u01bc\u01ba\3\2\2\2\u01bd\u01c0\3\2\2\2"+
		"\u01be\u01bc\3\2\2\2\u01be\u01bf\3\2\2\2\u01bf\u01c1\3\2\2\2\u01c0\u01be"+
		"\3\2\2\2\u01c1\u01c2\7$\2\2\u01c2\61\3\2\2\2\u01c3\u01c4\7`\2\2\u01c4"+
		"\u01c5\7\4\2\2\u01c5\u01c6\5\u0084C\2\u01c6\u01c7\7\4\2\2\u01c7\u01c8"+
		"\5\64\33\2\u01c8\63\3\2\2\2\u01c9\u01ca\7\"\2\2\u01ca\u01cf\5&\24\2\u01cb"+
		"\u01cc\7#\2\2\u01cc\u01ce\5&\24\2\u01cd\u01cb\3\2\2\2\u01ce\u01d1\3\2"+
		"\2\2\u01cf\u01cd\3\2\2\2\u01cf\u01d0\3\2\2\2\u01d0\u01d2\3\2\2\2\u01d1"+
		"\u01cf\3\2\2\2\u01d2\u01d3\7$\2\2\u01d3\65\3\2\2\2\u01d4\u01d7\7]\2\2"+
		"\u01d5\u01d7\5,\27\2\u01d6\u01d4\3\2\2\2\u01d6\u01d5\3\2\2\2\u01d7\u01dd"+
		"\3\2\2\2\u01d8\u01db\7\4\2\2\u01d9\u01dc\5.\30\2\u01da\u01dc\5&\24\2\u01db"+
		"\u01d9\3\2\2\2\u01db\u01da\3\2\2\2\u01dc\u01de\3\2\2\2\u01dd\u01d8\3\2"+
		"\2\2\u01dd\u01de\3\2\2\2\u01de\67\3\2\2\2\u01df\u01e0\7%\2\2\u01e0\u01e1"+
		"\7\21\2\2\u01e1\u01e2\5\u0082B\2\u01e29\3\2\2\2\u01e3\u01e4\7\20\2\2\u01e4"+
		"\u01e5\7&\2\2\u01e5\u01e6\5\u0082B\2\u01e6\u01e7\7\'\2\2\u01e7\u01e8\5"+
		"b\62\2\u01e8;\3\2\2\2\u01e9\u01ea\7%\2\2\u01ea\u01eb\7&\2\2\u01eb\u01ec"+
		"\5\u0082B\2\u01ec=\3\2\2\2\u01ed\u01ef\7\20\2\2\u01ee\u01f0\7Y\2\2\u01ef"+
		"\u01ee\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01f1\3\2\2\2\u01f1\u01f2\7("+
		"\2\2\u01f2\u01f3\5\u0082B\2\u01f3\u01f4\7\6\2\2\u01f4\u01f5\5\u0082B\2"+
		"\u01f5\u01f6\7\13\2\2\u01f6\u01fb\5H%\2\u01f7\u01f8\7\4\2\2\u01f8\u01fa"+
		"\5H%\2\u01f9\u01f7\3\2\2\2\u01fa\u01fd\3\2\2\2\u01fb\u01f9\3\2\2\2\u01fb"+
		"\u01fc\3\2\2\2\u01fc\u01fe\3\2\2\2\u01fd\u01fb\3\2\2\2\u01fe\u01ff\7\f"+
		"\2\2\u01ff?\3\2\2\2\u0200\u0201\7%\2\2\u0201\u0202\7(\2\2\u0202\u0203"+
		"\5\u0082B\2\u0203A\3\2\2\2\u0204\u0205\7)\2\2\u0205\u0206\t\3\2\2\u0206"+
		"\u0207\7\b\2\2\u0207\u020a\5\u0082B\2\u0208\u0209\7*\2\2\u0209\u020b\5"+
		"D#\2\u020a\u0208\3\2\2\2\u020a\u020b\3\2\2\2\u020b\u020c\3\2\2\2\u020c"+
		"\u020d\7\t\2\2\u020d\u020e\5F$\2\u020eC\3\2\2\2\u020f\u0210\13\2\2\2\u0210"+
		"E\3\2\2\2\u0211\u0213\13\2\2\2\u0212\u0211\3\2\2\2\u0213\u0216\3\2\2\2"+
		"\u0214\u0212\3\2\2\2\u0214\u0215\3\2\2\2\u0215\u0217\3\2\2\2\u0216\u0214"+
		"\3\2\2\2\u0217\u0218\7\2\2\3\u0218G\3\2\2\2\u0219\u021b\5\u0084C\2\u021a"+
		"\u021c\7Q\2\2\u021b\u021a\3\2\2\2\u021b\u021c\3\2\2\2\u021cI\3\2\2\2\u021d"+
		"\u021e\5\u0084C\2\u021e\u0220\5R*\2\u021f\u0221\5N(\2\u0220\u021f\3\2"+
		"\2\2\u0220\u0221\3\2\2\2\u0221\u0223\3\2\2\2\u0222\u0224\5P)\2\u0223\u0222"+
		"\3\2\2\2\u0223\u0224\3\2\2\2\u0224K\3\2\2\2\u0225\u0226\7+\2\2\u0226\u0227"+
		"\7,\2\2\u0227\u0228\7\13\2\2\u0228\u022d\5\u0084C\2\u0229\u022a\7\4\2"+
		"\2\u022a\u022c\5\u0084C\2\u022b\u0229\3\2\2\2\u022c\u022f\3\2\2\2\u022d"+
		"\u022b\3\2\2\2\u022d\u022e\3\2\2\2\u022e\u0230\3\2\2\2\u022f\u022d\3\2"+
		"\2\2\u0230\u0231\7\f\2\2\u0231M\3\2\2\2\u0232\u0233\7O\2\2\u0233\u0234"+
		"\7P\2\2\u0234O\3\2\2\2\u0235\u0236\7+\2\2\u0236\u0237\7,\2\2\u0237Q\3"+
		"\2\2\2\u0238\u023e\5T+\2\u0239\u023e\5V,\2\u023a\u023e\5X-\2\u023b\u023e"+
		"\5Z.\2\u023c\u023e\5\\/\2\u023d\u0238\3\2\2\2\u023d\u0239\3\2\2\2\u023d"+
		"\u023a\3\2\2\2\u023d\u023b\3\2\2\2\u023d\u023c\3\2\2\2\u023eS\3\2\2\2"+
		"\u023f\u0240\t\4\2\2\u0240\u0241\7\13\2\2\u0241\u0242\7W\2\2\u0242\u0243"+
		"\7\f\2\2\u0243U\3\2\2\2\u0244\u0245\7/\2\2\u0245W\3\2\2\2\u0246\u0247"+
		"\7\60\2\2\u0247Y\3\2\2\2\u0248\u0249\7a\2\2\u0249[\3\2\2\2\u024a\u024b"+
		"\t\5\2\2\u024b]\3\2\2\2\u024c\u024d\7\13\2\2\u024d\u0252\5\u0084C\2\u024e"+
		"\u024f\7\4\2\2\u024f\u0251\5\u0084C\2\u0250\u024e\3\2\2\2\u0251\u0254"+
		"\3\2\2\2\u0252\u0250\3\2\2\2\u0252\u0253\3\2\2\2\u0253\u0255\3\2\2\2\u0254"+
		"\u0252\3\2\2\2\u0255\u0256\7\f\2\2\u0256_\3\2\2\2\u0257\u0263\7d\2\2\u0258"+
		"\u0259\7\13\2\2\u0259\u025e\5\u0084C\2\u025a\u025b\7\4\2\2\u025b\u025d"+
		"\5\u0084C\2\u025c\u025a\3\2\2\2\u025d\u0260\3\2\2\2\u025e\u025c\3\2\2"+
		"\2\u025e\u025f\3\2\2\2\u025f\u0261\3\2\2\2\u0260\u025e\3\2\2\2\u0261\u0262"+
		"\7\f\2\2\u0262\u0264\3\2\2\2\u0263\u0258\3\2\2\2\u0263\u0264\3\2\2\2\u0264"+
		"\u0265\3\2\2\2\u0265\u0266\7\'\2\2\u0266\u0267\7\13\2\2\u0267\u0268\5"+
		"b\62\2\u0268\u0269\7\f\2\2\u0269a\3\2\2\2\u026a\u0270\5f\64\2\u026b\u026c"+
		"\7\13\2\2\u026c\u026d\5b\62\2\u026d\u026e\7\f\2\2\u026e\u0270\3\2\2\2"+
		"\u026f\u026a\3\2\2\2\u026f\u026b\3\2\2\2\u0270\u0274\3\2\2\2\u0271\u0273"+
		"\5d\63\2\u0272\u0271\3\2\2\2\u0273\u0276\3\2\2\2\u0274\u0272\3\2\2\2\u0274"+
		"\u0275\3\2\2\2\u0275\u0278\3\2\2\2\u0276\u0274\3\2\2\2\u0277\u0279\5z"+
		">\2\u0278\u0277\3\2\2\2\u0278\u0279\3\2\2\2\u0279\u027b\3\2\2\2\u027a"+
		"\u027c\5\u0080A\2\u027b\u027a\3\2\2\2\u027b\u027c\3\2\2\2\u027cc\3\2\2"+
		"\2\u027d\u0283\7T\2\2\u027e\u0284\5f\64\2\u027f\u0280\7\13\2\2\u0280\u0281"+
		"\5b\62\2\u0281\u0282\7\f\2\2\u0282\u0284\3\2\2\2\u0283\u027e\3\2\2\2\u0283"+
		"\u027f\3\2\2\2\u0284e\3\2\2\2\u0285\u0286\5h\65\2\u0286\u0288\5n8\2\u0287"+
		"\u0289\5t;\2\u0288\u0287\3\2\2\2\u0288\u0289\3\2\2\2\u0289\u028b\3\2\2"+
		"\2\u028a\u028c\5v<\2\u028b\u028a\3\2\2\2\u028b\u028c\3\2\2\2\u028c\u028e"+
		"\3\2\2\2\u028d\u028f\5x=\2\u028e\u028d\3\2\2\2\u028e\u028f\3\2\2\2\u028f"+
		"\u0291\3\2\2\2\u0290\u0292\5z>\2\u0291\u0290\3\2\2\2\u0291\u0292\3\2\2"+
		"\2\u0292\u0294\3\2\2\2\u0293\u0295\5\u0080A\2\u0294\u0293\3\2\2\2\u0294"+
		"\u0295\3\2\2\2\u0295g\3\2\2\2\u0296\u0298\7\63\2\2\u0297\u0299\5j\66\2"+
		"\u0298\u0297\3\2\2\2\u0298\u0299\3\2\2\2\u0299\u02a3\3\2\2\2\u029a\u02a4"+
		"\7F\2\2\u029b\u02a0\5l\67\2\u029c\u029d\7\4\2\2\u029d\u029f\5l\67\2\u029e"+
		"\u029c\3\2\2\2\u029f\u02a2\3\2\2\2\u02a0\u029e\3\2\2\2\u02a0\u02a1\3\2"+
		"\2\2\u02a1\u02a4\3\2\2\2\u02a2\u02a0\3\2\2\2\u02a3\u029a\3\2\2\2\u02a3"+
		"\u029b\3\2\2\2\u02a4i\3\2\2\2\u02a5\u02a6\t\6\2\2\u02a6k\3\2\2\2\u02a7"+
		"\u02a9\5\u0084C\2\u02a8\u02aa\7\'\2\2\u02a9\u02a8\3\2\2\2\u02a9\u02aa"+
		"\3\2\2\2\u02aa\u02ac\3\2\2\2\u02ab\u02ad\7d\2\2\u02ac\u02ab\3\2\2\2\u02ac"+
		"\u02ad\3\2\2\2\u02ad\u02b6\3\2\2\2\u02ae\u02b0\5\u0090I\2\u02af\u02b1"+
		"\7\'\2\2\u02b0\u02af\3\2\2\2\u02b0\u02b1\3\2\2\2\u02b1\u02b3\3\2\2\2\u02b2"+
		"\u02b4\7d\2\2\u02b3\u02b2\3\2\2\2\u02b3\u02b4\3\2\2\2\u02b4\u02b6\3\2"+
		"\2\2\u02b5\u02a7\3\2\2\2\u02b5\u02ae\3\2\2\2\u02b6m\3\2\2\2\u02b7\u02b8"+
		"\7\t\2\2\u02b8\u02bd\5p9\2\u02b9\u02ba\7\4\2\2\u02ba\u02bc\5p9\2\u02bb"+
		"\u02b9\3\2\2\2\u02bc\u02bf\3\2\2\2\u02bd\u02bb\3\2\2\2\u02bd\u02be\3\2"+
		"\2\2\u02beo\3\2\2\2\u02bf\u02bd\3\2\2\2\u02c0\u02c1\b9\1\2\u02c1\u02c2"+
		"\7\13\2\2\u02c2\u02c4\5p9\2\u02c3\u02c5\7R\2\2\u02c4\u02c3\3\2\2\2\u02c4"+
		"\u02c5\3\2\2\2\u02c5\u02c6\3\2\2\2\u02c6\u02c7\7\64\2\2\u02c7\u02c8\5"+
		"p9\2\u02c8\u02c9\7\6\2\2\u02c9\u02ca\5\u0086D\2\u02ca\u02cc\7\f\2\2\u02cb"+
		"\u02cd\5~@\2\u02cc\u02cb\3\2\2\2\u02cc\u02cd\3\2\2\2\u02cd\u02de\3\2\2"+
		"\2\u02ce\u02cf\7\13\2\2\u02cf\u02d0\5p9\2\u02d0\u02d1\7S\2\2\u02d1\u02d2"+
		"\5p9\2\u02d2\u02d4\7\f\2\2\u02d3\u02d5\5~@\2\u02d4\u02d3\3\2\2\2\u02d4"+
		"\u02d5\3\2\2\2\u02d5\u02de\3\2\2\2\u02d6\u02d7\7\13\2\2\u02d7\u02d8\5"+
		"b\62\2\u02d8\u02da\7\f\2\2\u02d9\u02db\5~@\2\u02da\u02d9\3\2\2\2\u02da"+
		"\u02db\3\2\2\2\u02db\u02de\3\2\2\2\u02dc\u02de\5r:\2\u02dd\u02c0\3\2\2"+
		"\2\u02dd\u02ce\3\2\2\2\u02dd\u02d6\3\2\2\2\u02dd\u02dc\3\2\2\2\u02de\u02f2"+
		"\3\2\2\2\u02df\u02e1\f\b\2\2\u02e0\u02e2\7R\2\2\u02e1\u02e0\3\2\2\2\u02e1"+
		"\u02e2\3\2\2\2\u02e2\u02e3\3\2\2\2\u02e3\u02e4\7\64\2\2\u02e4\u02e5\5"+
		"p9\2\u02e5\u02e6\7\6\2\2\u02e6\u02e8\5\u0086D\2\u02e7\u02e9\5~@\2\u02e8"+
		"\u02e7\3\2\2\2\u02e8\u02e9\3\2\2\2\u02e9\u02f1\3\2\2\2\u02ea\u02eb\f\7"+
		"\2\2\u02eb\u02ec\7S\2\2\u02ec\u02ee\5p9\2\u02ed\u02ef\5~@\2\u02ee\u02ed"+
		"\3\2\2\2\u02ee\u02ef\3\2\2\2\u02ef\u02f1\3\2\2\2\u02f0\u02df\3\2\2\2\u02f0"+
		"\u02ea\3\2\2\2\u02f1\u02f4\3\2\2\2\u02f2\u02f0\3\2\2\2\u02f2\u02f3\3\2"+
		"\2\2\u02f3q\3\2\2\2\u02f4\u02f2\3\2\2\2\u02f5\u02f7\5\u0082B\2\u02f6\u02f8"+
		"\5~@\2\u02f7\u02f6\3\2\2\2\u02f7\u02f8\3\2\2\2\u02f8s\3\2\2\2\u02f9\u02fa"+
		"\7\65\2\2\u02fa\u02fb\5\u0086D\2\u02fbu\3\2\2\2\u02fc\u02fd\7\66\2\2\u02fd"+
		"\u02fe\7\26\2\2\u02fe\u0303\5\u0084C\2\u02ff\u0300\7\4\2\2\u0300\u0302"+
		"\5\u0084C\2\u0301\u02ff\3\2\2\2\u0302\u0305\3\2\2\2\u0303\u0301\3\2\2"+
		"\2\u0303\u0304\3\2\2\2\u0304w\3\2\2\2\u0305\u0303\3\2\2\2\u0306\u0307"+
		"\7\67\2\2\u0307\u0308\5\u0086D\2\u0308y\3\2\2\2\u0309\u030a\78\2\2\u030a"+
		"\u030b\7\26\2\2\u030b\u0310\5|?\2\u030c\u030d\7\4\2\2\u030d\u030f\5|?"+
		"\2\u030e\u030c\3\2\2\2\u030f\u0312\3\2\2\2\u0310\u030e\3\2\2\2\u0310\u0311"+
		"\3\2\2\2\u0311{\3\2\2\2\u0312\u0310\3\2\2\2\u0313\u0315\7W\2\2\u0314\u0316"+
		"\7Q\2\2\u0315\u0314\3\2\2\2\u0315\u0316\3\2\2\2\u0316\u031c\3\2\2\2\u0317"+
		"\u0319\5\u0084C\2\u0318\u031a\7Q\2\2\u0319\u0318\3\2\2\2\u0319\u031a\3"+
		"\2\2\2\u031a\u031c\3\2\2\2\u031b\u0313\3\2\2\2\u031b\u0317\3\2\2\2\u031c"+
		"}\3\2\2\2\u031d\u031f\7\'\2\2\u031e\u031d\3\2\2\2\u031e\u031f\3\2\2\2"+
		"\u031f\u0320\3\2\2\2\u0320\u0321\7d\2\2\u0321\177\3\2\2\2\u0322\u0323"+
		"\79\2\2\u0323\u0325\7:\2\2\u0324\u0326\7W\2\2\u0325\u0324\3\2\2\2\u0325"+
		"\u0326\3\2\2\2\u0326\u0327\3\2\2\2\u0327\u0328\t\7\2\2\u0328\u0329\7<"+
		"\2\2\u0329\u0081\3\2\2\2\u032a\u032f\7d\2\2\u032b\u032c\7d\2\2\u032c\u032d"+
		"\7\34\2\2\u032d\u032f\7d\2\2\u032e\u032a\3\2\2\2\u032e\u032b\3\2\2\2\u032f"+
		"\u0083\3\2\2\2\u0330\u0335\7d\2\2\u0331\u0332\7d\2\2\u0332\u0333\7\34"+
		"\2\2\u0333\u0335\7d\2\2\u0334\u0330\3\2\2\2\u0334\u0331\3\2\2\2\u0335"+
		"\u0085\3\2\2\2\u0336\u033a\5\u008aF\2\u0337\u0339\5\u0088E\2\u0338\u0337"+
		"\3\2\2\2\u0339\u033c\3\2\2\2\u033a\u0338\3\2\2\2\u033a\u033b\3\2\2\2\u033b"+
		"\u0087\3\2\2\2\u033c\u033a\3\2\2\2\u033d\u033e\t\b\2\2\u033e\u033f\5\u008a"+
		"F\2\u033f\u0089\3\2\2\2\u0340\u0342\7O\2\2\u0341\u0340\3\2\2\2\u0341\u0342"+
		"\3\2\2\2\u0342\u0348\3\2\2\2\u0343\u0349\5\u008cG\2\u0344\u0345\7\13\2"+
		"\2\u0345\u0346\5\u0086D\2\u0346\u0347\7\f\2\2\u0347\u0349\3\2\2\2\u0348"+
		"\u0343\3\2\2\2\u0348\u0344\3\2\2\2\u0349\u008b\3\2\2\2\u034a\u034b\5\u0090"+
		"I\2\u034b\u034c\5\u008eH\2\u034c\u034d\5\u0090I\2\u034d\u0357\3\2\2\2"+
		"\u034e\u034f\5\u0090I\2\u034f\u0350\7L\2\2\u0350\u0357\3\2\2\2\u0351\u0352"+
		"\7=\2\2\u0352\u0353\7\13\2\2\u0353\u0354\5f\64\2\u0354\u0355\7\f\2\2\u0355"+
		"\u0357\3\2\2\2\u0356\u034a\3\2\2\2\u0356\u034e\3\2\2\2\u0356\u0351\3\2"+
		"\2\2\u0357\u008d\3\2\2\2\u0358\u0359\t\t\2\2\u0359\u008f\3\2\2\2\u035a"+
		"\u035b\bI\1\2\u035b\u035c\5\u0094K\2\u035c\u035d\7\13\2\2\u035d\u0362"+
		"\5\u0090I\2\u035e\u035f\7\4\2\2\u035f\u0361\5\u0090I\2\u0360\u035e\3\2"+
		"\2\2\u0361\u0364\3\2\2\2\u0362\u0360\3\2\2\2\u0362\u0363\3\2\2\2\u0363"+
		"\u0365\3\2\2\2\u0364\u0362\3\2\2\2\u0365\u0366\7\f\2\2\u0366\u0395\3\2"+
		"\2\2\u0367\u0368\7G\2\2\u0368\u0369\7\13\2\2\u0369\u036a\7V\2\2\u036a"+
		"\u036b\5\u0090I\2\u036b\u036c\7\f\2\2\u036c\u0395\3\2\2\2\u036d\u036e"+
		"\7\13\2\2\u036e\u0371\5\u0090I\2\u036f\u0370\7\4\2\2\u0370\u0372\5\u0090"+
		"I\2\u0371\u036f\3\2\2\2\u0372\u0373\3\2\2\2\u0373\u0371\3\2\2\2\u0373"+
		"\u0374\3\2\2\2\u0374\u0375\3\2\2\2\u0375\u0376\7\f\2\2\u0376\u0395\3\2"+
		"\2\2\u0377\u0378\7G\2\2\u0378\u0379\7\13\2\2\u0379\u037a\7F\2\2\u037a"+
		"\u0395\7\f\2\2\u037b\u0395\5\u0096L\2\u037c\u0395\5\u0084C\2\u037d\u037e"+
		"\7\13\2\2\u037e\u037f\5f\64\2\u037f\u0380\7\f\2\2\u0380\u0395\3\2\2\2"+
		"\u0381\u0382\7\13\2\2\u0382\u0383\5\u0090I\2\u0383\u0384\7\f\2\2\u0384"+
		"\u0395\3\2\2\2\u0385\u0395\7P\2\2\u0386\u0387\7@\2\2\u0387\u038b\5\u0092"+
		"J\2\u0388\u038a\5\u0092J\2\u0389\u0388\3\2\2\2\u038a\u038d\3\2\2\2\u038b"+
		"\u0389\3\2\2\2\u038b\u038c\3\2\2\2\u038c\u038e\3\2\2\2\u038d\u038b\3\2"+
		"\2\2\u038e\u038f\7A\2\2\u038f\u0390\5\u0090I\2\u0390\u0392\7B\2\2\u0391"+
		"\u0393\7@\2\2\u0392\u0391\3\2\2\2\u0392\u0393\3\2\2\2\u0393\u0395\3\2"+
		"\2\2\u0394\u035a\3\2\2\2\u0394\u0367\3\2\2\2\u0394\u036d\3\2\2\2\u0394"+
		"\u0377\3\2\2\2\u0394\u037b\3\2\2\2\u0394\u037c\3\2\2\2\u0394\u037d\3\2"+
		"\2\2\u0394\u0381\3\2\2\2\u0394\u0385\3\2\2\2\u0394\u0386\3\2\2\2\u0395"+
		"\u03a1\3\2\2\2\u0396\u0397\f\17\2\2\u0397\u0398\t\n\2\2\u0398\u03a0\5"+
		"\u0090I\20\u0399\u039a\f\16\2\2\u039a\u039b\t\13\2\2\u039b\u03a0\5\u0090"+
		"I\17\u039c\u039d\f\r\2\2\u039d\u039e\7H\2\2\u039e\u03a0\5\u0090I\16\u039f"+
		"\u0396\3\2\2\2\u039f\u0399\3\2\2\2\u039f\u039c\3\2\2\2\u03a0\u03a3\3\2"+
		"\2\2\u03a1\u039f\3\2\2\2\u03a1\u03a2\3\2\2\2\u03a2\u0091\3\2\2\2\u03a3"+
		"\u03a1\3\2\2\2\u03a4\u03a5\7C\2\2\u03a5\u03a6\5\u0086D\2\u03a6\u03a7\7"+
		"D\2\2\u03a7\u03a8\5\u0090I\2\u03a8\u0093\3\2\2\2\u03a9\u03aa\t\f\2\2\u03aa"+
		"\u0095\3\2\2\2\u03ab\u03ad\7I\2\2\u03ac\u03ab\3\2\2\2\u03ac\u03ad\3\2"+
		"\2\2\u03ad\u03ae\3\2\2\2\u03ae\u03b1\7W\2\2\u03af\u03b0\7\34\2\2\u03b0"+
		"\u03b2\7W\2\2\u03b1\u03af\3\2\2\2\u03b1\u03b2\3\2\2\2\u03b2\u03b5\3\2"+
		"\2\2\u03b3\u03b5\7E\2\2\u03b4\u03ac\3\2\2\2\u03b4\u03b3\3\2\2\2\u03b5"+
		"\u0097\3\2\2\2h\u00c2\u00c5\u00cb\u00d5\u00df\u00e4\u00eb\u00f0\u00f6"+
		"\u00fa\u0104\u0109\u010d\u0110\u0113\u011f\u012e\u0134\u0152\u0161\u016f"+
		"\u0176\u017e\u0185\u0187\u018f\u0197\u019c\u01a0\u01a5\u01a7\u01af\u01be"+
		"\u01cf\u01d6\u01db\u01dd\u01ef\u01fb\u020a\u0214\u021b\u0220\u0223\u022d"+
		"\u023d\u0252\u025e\u0263\u026f\u0274\u0278\u027b\u0283\u0288\u028b\u028e"+
		"\u0291\u0294\u0298\u02a0\u02a3\u02a9\u02ac\u02b0\u02b3\u02b5\u02bd\u02c4"+
		"\u02cc\u02d4\u02da\u02dd\u02e1\u02e8\u02ee\u02f0\u02f2\u02f7\u0303\u0310"+
		"\u0315\u0319\u031b\u031e\u0325\u032e\u0334\u033a\u0341\u0348\u0356\u0362"+
		"\u0373\u038b\u0392\u0394\u039f\u03a1\u03ac\u03b1\u03b4";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}