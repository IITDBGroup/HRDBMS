package com.exascale.optimizer;

import java.util.List;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast" })
public class SelectParser extends Parser
{
	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
	public static final int T__52 = 1, T__51 = 2, T__50 = 3, T__49 = 4, T__48 = 5, T__47 = 6, T__46 = 7, T__45 = 8, T__44 = 9, T__43 = 10, T__42 = 11, T__41 = 12, T__40 = 13, T__39 = 14, T__38 = 15, T__37 = 16, T__36 = 17, T__35 = 18, T__34 = 19, T__33 = 20, T__32 = 21, T__31 = 22, T__30 = 23, T__29 = 24, T__28 = 25, T__27 = 26, T__26 = 27, T__25 = 28, T__24 = 29, T__23 = 30, T__22 = 31, T__21 = 32, T__20 = 33, T__19 = 34, T__18 = 35, T__17 = 36, T__16 = 37, T__15 = 38, T__14 = 39, T__13 = 40, T__12 = 41, T__11 = 42, T__10 = 43, T__9 = 44, T__8 = 45, T__7 = 46, T__6 = 47, T__5 = 48, T__4 = 49, T__3 = 50, T__2 = 51, T__1 = 52, T__0 = 53, STRING = 54, STAR = 55, COUNT = 56, CONCAT = 57, NEGATIVE = 58, EQUALS = 59, OPERATOR = 60, NULLOPERATOR = 61, AND = 62, OR = 63, NOT = 64, NULL = 65, DIRECTION = 66, JOINTYPE = 67, CROSSJOIN = 68, TABLECOMBINATION = 69, COLUMN = 70, DISTINCT = 71, INTEGER = 72, WS = 73, UNIQUE = 74, REPLACE = 75, RESUME = 76, NONE = 77, ALL = 78, ANYTEXT = 79, HASH = 80, RANGE = 81, DATE = 82, COLORDER = 83, ORGANIZATION = 84, IDENTIFIER = 85, ANY = 86;
	public static final String[] tokenNames = { "<INVALID>", "'DOUBLE'", "'INTEGER'", "'FROM'", "'EXISTS'", "'{'", "'GROUP'", "'CASE'", "'('", "','", "'PRIMARY'", "'DELIMITER'", "'LOAD'", "'VALUES'", "'VARCHAR'", "'UPDATE'", "'DELETE'", "'BIGINT'", "'FIRST'", "'FETCH'", "'HAVING'", "'INSERT'", "'+'", "'CREATE'", "'/'", "'ONLY'", "'TABLE'", "'AS'", "'BY'", "'ELSE'", "'WHERE'", "'INTO'", "'END'", "'ON'", "'JOIN'", "'}'", "'VIEW'", "'THEN'", "'KEY'", "'ORDER'", "'SELECT'", "'WITH'", "'.'", "'DROP'", "'WHEN'", "'ROW'", "'CHAR'", "'INDEX'", "'ROWS'", "'|'", "'RUNSTATS'", "'FLOAT'", "')'", "'SET'", "STRING", "'*'", "'COUNT'", "'||'", "'-'", "'='", "OPERATOR", "NULLOPERATOR", "'AND'", "'OR'", "'NOT'", "'NULL'", "DIRECTION", "JOINTYPE", "'CROSS JOIN'", "TABLECOMBINATION", "'COLUMN'", "'DISTINCT'", "INTEGER", "WS", "'UNIQUE'", "'REPLACE'", "'RESUME'", "'NONE'", "'ALL'", "'ANY'", "'HASH'", "'RANGE'", "'DATE'", "'COLORDER'", "'ORGANIZATION'", "IDENTIFIER", "ANY" };
	public static final int RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_update = 3, RULE_delete = 4, RULE_createTable = 5, RULE_organization = 6, RULE_colOrder = 7, RULE_groupExp = 8, RULE_realGroupExp = 9, RULE_groupDef = 10, RULE_rangeExp = 11, RULE_nodeExp = 12, RULE_realNodeExp = 13, RULE_integerSet = 14, RULE_hashExp = 15, RULE_columnSet = 16, RULE_rangeType = 17, RULE_rangeSet = 18, RULE_deviceExp = 19, RULE_dropTable = 20, RULE_createView = 21, RULE_dropView = 22, RULE_createIndex = 23, RULE_dropIndex = 24, RULE_load = 25, RULE_any = 26, RULE_remainder = 27, RULE_indexDef = 28, RULE_colDef = 29, RULE_primaryKey = 30, RULE_notNull = 31, RULE_primary = 32, RULE_dataType = 33, RULE_char2 = 34, RULE_int2 = 35, RULE_long2 = 36, RULE_date2 = 37, RULE_float2 = 38, RULE_colList = 39, RULE_commonTableExpression = 40, RULE_fullSelect = 41, RULE_connectedSelect = 42, RULE_subSelect = 43, RULE_selectClause = 44, RULE_selecthow = 45, RULE_selectListEntry = 46, RULE_fromClause = 47, RULE_tableReference = 48, RULE_singleTable = 49, RULE_whereClause = 50, RULE_groupBy = 51, RULE_havingClause = 52, RULE_orderBy = 53, RULE_sortKey = 54, RULE_correlationClause = 55, RULE_fetchFirst = 56, RULE_tableName = 57, RULE_columnName = 58, RULE_searchCondition = 59, RULE_connectedSearchClause = 60, RULE_searchClause = 61, RULE_predicate = 62, RULE_operator = 63, RULE_expression = 64, RULE_caseCase = 65, RULE_identifier = 66, RULE_literal = 67;
	public static final String[] ruleNames = { "select", "runstats", "insert", "update", "delete", "createTable", "organization", "colOrder", "groupExp", "realGroupExp", "groupDef", "rangeExp", "nodeExp", "realNodeExp", "integerSet", "hashExp", "columnSet", "rangeType", "rangeSet", "deviceExp", "dropTable", "createView", "dropView", "createIndex", "dropIndex", "load", "any", "remainder", "indexDef", "colDef", "primaryKey", "notNull", "primary", "dataType", "char2", "int2", "long2", "date2", "float2", "colList", "commonTableExpression", "fullSelect", "connectedSelect", "subSelect", "selectClause", "selecthow", "selectListEntry", "fromClause", "tableReference", "singleTable", "whereClause", "groupBy", "havingClause", "orderBy", "sortKey", "correlationClause", "fetchFirst", "tableName", "columnName", "searchCondition", "connectedSearchClause", "searchClause", "predicate", "operator", "expression", "caseCase", "identifier", "literal" };

	public static final String _serializedATN = "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3X\u0362\4\2\t\2\4" + "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t" + "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22" + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31" + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!" + "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4" + ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t" + "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t=" + "\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\3\2\3\2\3\2\3\2\3\2\3" + "\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2" + "\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\7\2\u00b0\n\2" + "\f\2\16\2\u00b3\13\2\5\2\u00b5\n\2\3\2\3\2\3\2\3\2\5\2\u00bb\n\2\3\3\3" + "\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4\u00c5\n\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4\u00cd" + "\n\4\f\4\16\4\u00d0\13\4\3\4\3\4\5\4\u00d4\n\4\3\5\3\5\3\5\3\5\3\5\5\5" + "\u00db\n\5\3\5\3\5\3\5\5\5\u00e0\n\5\3\6\3\6\3\6\3\6\5\6\u00e6\n\6\3\7" + "\3\7\5\7\u00ea\n\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7\u00f2\n\7\f\7\16\7\u00f5" + "\13\7\3\7\3\7\5\7\u00f9\n\7\3\7\3\7\5\7\u00fd\n\7\3\7\5\7\u0100\n\7\3" + "\7\5\7\u0103\n\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\7\b\u010d\n\b\f\b\16" + "\b\u0110\13\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\7\t\u0119\n\t\f\t\16\t\u011c" + "\13\t\3\t\3\t\3\n\3\n\5\n\u0122\n\n\3\13\3\13\3\13\3\13\7\13\u0128\n\13" + "\f\13\16\13\u012b\13\13\3\13\3\13\3\13\3\13\5\13\u0131\n\13\5\13\u0133" + "\n\13\3\f\3\f\3\f\3\f\7\f\u0139\n\f\f\f\16\f\u013c\13\f\3\f\3\f\3\r\7" + "\r\u0141\n\r\f\r\16\r\u0144\13\r\3\16\3\16\5\16\u0148\n\16\3\17\3\17\5" + "\17\u014c\n\17\3\17\3\17\3\17\5\17\u0151\n\17\5\17\u0153\n\17\3\20\3\20" + "\3\20\3\20\7\20\u0159\n\20\f\20\16\20\u015c\13\20\3\20\3\20\3\21\3\21" + "\3\21\3\21\3\22\3\22\3\22\3\22\7\22\u0168\n\22\f\22\16\22\u016b\13\22" + "\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\7\24\u0179" + "\n\24\f\24\16\24\u017c\13\24\3\24\3\24\3\25\3\25\5\25\u0182\n\25\3\25" + "\3\25\3\25\5\25\u0187\n\25\5\25\u0189\n\25\3\26\3\26\3\26\3\26\3\27\3" + "\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\31\3\31\5\31\u019b\n\31" + "\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\7\31\u01a5\n\31\f\31\16\31\u01a8" + "\13\31\3\31\3\31\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\5\33" + "\u01b6\n\33\3\33\3\33\3\33\3\34\3\34\3\35\7\35\u01be\n\35\f\35\16\35\u01c1" + "\13\35\3\35\3\35\3\36\3\36\5\36\u01c7\n\36\3\37\3\37\3\37\5\37\u01cc\n" + "\37\3\37\5\37\u01cf\n\37\3 \3 \3 \3 \3 \3 \7 \u01d7\n \f \16 \u01da\13" + " \3 \3 \3!\3!\3!\3\"\3\"\3\"\3#\3#\3#\3#\3#\5#\u01e9\n#\3$\3$\3$\3$\3" + "$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3)\3)\7)\u01fc\n)\f)\16)\u01ff\13)\3" + ")\3)\3*\3*\3*\3*\3*\7*\u0208\n*\f*\16*\u020b\13*\3*\3*\5*\u020f\n*\3*" + "\3*\3*\3*\3*\3+\3+\3+\3+\3+\5+\u021b\n+\3+\7+\u021e\n+\f+\16+\u0221\13" + "+\3+\5+\u0224\n+\3+\5+\u0227\n+\3,\3,\3,\3,\3,\3,\5,\u022f\n,\3-\3-\3" + "-\5-\u0234\n-\3-\5-\u0237\n-\3-\5-\u023a\n-\3-\5-\u023d\n-\3-\5-\u0240" + "\n-\3.\3.\5.\u0244\n.\3.\3.\3.\3.\7.\u024a\n.\f.\16.\u024d\13.\5.\u024f" + "\n.\3/\3/\3\60\3\60\5\60\u0255\n\60\3\60\5\60\u0258\n\60\3\60\3\60\5\60" + "\u025c\n\60\3\60\5\60\u025f\n\60\5\60\u0261\n\60\3\61\3\61\3\61\3\61\7" + "\61\u0267\n\61\f\61\16\61\u026a\13\61\3\62\3\62\3\62\3\62\5\62\u0270\n" + "\62\3\62\3\62\3\62\3\62\3\62\3\62\5\62\u0278\n\62\3\62\3\62\3\62\3\62" + "\3\62\3\62\5\62\u0280\n\62\3\62\3\62\3\62\3\62\5\62\u0286\n\62\3\62\5" + "\62\u0289\n\62\3\62\3\62\5\62\u028d\n\62\3\62\3\62\3\62\3\62\3\62\5\62" + "\u0294\n\62\3\62\3\62\3\62\3\62\5\62\u029a\n\62\7\62\u029c\n\62\f\62\16" + "\62\u029f\13\62\3\63\3\63\5\63\u02a3\n\63\3\64\3\64\3\64\3\65\3\65\3\65" + "\3\65\3\65\7\65\u02ad\n\65\f\65\16\65\u02b0\13\65\3\66\3\66\3\66\3\67" + "\3\67\3\67\3\67\3\67\7\67\u02ba\n\67\f\67\16\67\u02bd\13\67\38\38\58\u02c1" + "\n8\38\38\58\u02c5\n8\58\u02c7\n8\39\59\u02ca\n9\39\39\3:\3:\3:\5:\u02d1" + "\n:\3:\3:\3:\3;\3;\3;\3;\5;\u02da\n;\3<\3<\3<\3<\5<\u02e0\n<\3=\3=\7=" + "\u02e4\n=\f=\16=\u02e7\13=\3>\3>\3>\3?\5?\u02ed\n?\3?\3?\3?\3?\3?\5?\u02f4" + "\n?\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\5@\u0302\n@\3A\3A\3B\3B\3B\3B" + "\3B\3B\7B\u030c\nB\fB\16B\u030f\13B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3" + "B\6B\u031d\nB\rB\16B\u031e\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3" + "B\3B\3B\3B\3B\3B\7B\u0335\nB\fB\16B\u0338\13B\3B\3B\3B\3B\5B\u033e\nB" + "\5B\u0340\nB\3B\3B\3B\3B\3B\3B\3B\3B\3B\7B\u034b\nB\fB\16B\u034e\13B\3" + "C\3C\3C\3C\3C\3D\3D\3E\5E\u0358\nE\3E\3E\3E\5E\u035d\nE\3E\5E\u0360\n" + "E\3E\3\u0142\4b\u0082F\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*," + ".\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086" + "\u0088\2\f\3\2MN\4\2\20\20\60\60\4\2\3\3\65\65\4\2IIPP\4\2//\62\62\3\2" + "@A\3\2=>\4\2\32\3299\4\2\30\30<<\5\2::TTWW\u0398\2\u00ba\3\2\2\2\4\u00bc" + "\3\2\2\2\6\u00c0\3\2\2\2\b\u00d5\3\2\2\2\n\u00e1\3\2\2\2\f\u00e7\3\2\2" + "\2\16\u0107\3\2\2\2\20\u0113\3\2\2\2\22\u0121\3\2\2\2\24\u0123\3\2\2\2" + "\26\u0134\3\2\2\2\30\u0142\3\2\2\2\32\u0147\3\2\2\2\34\u014b\3\2\2\2\36" + "\u0154\3\2\2\2 \u015f\3\2\2\2\"\u0163\3\2\2\2$\u016e\3\2\2\2&\u0174\3" + "\2\2\2(\u0181\3\2\2\2*\u018a\3\2\2\2,\u018e\3\2\2\2.\u0194\3\2\2\2\60" + "\u0198\3\2\2\2\62\u01ab\3\2\2\2\64\u01af\3\2\2\2\66\u01ba\3\2\2\28\u01bf" + "\3\2\2\2:\u01c4\3\2\2\2<\u01c8\3\2\2\2>\u01d0\3\2\2\2@\u01dd\3\2\2\2B" + "\u01e0\3\2\2\2D\u01e8\3\2\2\2F\u01ea\3\2\2\2H\u01ef\3\2\2\2J\u01f1\3\2" + "\2\2L\u01f3\3\2\2\2N\u01f5\3\2\2\2P\u01f7\3\2\2\2R\u0202\3\2\2\2T\u021a" + "\3\2\2\2V\u0228\3\2\2\2X\u0230\3\2\2\2Z\u0241\3\2\2\2\\\u0250\3\2\2\2" + "^\u0260\3\2\2\2`\u0262\3\2\2\2b\u0288\3\2\2\2d\u02a0\3\2\2\2f\u02a4\3" + "\2\2\2h\u02a7\3\2\2\2j\u02b1\3\2\2\2l\u02b4\3\2\2\2n\u02c6\3\2\2\2p\u02c9" + "\3\2\2\2r\u02cd\3\2\2\2t\u02d9\3\2\2\2v\u02df\3\2\2\2x\u02e1\3\2\2\2z" + "\u02e8\3\2\2\2|\u02ec\3\2\2\2~\u0301\3\2\2\2\u0080\u0303\3\2\2\2\u0082" + "\u033f\3\2\2\2\u0084\u034f\3\2\2\2\u0086\u0354\3\2\2\2\u0088\u035f\3\2" + "\2\2\u008a\u008b\5\6\4\2\u008b\u008c\7\2\2\3\u008c\u00bb\3\2\2\2\u008d" + "\u008e\5\b\5\2\u008e\u008f\7\2\2\3\u008f\u00bb\3\2\2\2\u0090\u0091\5\n" + "\6\2\u0091\u0092\7\2\2\3\u0092\u00bb\3\2\2\2\u0093\u0094\5\f\7\2\u0094" + "\u0095\7\2\2\3\u0095\u00bb\3\2\2\2\u0096\u0097\5\60\31\2\u0097\u0098\7" + "\2\2\3\u0098\u00bb\3\2\2\2\u0099\u009a\5,\27\2\u009a\u009b\7\2\2\3\u009b" + "\u00bb\3\2\2\2\u009c\u009d\5*\26\2\u009d\u009e\7\2\2\3\u009e\u00bb\3\2" + "\2\2\u009f\u00a0\5\62\32\2\u00a0\u00a1\7\2\2\3\u00a1\u00bb\3\2\2\2\u00a2" + "\u00a3\5.\30\2\u00a3\u00a4\7\2\2\3\u00a4\u00bb\3\2\2\2\u00a5\u00a6\5\64" + "\33\2\u00a6\u00a7\7\2\2\3\u00a7\u00bb\3\2\2\2\u00a8\u00a9\5\4\3\2\u00a9" + "\u00aa\7\2\2\3\u00aa\u00bb\3\2\2\2\u00ab\u00ac\7+\2\2\u00ac\u00b1\5R*" + "\2\u00ad\u00ae\7\13\2\2\u00ae\u00b0\5R*\2\u00af\u00ad\3\2\2\2\u00b0\u00b3" + "\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b5\3\2\2\2\u00b3" + "\u00b1\3\2\2\2\u00b4\u00ab\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00b6\3\2" + "\2\2\u00b6\u00b7\5T+\2\u00b7\u00b8\3\2\2\2\u00b8\u00b9\7\2\2\3\u00b9\u00bb" + "\3\2\2\2\u00ba\u008a\3\2\2\2\u00ba\u008d\3\2\2\2\u00ba\u0090\3\2\2\2\u00ba" + "\u0093\3\2\2\2\u00ba\u0096\3\2\2\2\u00ba\u0099\3\2\2\2\u00ba\u009c\3\2" + "\2\2\u00ba\u009f\3\2\2\2\u00ba\u00a2\3\2\2\2\u00ba\u00a5\3\2\2\2\u00ba" + "\u00a8\3\2\2\2\u00ba\u00b4\3\2\2\2\u00bb\3\3\2\2\2\u00bc\u00bd\7\64\2" + "\2\u00bd\u00be\7#\2\2\u00be\u00bf\5t;\2\u00bf\5\3\2\2\2\u00c0\u00c1\7" + "\27\2\2\u00c1\u00c2\7!\2\2\u00c2\u00d3\5t;\2\u00c3\u00c5\7\5\2\2\u00c4" + "\u00c3\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6\u00d4\5T" + "+\2\u00c7\u00c8\7\17\2\2\u00c8\u00c9\7\n\2\2\u00c9\u00ce\5\u0082B\2\u00ca" + "\u00cb\7\13\2\2\u00cb\u00cd\5\u0082B\2\u00cc\u00ca\3\2\2\2\u00cd\u00d0" + "\3\2\2\2\u00ce\u00cc\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d1\3\2\2\2\u00d0" + "\u00ce\3\2\2\2\u00d1\u00d2\7\66\2\2\u00d2\u00d4\3\2\2\2\u00d3\u00c4\3" + "\2\2\2\u00d3\u00c7\3\2\2\2\u00d4\7\3\2\2\2\u00d5\u00d6\7\21\2\2\u00d6" + "\u00d7\5t;\2\u00d7\u00da\7\67\2\2\u00d8\u00db\5v<\2\u00d9\u00db\5P)\2" + "\u00da\u00d8\3\2\2\2\u00da\u00d9\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc\u00dd" + "\7=\2\2\u00dd\u00df\5\u0082B\2\u00de\u00e0\5f\64\2\u00df\u00de\3\2\2\2" + "\u00df\u00e0\3\2\2\2\u00e0\t\3\2\2\2\u00e1\u00e2\7\22\2\2\u00e2\u00e3" + "\7\5\2\2\u00e3\u00e5\5t;\2\u00e4\u00e6\5f\64\2\u00e5\u00e4\3\2\2\2\u00e5" + "\u00e6\3\2\2\2\u00e6\13\3\2\2\2\u00e7\u00e9\7\31\2\2\u00e8\u00ea\7H\2" + "\2\u00e9\u00e8\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00eb\3\2\2\2\u00eb\u00ec" + "\7\34\2\2\u00ec\u00ed\5t;\2\u00ed\u00ee\7\n\2\2\u00ee\u00f3\5<\37\2\u00ef" + "\u00f0\7\13\2\2\u00f0\u00f2\5<\37\2\u00f1\u00ef\3\2\2\2\u00f2\u00f5\3" + "\2\2\2\u00f3\u00f1\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f8\3\2\2\2\u00f5" + "\u00f3\3\2\2\2\u00f6\u00f7\7\13\2\2\u00f7\u00f9\5> \2\u00f8\u00f6\3\2" + "\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fc\7\66\2\2\u00fb" + "\u00fd\5\20\t\2\u00fc\u00fb\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00ff\3" + "\2\2\2\u00fe\u0100\5\16\b\2\u00ff\u00fe\3\2\2\2\u00ff\u0100\3\2\2\2\u0100" + "\u0102\3\2\2\2\u0101\u0103\5\22\n\2\u0102\u0101\3\2\2\2\u0102\u0103\3" + "\2\2\2\u0103\u0104\3\2\2\2\u0104\u0105\5\32\16\2\u0105\u0106\5(\25\2\u0106" + "\r\3\2\2\2\u0107\u0108\7V\2\2\u0108\u0109\7\n\2\2\u0109\u010e\7J\2\2\u010a" + "\u010b\7\13\2\2\u010b\u010d\7J\2\2\u010c\u010a\3\2\2\2\u010d\u0110\3\2" + "\2\2\u010e\u010c\3\2\2\2\u010e\u010f\3\2\2\2\u010f\u0111\3\2\2\2\u0110" + "\u010e\3\2\2\2\u0111\u0112\7\66\2\2\u0112\17\3\2\2\2\u0113\u0114\7U\2" + "\2\u0114\u0115\7\n\2\2\u0115\u011a\7J\2\2\u0116\u0117\7\13\2\2\u0117\u0119" + "\7J\2\2\u0118\u0116\3\2\2\2\u0119\u011c\3\2\2\2\u011a\u0118\3\2\2\2\u011a" + "\u011b\3\2\2\2\u011b\u011d\3\2\2\2\u011c\u011a\3\2\2\2\u011d\u011e\7\66" + "\2\2\u011e\21\3\2\2\2\u011f\u0122\7O\2\2\u0120\u0122\5\24\13\2\u0121\u011f" + "\3\2\2\2\u0121\u0120\3\2\2\2\u0122\23\3\2\2\2\u0123\u0124\7\7\2\2\u0124" + "\u0129\5\26\f\2\u0125\u0126\7\63\2\2\u0126\u0128\5\26\f\2\u0127\u0125" + "\3\2\2\2\u0128\u012b\3\2\2\2\u0129\u0127\3\2\2\2\u0129\u012a\3\2\2\2\u012a" + "\u012c\3\2\2\2\u012b\u0129\3\2\2\2\u012c\u0132\7%\2\2\u012d\u0130\7\13" + "\2\2\u012e\u0131\5 \21\2\u012f\u0131\5$\23\2\u0130\u012e\3\2\2\2\u0130" + "\u012f\3\2\2\2\u0131\u0133\3\2\2\2\u0132\u012d\3\2\2\2\u0132\u0133\3\2" + "\2\2\u0133\25\3\2\2\2\u0134\u0135\7\7\2\2\u0135\u013a\7J\2\2\u0136\u0137" + "\7\63\2\2\u0137\u0139\7J\2\2\u0138\u0136\3\2\2\2\u0139\u013c\3\2\2\2\u013a" + "\u0138\3\2\2\2\u013a\u013b\3\2\2\2\u013b\u013d\3\2\2\2\u013c\u013a\3\2" + "\2\2\u013d\u013e\7%\2\2\u013e\27\3\2\2\2\u013f\u0141\13\2\2\2\u0140\u013f" + "\3\2\2\2\u0141\u0144\3\2\2\2\u0142\u0143\3\2\2\2\u0142\u0140\3\2\2\2\u0143" + "\31\3\2\2\2\u0144\u0142\3\2\2\2\u0145\u0148\7Q\2\2\u0146\u0148\5\34\17" + "\2\u0147\u0145\3\2\2\2\u0147\u0146\3\2\2\2\u0148\33\3\2\2\2\u0149\u014c" + "\7P\2\2\u014a\u014c\5\36\20\2\u014b\u0149\3\2\2\2\u014b\u014a\3\2\2\2" + "\u014c\u0152\3\2\2\2\u014d\u0150\7\13\2\2\u014e\u0151\5 \21\2\u014f\u0151" + "\5$\23\2\u0150\u014e\3\2\2\2\u0150\u014f\3\2\2\2\u0151\u0153\3\2\2\2\u0152" + "\u014d\3\2\2\2\u0152\u0153\3\2\2\2\u0153\35\3\2\2\2\u0154\u0155\7\7\2" + "\2\u0155\u015a\7J\2\2\u0156\u0157\7\63\2\2\u0157\u0159\7J\2\2\u0158\u0156" + "\3\2\2\2\u0159\u015c\3\2\2\2\u015a\u0158\3\2\2\2\u015a\u015b\3\2\2\2\u015b" + "\u015d\3\2\2\2\u015c\u015a\3\2\2\2\u015d\u015e\7%\2\2\u015e\37\3\2\2\2" + "\u015f\u0160\7R\2\2\u0160\u0161\7\13\2\2\u0161\u0162\5\"\22\2\u0162!\3" + "\2\2\2\u0163\u0164\7\7\2\2\u0164\u0169\5v<\2\u0165\u0166\7\63\2\2\u0166" + "\u0168\5v<\2\u0167\u0165\3\2\2\2\u0168\u016b\3\2\2\2\u0169\u0167\3\2\2" + "\2\u0169\u016a\3\2\2\2\u016a\u016c\3\2\2\2\u016b\u0169\3\2\2\2\u016c\u016d" + "\7%\2\2\u016d#\3\2\2\2\u016e\u016f\7S\2\2\u016f\u0170\7\13\2\2\u0170\u0171" + "\5v<\2\u0171\u0172\7\13\2\2\u0172\u0173\5&\24\2\u0173%\3\2\2\2\u0174\u0175" + "\7\7\2\2\u0175\u017a\5\30\r\2\u0176\u0177\7\63\2\2\u0177\u0179\5\30\r" + "\2\u0178\u0176\3\2\2\2\u0179\u017c\3\2\2\2\u017a\u0178\3\2\2\2\u017a\u017b" + "\3\2\2\2\u017b\u017d\3\2\2\2\u017c\u017a\3\2\2\2\u017d\u017e\7%\2\2\u017e" + "\'\3\2\2\2\u017f\u0182\7P\2\2\u0180\u0182\5\36\20\2\u0181\u017f\3\2\2" + "\2\u0181\u0180\3\2\2\2\u0182\u0188\3\2\2\2\u0183\u0186\7\13\2\2\u0184" + "\u0187\5 \21\2\u0185\u0187\5\30\r\2\u0186\u0184\3\2\2\2\u0186\u0185\3" + "\2\2\2\u0187\u0189\3\2\2\2\u0188\u0183\3\2\2\2\u0188\u0189\3\2\2\2\u0189" + ")\3\2\2\2\u018a\u018b\7-\2\2\u018b\u018c\7\34\2\2\u018c\u018d\5t;\2\u018d" + "+\3\2\2\2\u018e\u018f\7\31\2\2\u018f\u0190\7&\2\2\u0190\u0191\5t;\2\u0191" + "\u0192\7\35\2\2\u0192\u0193\5T+\2\u0193-\3\2\2\2\u0194\u0195\7-\2\2\u0195" + "\u0196\7&\2\2\u0196\u0197\5t;\2\u0197/\3\2\2\2\u0198\u019a\7\31\2\2\u0199" + "\u019b\7L\2\2\u019a\u0199\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019c\3\2" + "\2\2\u019c\u019d\7\61\2\2\u019d\u019e\5t;\2\u019e\u019f\7#\2\2\u019f\u01a0" + "\5t;\2\u01a0\u01a1\7\n\2\2\u01a1\u01a6\5:\36\2\u01a2\u01a3\7\13\2\2\u01a3" + "\u01a5\5:\36\2\u01a4\u01a2\3\2\2\2\u01a5\u01a8\3\2\2\2\u01a6\u01a4\3\2" + "\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a9\3\2\2\2\u01a8\u01a6\3\2\2\2\u01a9" + "\u01aa\7\66\2\2\u01aa\61\3\2\2\2\u01ab\u01ac\7-\2\2\u01ac\u01ad\7\61\2" + "\2\u01ad\u01ae\5t;\2\u01ae\63\3\2\2\2\u01af\u01b0\7\16\2\2\u01b0\u01b1" + "\t\2\2\2\u01b1\u01b2\7!\2\2\u01b2\u01b5\5t;\2\u01b3\u01b4\7\r\2\2\u01b4" + "\u01b6\5\66\34\2\u01b5\u01b3\3\2\2\2\u01b5\u01b6\3\2\2\2\u01b6\u01b7\3" + "\2\2\2\u01b7\u01b8\7\5\2\2\u01b8\u01b9\58\35\2\u01b9\65\3\2\2\2\u01ba" + "\u01bb\13\2\2\2\u01bb\67\3\2\2\2\u01bc\u01be\13\2\2\2\u01bd\u01bc\3\2" + "\2\2\u01be\u01c1\3\2\2\2\u01bf\u01bd\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0" + "\u01c2\3\2\2\2\u01c1\u01bf\3\2\2\2\u01c2\u01c3\7\2\2\3\u01c39\3\2\2\2" + "\u01c4\u01c6\5v<\2\u01c5\u01c7\7D\2\2\u01c6\u01c5\3\2\2\2\u01c6\u01c7" + "\3\2\2\2\u01c7;\3\2\2\2\u01c8\u01c9\5v<\2\u01c9\u01cb\5D#\2\u01ca\u01cc" + "\5@!\2\u01cb\u01ca\3\2\2\2\u01cb\u01cc\3\2\2\2\u01cc\u01ce\3\2\2\2\u01cd" + "\u01cf\5B\"\2\u01ce\u01cd\3\2\2\2\u01ce\u01cf\3\2\2\2\u01cf=\3\2\2\2\u01d0" + "\u01d1\7\f\2\2\u01d1\u01d2\7(\2\2\u01d2\u01d3\7\n\2\2\u01d3\u01d8\5v<" + "\2\u01d4\u01d5\7\13\2\2\u01d5\u01d7\5v<\2\u01d6\u01d4\3\2\2\2\u01d7\u01da" + "\3\2\2\2\u01d8\u01d6\3\2\2\2\u01d8\u01d9\3\2\2\2\u01d9\u01db\3\2\2\2\u01da" + "\u01d8\3\2\2\2\u01db\u01dc\7\66\2\2\u01dc?\3\2\2\2\u01dd\u01de\7B\2\2" + "\u01de\u01df\7C\2\2\u01dfA\3\2\2\2\u01e0\u01e1\7\f\2\2\u01e1\u01e2\7(" + "\2\2\u01e2C\3\2\2\2\u01e3\u01e9\5F$\2\u01e4\u01e9\5H%\2\u01e5\u01e9\5" + "J&\2\u01e6\u01e9\5L\'\2\u01e7\u01e9\5N(\2\u01e8\u01e3\3\2\2\2\u01e8\u01e4" + "\3\2\2\2\u01e8\u01e5\3\2\2\2\u01e8\u01e6\3\2\2\2\u01e8\u01e7\3\2\2\2\u01e9" + "E\3\2\2\2\u01ea\u01eb\t\3\2\2\u01eb\u01ec\7\n\2\2\u01ec\u01ed\7J\2\2\u01ed" + "\u01ee\7\66\2\2\u01eeG\3\2\2\2\u01ef\u01f0\7\4\2\2\u01f0I\3\2\2\2\u01f1" + "\u01f2\7\23\2\2\u01f2K\3\2\2\2\u01f3\u01f4\7T\2\2\u01f4M\3\2\2\2\u01f5" + "\u01f6\t\4\2\2\u01f6O\3\2\2\2\u01f7\u01f8\7\n\2\2\u01f8\u01fd\5v<\2\u01f9" + "\u01fa\7\13\2\2\u01fa\u01fc\5v<\2\u01fb\u01f9\3\2\2\2\u01fc\u01ff\3\2" + "\2\2\u01fd\u01fb\3\2\2\2\u01fd\u01fe\3\2\2\2\u01fe\u0200\3\2\2\2\u01ff" + "\u01fd\3\2\2\2\u0200\u0201\7\66\2\2\u0201Q\3\2\2\2\u0202\u020e\7W\2\2" + "\u0203\u0204\7\n\2\2\u0204\u0209\5v<\2\u0205\u0206\7\13\2\2\u0206\u0208" + "\5v<\2\u0207\u0205\3\2\2\2\u0208\u020b\3\2\2\2\u0209\u0207\3\2\2\2\u0209" + "\u020a\3\2\2\2\u020a\u020c\3\2\2\2\u020b\u0209\3\2\2\2\u020c\u020d\7\66" + "\2\2\u020d\u020f\3\2\2\2\u020e\u0203\3\2\2\2\u020e\u020f\3\2\2\2\u020f" + "\u0210\3\2\2\2\u0210\u0211\7\35\2\2\u0211\u0212\7\n\2\2\u0212\u0213\5" + "T+\2\u0213\u0214\7\66\2\2\u0214S\3\2\2\2\u0215\u021b\5X-\2\u0216\u0217" + "\7\n\2\2\u0217\u0218\5T+\2\u0218\u0219\7\66\2\2\u0219\u021b\3\2\2\2\u021a" + "\u0215\3\2\2\2\u021a\u0216\3\2\2\2\u021b\u021f\3\2\2\2\u021c\u021e\5V" + ",\2\u021d\u021c\3\2\2\2\u021e\u0221\3\2\2\2\u021f\u021d\3\2\2\2\u021f" + "\u0220\3\2\2\2\u0220\u0223\3\2\2\2\u0221\u021f\3\2\2\2\u0222\u0224\5l" + "\67\2\u0223\u0222\3\2\2\2\u0223\u0224\3\2\2\2\u0224\u0226\3\2\2\2\u0225" + "\u0227\5r:\2\u0226\u0225\3\2\2\2\u0226\u0227\3\2\2\2\u0227U\3\2\2\2\u0228" + "\u022e\7G\2\2\u0229\u022f\5X-\2\u022a\u022b\7\n\2\2\u022b\u022c\5T+\2" + "\u022c\u022d\7\66\2\2\u022d\u022f\3\2\2\2\u022e\u0229\3\2\2\2\u022e\u022a" + "\3\2\2\2\u022fW\3\2\2\2\u0230\u0231\5Z.\2\u0231\u0233\5`\61\2\u0232\u0234" + "\5f\64\2\u0233\u0232\3\2\2\2\u0233\u0234\3\2\2\2\u0234\u0236\3\2\2\2\u0235" + "\u0237\5h\65\2\u0236\u0235\3\2\2\2\u0236\u0237\3\2\2\2\u0237\u0239\3\2" + "\2\2\u0238\u023a\5j\66\2\u0239\u0238\3\2\2\2\u0239\u023a\3\2\2\2\u023a" + "\u023c\3\2\2\2\u023b\u023d\5l\67\2\u023c\u023b\3\2\2\2\u023c\u023d\3\2" + "\2\2\u023d\u023f\3\2\2\2\u023e\u0240\5r:\2\u023f\u023e\3\2\2\2\u023f\u0240" + "\3\2\2\2\u0240Y\3\2\2\2\u0241\u0243\7*\2\2\u0242\u0244\5\\/\2\u0243\u0242" + "\3\2\2\2\u0243\u0244\3\2\2\2\u0244\u024e\3\2\2\2\u0245\u024f\79\2\2\u0246" + "\u024b\5^\60\2\u0247\u0248\7\13\2\2\u0248\u024a\5^\60\2\u0249\u0247\3" + "\2\2\2\u024a\u024d\3\2\2\2\u024b\u0249\3\2\2\2\u024b\u024c\3\2\2\2\u024c" + "\u024f\3\2\2\2\u024d\u024b\3\2\2\2\u024e\u0245\3\2\2\2\u024e\u0246\3\2" + "\2\2\u024f[\3\2\2\2\u0250\u0251\t\5\2\2\u0251]\3\2\2\2\u0252\u0254\5v" + "<\2\u0253\u0255\7\35\2\2\u0254\u0253\3\2\2\2\u0254\u0255\3\2\2\2\u0255" + "\u0257\3\2\2\2\u0256\u0258\7W\2\2\u0257\u0256\3\2\2\2\u0257\u0258\3\2" + "\2\2\u0258\u0261\3\2\2\2\u0259\u025b\5\u0082B\2\u025a\u025c\7\35\2\2\u025b" + "\u025a\3\2\2\2\u025b\u025c\3\2\2\2\u025c\u025e\3\2\2\2\u025d\u025f\7W" + "\2\2\u025e\u025d\3\2\2\2\u025e\u025f\3\2\2\2\u025f\u0261\3\2\2\2\u0260" + "\u0252\3\2\2\2\u0260\u0259\3\2\2\2\u0261_\3\2\2\2\u0262\u0263\7\5\2\2" + "\u0263\u0268\5b\62\2\u0264\u0265\7\13\2\2\u0265\u0267\5b\62\2\u0266\u0264" + "\3\2\2\2\u0267\u026a\3\2\2\2\u0268\u0266\3\2\2\2\u0268\u0269\3\2\2\2\u0269" + "a\3\2\2\2\u026a\u0268\3\2\2\2\u026b\u026c\b\62\1\2\u026c\u026d\7\n\2\2" + "\u026d\u026f\5b\62\2\u026e\u0270\7E\2\2\u026f\u026e\3\2\2\2\u026f\u0270" + "\3\2\2\2\u0270\u0271\3\2\2\2\u0271\u0272\7$\2\2\u0272\u0273\5b\62\2\u0273" + "\u0274\7#\2\2\u0274\u0275\5x=\2\u0275\u0277\7\66\2\2\u0276\u0278\5p9\2" + "\u0277\u0276\3\2\2\2\u0277\u0278\3\2\2\2\u0278\u0289\3\2\2\2\u0279\u027a" + "\7\n\2\2\u027a\u027b\5b\62\2\u027b\u027c\7F\2\2\u027c\u027d\5b\62\2\u027d" + "\u027f\7\66\2\2\u027e\u0280\5p9\2\u027f\u027e\3\2\2\2\u027f\u0280\3\2" + "\2\2\u0280\u0289\3\2\2\2\u0281\u0282\7\n\2\2\u0282\u0283\5T+\2\u0283\u0285" + "\7\66\2\2\u0284\u0286\5p9\2\u0285\u0284\3\2\2\2\u0285\u0286\3\2\2\2\u0286" + "\u0289\3\2\2\2\u0287\u0289\5d\63\2\u0288\u026b\3\2\2\2\u0288\u0279\3\2" + "\2\2\u0288\u0281\3\2\2\2\u0288\u0287\3\2\2\2\u0289\u029d\3\2\2\2\u028a" + "\u028c\f\b\2\2\u028b\u028d\7E\2\2\u028c\u028b\3\2\2\2\u028c\u028d\3\2" + "\2\2\u028d\u028e\3\2\2\2\u028e\u028f\7$\2\2\u028f\u0290\5b\62\2\u0290" + "\u0291\7#\2\2\u0291\u0293\5x=\2\u0292\u0294\5p9\2\u0293\u0292\3\2\2\2" + "\u0293\u0294\3\2\2\2\u0294\u029c\3\2\2\2\u0295\u0296\f\7\2\2\u0296\u0297" + "\7F\2\2\u0297\u0299\5b\62\2\u0298\u029a\5p9\2\u0299\u0298\3\2\2\2\u0299" + "\u029a\3\2\2\2\u029a\u029c\3\2\2\2\u029b\u028a\3\2\2\2\u029b\u0295\3\2" + "\2\2\u029c\u029f\3\2\2\2\u029d\u029b\3\2\2\2\u029d\u029e\3\2\2\2\u029e" + "c\3\2\2\2\u029f\u029d\3\2\2\2\u02a0\u02a2\5t;\2\u02a1\u02a3\5p9\2\u02a2" + "\u02a1\3\2\2\2\u02a2\u02a3\3\2\2\2\u02a3e\3\2\2\2\u02a4\u02a5\7 \2\2\u02a5" + "\u02a6\5x=\2\u02a6g\3\2\2\2\u02a7\u02a8\7\b\2\2\u02a8\u02a9\7\36\2\2\u02a9" + "\u02ae\5v<\2\u02aa\u02ab\7\13\2\2\u02ab\u02ad\5v<\2\u02ac\u02aa\3\2\2" + "\2\u02ad\u02b0\3\2\2\2\u02ae\u02ac\3\2\2\2\u02ae\u02af\3\2\2\2\u02afi" + "\3\2\2\2\u02b0\u02ae\3\2\2\2\u02b1\u02b2\7\26\2\2\u02b2\u02b3\5x=\2\u02b3" + "k\3\2\2\2\u02b4\u02b5\7)\2\2\u02b5\u02b6\7\36\2\2\u02b6\u02bb\5n8\2\u02b7" + "\u02b8\7\13\2\2\u02b8\u02ba\5n8\2\u02b9\u02b7\3\2\2\2\u02ba\u02bd\3\2" + "\2\2\u02bb\u02b9\3\2\2\2\u02bb\u02bc\3\2\2\2\u02bcm\3\2\2\2\u02bd\u02bb" + "\3\2\2\2\u02be\u02c0\7J\2\2\u02bf\u02c1\7D\2\2\u02c0\u02bf\3\2\2\2\u02c0" + "\u02c1\3\2\2\2\u02c1\u02c7\3\2\2\2\u02c2\u02c4\5v<\2\u02c3\u02c5\7D\2" + "\2\u02c4\u02c3\3\2\2\2\u02c4\u02c5\3\2\2\2\u02c5\u02c7\3\2\2\2\u02c6\u02be" + "\3\2\2\2\u02c6\u02c2\3\2\2\2\u02c7o\3\2\2\2\u02c8\u02ca\7\35\2\2\u02c9" + "\u02c8\3\2\2\2\u02c9\u02ca\3\2\2\2\u02ca\u02cb\3\2\2\2\u02cb\u02cc\7W" + "\2\2\u02ccq\3\2\2\2\u02cd\u02ce\7\25\2\2\u02ce\u02d0\7\24\2\2\u02cf\u02d1" + "\7J\2\2\u02d0\u02cf\3\2\2\2\u02d0\u02d1\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d2" + "\u02d3\t\6\2\2\u02d3\u02d4\7\33\2\2\u02d4s\3\2\2\2\u02d5\u02da\7W\2\2" + "\u02d6\u02d7\7W\2\2\u02d7\u02d8\7,\2\2\u02d8\u02da\7W\2\2\u02d9\u02d5" + "\3\2\2\2\u02d9\u02d6\3\2\2\2\u02dau\3\2\2\2\u02db\u02e0\7W\2\2\u02dc\u02dd" + "\7W\2\2\u02dd\u02de\7,\2\2\u02de\u02e0\7W\2\2\u02df\u02db\3\2\2\2\u02df" + "\u02dc\3\2\2\2\u02e0w\3\2\2\2\u02e1\u02e5\5|?\2\u02e2\u02e4\5z>\2\u02e3" + "\u02e2\3\2\2\2\u02e4\u02e7\3\2\2\2\u02e5\u02e3\3\2\2\2\u02e5\u02e6\3\2" + "\2\2\u02e6y\3\2\2\2\u02e7\u02e5\3\2\2\2\u02e8\u02e9\t\7\2\2\u02e9\u02ea" + "\5|?\2\u02ea{\3\2\2\2\u02eb\u02ed\7B\2\2\u02ec\u02eb\3\2\2\2\u02ec\u02ed" + "\3\2\2\2\u02ed\u02f3\3\2\2\2\u02ee\u02f4\5~@\2\u02ef\u02f0\7\n\2\2\u02f0" + "\u02f1\5x=\2\u02f1\u02f2\7\66\2\2\u02f2\u02f4\3\2\2\2\u02f3\u02ee\3\2" + "\2\2\u02f3\u02ef\3\2\2\2\u02f4}\3\2\2\2\u02f5\u02f6\5\u0082B\2\u02f6\u02f7" + "\5\u0080A\2\u02f7\u02f8\5\u0082B\2\u02f8\u0302\3\2\2\2\u02f9\u02fa\5\u0082" + "B\2\u02fa\u02fb\7?\2\2\u02fb\u0302\3\2\2\2\u02fc\u02fd\7\6\2\2\u02fd\u02fe" + "\7\n\2\2\u02fe\u02ff\5X-\2\u02ff\u0300\7\66\2\2\u0300\u0302\3\2\2\2\u0301" + "\u02f5\3\2\2\2\u0301\u02f9\3\2\2\2\u0301\u02fc\3\2\2\2\u0302\177\3\2\2" + "\2\u0303\u0304\t\b\2\2\u0304\u0081\3\2\2\2\u0305\u0306\bB\1\2\u0306\u0307" + "\5\u0086D\2\u0307\u0308\7\n\2\2\u0308\u030d\5\u0082B\2\u0309\u030a\7\13" + "\2\2\u030a\u030c\5\u0082B\2\u030b\u0309\3\2\2\2\u030c\u030f\3\2\2\2\u030d" + "\u030b\3\2\2\2\u030d\u030e\3\2\2\2\u030e\u0310\3\2\2\2\u030f\u030d\3\2" + "\2\2\u0310\u0311\7\66\2\2\u0311\u0340\3\2\2\2\u0312\u0313\7:\2\2\u0313" + "\u0314\7\n\2\2\u0314\u0315\7I\2\2\u0315\u0316\5\u0082B\2\u0316\u0317\7" + "\66\2\2\u0317\u0340\3\2\2\2\u0318\u0319\7\n\2\2\u0319\u031c\5\u0082B\2" + "\u031a\u031b\7\13\2\2\u031b\u031d\5\u0082B\2\u031c\u031a\3\2\2\2\u031d" + "\u031e\3\2\2\2\u031e\u031c\3\2\2\2\u031e\u031f\3\2\2\2\u031f\u0320\3\2" + "\2\2\u0320\u0321\7\66\2\2\u0321\u0340\3\2\2\2\u0322\u0323\7:\2\2\u0323" + "\u0324\7\n\2\2\u0324\u0325\79\2\2\u0325\u0340\7\66\2\2\u0326\u0340\5\u0088" + "E\2\u0327\u0340\5v<\2\u0328\u0329\7\n\2\2\u0329\u032a\5X-\2\u032a\u032b" + "\7\66\2\2\u032b\u0340\3\2\2\2\u032c\u032d\7\n\2\2\u032d\u032e\5\u0082" + "B\2\u032e\u032f\7\66\2\2\u032f\u0340\3\2\2\2\u0330\u0340\7C\2\2\u0331" + "\u0332\7\t\2\2\u0332\u0336\5\u0084C\2\u0333\u0335\5\u0084C\2\u0334\u0333" + "\3\2\2\2\u0335\u0338\3\2\2\2\u0336\u0334\3\2\2\2\u0336\u0337\3\2\2\2\u0337" + "\u0339\3\2\2\2\u0338\u0336\3\2\2\2\u0339\u033a\7\37\2\2\u033a\u033b\5" + "\u0082B\2\u033b\u033d\7\"\2\2\u033c\u033e\7\t\2\2\u033d\u033c\3\2\2\2" + "\u033d\u033e\3\2\2\2\u033e\u0340\3\2\2\2\u033f\u0305\3\2\2\2\u033f\u0312" + "\3\2\2\2\u033f\u0318\3\2\2\2\u033f\u0322\3\2\2\2\u033f\u0326\3\2\2\2\u033f" + "\u0327\3\2\2\2\u033f\u0328\3\2\2\2\u033f\u032c\3\2\2\2\u033f\u0330\3\2" + "\2\2\u033f\u0331\3\2\2\2\u0340\u034c\3\2\2\2\u0341\u0342\f\17\2\2\u0342" + "\u0343\t\t\2\2\u0343\u034b\5\u0082B\20\u0344\u0345\f\16\2\2\u0345\u0346" + "\t\n\2\2\u0346\u034b\5\u0082B\17\u0347\u0348\f\r\2\2\u0348\u0349\7;\2" + "\2\u0349\u034b\5\u0082B\16\u034a\u0341\3\2\2\2\u034a\u0344\3\2\2\2\u034a" + "\u0347\3\2\2\2\u034b\u034e\3\2\2\2\u034c\u034a\3\2\2\2\u034c\u034d\3\2" + "\2\2\u034d\u0083\3\2\2\2\u034e\u034c\3\2\2\2\u034f\u0350\7.\2\2\u0350" + "\u0351\5x=\2\u0351\u0352\7\'\2\2\u0352\u0353\5\u0082B\2\u0353\u0085\3" + "\2\2\2\u0354\u0355\t\13\2\2\u0355\u0087\3\2\2\2\u0356\u0358\7<\2\2\u0357" + "\u0356\3\2\2\2\u0357\u0358\3\2\2\2\u0358\u0359\3\2\2\2\u0359\u035c\7J" + "\2\2\u035a\u035b\7,\2\2\u035b\u035d\7J\2\2\u035c\u035a\3\2\2\2\u035c\u035d" + "\3\2\2\2\u035d\u0360\3\2\2\2\u035e\u0360\78\2\2\u035f\u0357\3\2\2\2\u035f" + "\u035e\3\2\2\2\u0360\u0089\3\2\2\2d\u00b1\u00b4\u00ba\u00c4\u00ce\u00d3" + "\u00da\u00df\u00e5\u00e9\u00f3\u00f8\u00fc\u00ff\u0102\u010e\u011a\u0121" + "\u0129\u0130\u0132\u013a\u0142\u0147\u014b\u0150\u0152\u015a\u0169\u017a" + "\u0181\u0186\u0188\u019a\u01a6\u01b5\u01bf\u01c6\u01cb\u01ce\u01d8\u01e8" + "\u01fd\u0209\u020e\u021a\u021f\u0223\u0226\u022e\u0233\u0236\u0239\u023c" + "\u023f\u0243\u024b\u024e\u0254\u0257\u025b\u025e\u0260\u0268\u026f\u0277" + "\u027f\u0285\u0288\u028c\u0293\u0299\u029b\u029d\u02a2\u02ae\u02bb\u02c0" + "\u02c4\u02c6\u02c9\u02d0\u02d9\u02df\u02e5\u02ec\u02f3\u0301\u030d\u031e" + "\u0336\u033d\u033f\u034a\u034c\u0357\u035c\u035f";

	public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());

	static
	{
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++)
		{
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}

	public SelectParser(TokenStream input)
	{
		super(input);
		_interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
	}

	public final AnyContext any() throws RecognitionException
	{
		AnyContext _localctx = new AnyContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_any);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(440);
				matchWildcard();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final CaseCaseContext caseCase() throws RecognitionException
	{
		CaseCaseContext _localctx = new CaseCaseContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_caseCase);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(845);
				match(44);
				setState(846);
				searchCondition();
				setState(847);
				match(37);
				setState(848);
				expression(0);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final Char2Context char2() throws RecognitionException
	{
		Char2Context _localctx = new Char2Context(_ctx, getState());
		enterRule(_localctx, 68, RULE_char2);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(488);
				_la = _input.LA(1);
				if (!(_la == 14 || _la == 46))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(489);
				match(8);
				setState(490);
				match(INTEGER);
				setState(491);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ColDefContext colDef() throws RecognitionException
	{
		ColDefContext _localctx = new ColDefContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_colDef);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(454);
				columnName();
				setState(455);
				dataType();
				setState(457);
				_la = _input.LA(1);
				if (_la == NOT)
				{
					{
						setState(456);
						notNull();
					}
				}

				setState(460);
				_la = _input.LA(1);
				if (_la == 10)
				{
					{
						setState(459);
						primary();
					}
				}

			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ColListContext colList() throws RecognitionException
	{
		ColListContext _localctx = new ColListContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_colList);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(501);
				match(8);
				setState(502);
				columnName();
				setState(507);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(503);
							match(9);
							setState(504);
							columnName();
						}
					}
					setState(509);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(510);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ColOrderContext colOrder() throws RecognitionException
	{
		ColOrderContext _localctx = new ColOrderContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_colOrder);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(273);
				match(COLORDER);
				setState(274);
				match(8);
				setState(275);
				match(INTEGER);
				setState(280);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(276);
							match(9);
							setState(277);
							match(INTEGER);
						}
					}
					setState(282);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(283);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ColumnNameContext columnName() throws RecognitionException
	{
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_columnName);
		try
		{
			setState(733);
			switch (getInterpreter().adaptivePredict(_input, 83, _ctx))
			{
				case 1:
					_localctx = new Col1PartContext(_localctx);
					enterOuterAlt(_localctx, 1);
				{
					setState(729);
					match(IDENTIFIER);
				}
					break;

				case 2:
					_localctx = new Col2PartContext(_localctx);
					enterOuterAlt(_localctx, 2);
				{
					{
						setState(730);
						match(IDENTIFIER);
						setState(731);
						match(42);
						setState(732);
						match(IDENTIFIER);
					}
				}
					break;
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ColumnSetContext columnSet() throws RecognitionException
	{
		ColumnSetContext _localctx = new ColumnSetContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_columnSet);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(353);
				match(5);
				setState(354);
				columnName();
				setState(359);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 49)
				{
					{
						{
							setState(355);
							match(49);
							setState(356);
							columnName();
						}
					}
					setState(361);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(362);
				match(35);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final CommonTableExpressionContext commonTableExpression() throws RecognitionException
	{
		CommonTableExpressionContext _localctx = new CommonTableExpressionContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_commonTableExpression);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(512);
				match(IDENTIFIER);
				setState(524);
				_la = _input.LA(1);
				if (_la == 8)
				{
					{
						setState(513);
						match(8);
						setState(514);
						columnName();
						setState(519);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la == 9)
						{
							{
								{
									setState(515);
									match(9);
									setState(516);
									columnName();
								}
							}
							setState(521);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(522);
						match(52);
					}
				}

				setState(526);
				match(27);
				setState(527);
				match(8);
				setState(528);
				fullSelect();
				setState(529);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ConnectedSearchClauseContext connectedSearchClause() throws RecognitionException
	{
		ConnectedSearchClauseContext _localctx = new ConnectedSearchClauseContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_connectedSearchClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(742);
				_la = _input.LA(1);
				if (!(_la == AND || _la == OR))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(743);
				searchClause();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ConnectedSelectContext connectedSelect() throws RecognitionException
	{
		ConnectedSelectContext _localctx = new ConnectedSelectContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_connectedSelect);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(550);
				match(TABLECOMBINATION);
				setState(556);
				switch (_input.LA(1))
				{
					case 40:
					{
						setState(551);
						subSelect();
					}
						break;
					case 8:
					{
						setState(552);
						match(8);
						setState(553);
						fullSelect();
						setState(554);
						match(52);
					}
						break;
					default:
						throw new NoViableAltException(this);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final CorrelationClauseContext correlationClause() throws RecognitionException
	{
		CorrelationClauseContext _localctx = new CorrelationClauseContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_correlationClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(711);
				_la = _input.LA(1);
				if (_la == 27)
				{
					{
						setState(710);
						match(27);
					}
				}

				setState(713);
				match(IDENTIFIER);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final CreateIndexContext createIndex() throws RecognitionException
	{
		CreateIndexContext _localctx = new CreateIndexContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_createIndex);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(406);
				match(23);
				setState(408);
				_la = _input.LA(1);
				if (_la == UNIQUE)
				{
					{
						setState(407);
						match(UNIQUE);
					}
				}

				setState(410);
				match(47);
				setState(411);
				tableName();
				setState(412);
				match(33);
				setState(413);
				tableName();
				setState(414);
				match(8);
				setState(415);
				indexDef();
				setState(420);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(416);
							match(9);
							setState(417);
							indexDef();
						}
					}
					setState(422);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(423);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final CreateTableContext createTable() throws RecognitionException
	{
		CreateTableContext _localctx = new CreateTableContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_createTable);
		int _la;
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(229);
				match(23);
				setState(231);
				_la = _input.LA(1);
				if (_la == COLUMN)
				{
					{
						setState(230);
						match(COLUMN);
					}
				}

				setState(233);
				match(26);
				setState(234);
				tableName();
				setState(235);
				match(8);
				setState(236);
				colDef();
				setState(241);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 10, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						{
							{
								setState(237);
								match(9);
								setState(238);
								colDef();
							}
						}
					}
					setState(243);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 10, _ctx);
				}
				setState(246);
				_la = _input.LA(1);
				if (_la == 9)
				{
					{
						setState(244);
						match(9);
						setState(245);
						primaryKey();
					}
				}

				setState(248);
				match(52);
				setState(250);
				_la = _input.LA(1);
				if (_la == COLORDER)
				{
					{
						setState(249);
						colOrder();
					}
				}

				setState(253);
				_la = _input.LA(1);
				if (_la == ORGANIZATION)
				{
					{
						setState(252);
						organization();
					}
				}

				setState(256);
				switch (getInterpreter().adaptivePredict(_input, 14, _ctx))
				{
					case 1:
					{
						setState(255);
						groupExp();
					}
						break;
				}
				setState(258);
				nodeExp();
				setState(259);
				deviceExp();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final CreateViewContext createView() throws RecognitionException
	{
		CreateViewContext _localctx = new CreateViewContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_createView);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(396);
				match(23);
				setState(397);
				match(36);
				setState(398);
				tableName();
				setState(399);
				match(27);
				setState(400);
				fullSelect();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final DataTypeContext dataType() throws RecognitionException
	{
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_dataType);
		try
		{
			setState(486);
			switch (_input.LA(1))
			{
				case 14:
				case 46:
					enterOuterAlt(_localctx, 1);
				{
					setState(481);
					char2();
				}
					break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(482);
					int2();
				}
					break;
				case 17:
					enterOuterAlt(_localctx, 3);
				{
					setState(483);
					long2();
				}
					break;
				case DATE:
					enterOuterAlt(_localctx, 4);
				{
					setState(484);
					date2();
				}
					break;
				case 1:
				case 51:
					enterOuterAlt(_localctx, 5);
				{
					setState(485);
					float2();
				}
					break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final Date2Context date2() throws RecognitionException
	{
		Date2Context _localctx = new Date2Context(_ctx, getState());
		enterRule(_localctx, 74, RULE_date2);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(497);
				match(DATE);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final DeleteContext delete() throws RecognitionException
	{
		DeleteContext _localctx = new DeleteContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_delete);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(223);
				match(16);
				setState(224);
				match(3);
				setState(225);
				tableName();
				setState(227);
				_la = _input.LA(1);
				if (_la == 30)
				{
					{
						setState(226);
						whereClause();
					}
				}

			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final DeviceExpContext deviceExp() throws RecognitionException
	{
		DeviceExpContext _localctx = new DeviceExpContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_deviceExp);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(383);
				switch (_input.LA(1))
				{
					case ALL:
					{
						setState(381);
						match(ALL);
					}
						break;
					case 5:
					{
						setState(382);
						integerSet();
					}
						break;
					default:
						throw new NoViableAltException(this);
				}
				setState(390);
				_la = _input.LA(1);
				if (_la == 9)
				{
					{
						setState(385);
						match(9);
						setState(388);
						switch (getInterpreter().adaptivePredict(_input, 31, _ctx))
						{
							case 1:
							{
								setState(386);
								hashExp();
							}
								break;

							case 2:
							{
								setState(387);
								rangeExp();
							}
								break;
						}
					}
				}

			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final DropIndexContext dropIndex() throws RecognitionException
	{
		DropIndexContext _localctx = new DropIndexContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_dropIndex);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(425);
				match(43);
				setState(426);
				match(47);
				setState(427);
				tableName();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final DropTableContext dropTable() throws RecognitionException
	{
		DropTableContext _localctx = new DropTableContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_dropTable);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(392);
				match(43);
				setState(393);
				match(26);
				setState(394);
				tableName();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final DropViewContext dropView() throws RecognitionException
	{
		DropViewContext _localctx = new DropViewContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_dropView);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(402);
				match(43);
				setState(403);
				match(36);
				setState(404);
				tableName();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final ExpressionContext expression() throws RecognitionException
	{
		return expression(0);
	}

	public final FetchFirstContext fetchFirst() throws RecognitionException
	{
		FetchFirstContext _localctx = new FetchFirstContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_fetchFirst);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(715);
				match(19);
				setState(716);
				match(18);
				setState(718);
				_la = _input.LA(1);
				if (_la == INTEGER)
				{
					{
						setState(717);
						match(INTEGER);
					}
				}

				setState(720);
				_la = _input.LA(1);
				if (!(_la == 45 || _la == 48))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(721);
				match(25);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final Float2Context float2() throws RecognitionException
	{
		Float2Context _localctx = new Float2Context(_ctx, getState());
		enterRule(_localctx, 76, RULE_float2);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(499);
				_la = _input.LA(1);
				if (!(_la == 1 || _la == 51))
				{
					_errHandler.recoverInline(this);
				}
				consume();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final FromClauseContext fromClause() throws RecognitionException
	{
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_fromClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(608);
				match(3);
				setState(609);
				tableReference(0);
				setState(614);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(610);
							match(9);
							setState(611);
							tableReference(0);
						}
					}
					setState(616);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final FullSelectContext fullSelect() throws RecognitionException
	{
		FullSelectContext _localctx = new FullSelectContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_fullSelect);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(536);
				switch (_input.LA(1))
				{
					case 40:
					{
						setState(531);
						subSelect();
					}
						break;
					case 8:
					{
						setState(532);
						match(8);
						setState(533);
						fullSelect();
						setState(534);
						match(52);
					}
						break;
					default:
						throw new NoViableAltException(this);
				}
				setState(541);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == TABLECOMBINATION)
				{
					{
						{
							setState(538);
							connectedSelect();
						}
					}
					setState(543);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(545);
				_la = _input.LA(1);
				if (_la == 39)
				{
					{
						setState(544);
						orderBy();
					}
				}

				setState(548);
				_la = _input.LA(1);
				if (_la == 19)
				{
					{
						setState(547);
						fetchFirst();
					}
				}

			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	@Override
	public ATN getATN()
	{
		return _ATN;
	}

	@Override
	public String getGrammarFileName()
	{
		return "Select.g4";
	}

	@Override
	public String[] getRuleNames()
	{
		return ruleNames;
	}

	@Override
	public String getSerializedATN()
	{
		return _serializedATN;
	}

	@Override
	public String[] getTokenNames()
	{
		return tokenNames;
	}

	public final GroupByContext groupBy() throws RecognitionException
	{
		GroupByContext _localctx = new GroupByContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_groupBy);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(677);
				match(6);
				setState(678);
				match(28);
				setState(679);
				columnName();
				setState(684);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(680);
							match(9);
							setState(681);
							columnName();
						}
					}
					setState(686);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final GroupDefContext groupDef() throws RecognitionException
	{
		GroupDefContext _localctx = new GroupDefContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_groupDef);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(306);
				match(5);
				setState(307);
				match(INTEGER);
				setState(312);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 49)
				{
					{
						{
							setState(308);
							match(49);
							setState(309);
							match(INTEGER);
						}
					}
					setState(314);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(315);
				match(35);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final GroupExpContext groupExp() throws RecognitionException
	{
		GroupExpContext _localctx = new GroupExpContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_groupExp);
		try
		{
			setState(287);
			switch (_input.LA(1))
			{
				case NONE:
					enterOuterAlt(_localctx, 1);
				{
					setState(285);
					match(NONE);
				}
					break;
				case 5:
					enterOuterAlt(_localctx, 2);
				{
					setState(286);
					realGroupExp();
				}
					break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final HashExpContext hashExp() throws RecognitionException
	{
		HashExpContext _localctx = new HashExpContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_hashExp);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(349);
				match(HASH);
				setState(350);
				match(9);
				setState(351);
				columnSet();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final HavingClauseContext havingClause() throws RecognitionException
	{
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_havingClause);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(687);
				match(20);
				setState(688);
				searchCondition();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final IdentifierContext identifier() throws RecognitionException
	{
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_identifier);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(850);
				_la = _input.LA(1);
				if (!(((((_la - 56)) & ~0x3f) == 0 && ((1L << (_la - 56)) & ((1L << (COUNT - 56)) | (1L << (DATE - 56)) | (1L << (IDENTIFIER - 56)))) != 0)))
				{
					_errHandler.recoverInline(this);
				}
				consume();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final IndexDefContext indexDef() throws RecognitionException
	{
		IndexDefContext _localctx = new IndexDefContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_indexDef);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(450);
				columnName();
				setState(452);
				_la = _input.LA(1);
				if (_la == DIRECTION)
				{
					{
						setState(451);
						match(DIRECTION);
					}
				}

			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final InsertContext insert() throws RecognitionException
	{
		InsertContext _localctx = new InsertContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_insert);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(190);
				match(21);
				setState(191);
				match(31);
				setState(192);
				tableName();
				setState(209);
				switch (_input.LA(1))
				{
					case 3:
					case 8:
					case 40:
					{
						{
							setState(194);
							_la = _input.LA(1);
							if (_la == 3)
							{
								{
									setState(193);
									match(3);
								}
							}

							setState(196);
							fullSelect();
						}
					}
						break;
					case 13:
					{
						{
							setState(197);
							match(13);
							setState(198);
							match(8);
							setState(199);
							expression(0);
							setState(204);
							_errHandler.sync(this);
							_la = _input.LA(1);
							while (_la == 9)
							{
								{
									{
										setState(200);
										match(9);
										setState(201);
										expression(0);
									}
								}
								setState(206);
								_errHandler.sync(this);
								_la = _input.LA(1);
							}
							setState(207);
							match(52);
						}
					}
						break;
					default:
						throw new NoViableAltException(this);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final Int2Context int2() throws RecognitionException
	{
		Int2Context _localctx = new Int2Context(_ctx, getState());
		enterRule(_localctx, 70, RULE_int2);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(493);
				match(2);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final IntegerSetContext integerSet() throws RecognitionException
	{
		IntegerSetContext _localctx = new IntegerSetContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_integerSet);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(338);
				match(5);
				setState(339);
				match(INTEGER);
				setState(344);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 49)
				{
					{
						{
							setState(340);
							match(49);
							setState(341);
							match(INTEGER);
						}
					}
					setState(346);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(347);
				match(35);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final LiteralContext literal() throws RecognitionException
	{
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_literal);
		int _la;
		try
		{
			setState(861);
			switch (_input.LA(1))
			{
				case NEGATIVE:
				case INTEGER:
					_localctx = new NumericLiteralContext(_localctx);
					enterOuterAlt(_localctx, 1);
				{
					setState(853);
					_la = _input.LA(1);
					if (_la == NEGATIVE)
					{
						{
							setState(852);
							match(NEGATIVE);
						}
					}

					setState(855);
					match(INTEGER);
					setState(858);
					switch (getInterpreter().adaptivePredict(_input, 96, _ctx))
					{
						case 1:
						{
							setState(856);
							match(42);
							setState(857);
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
					setState(860);
					match(STRING);
				}
					break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final LoadContext load() throws RecognitionException
	{
		LoadContext _localctx = new LoadContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_load);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(429);
				match(12);
				setState(430);
				_la = _input.LA(1);
				if (!(_la == REPLACE || _la == RESUME))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(431);
				match(31);
				setState(432);
				tableName();
				setState(435);
				_la = _input.LA(1);
				if (_la == 11)
				{
					{
						setState(433);
						match(11);
						setState(434);
						any();
					}
				}

				setState(437);
				match(3);
				setState(438);
				remainder();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final Long2Context long2() throws RecognitionException
	{
		Long2Context _localctx = new Long2Context(_ctx, getState());
		enterRule(_localctx, 72, RULE_long2);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(495);
				match(17);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final NodeExpContext nodeExp() throws RecognitionException
	{
		NodeExpContext _localctx = new NodeExpContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_nodeExp);
		try
		{
			setState(325);
			switch (_input.LA(1))
			{
				case ANYTEXT:
					enterOuterAlt(_localctx, 1);
				{
					setState(323);
					match(ANYTEXT);
				}
					break;
				case 5:
				case ALL:
					enterOuterAlt(_localctx, 2);
				{
					setState(324);
					realNodeExp();
				}
					break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final NotNullContext notNull() throws RecognitionException
	{
		NotNullContext _localctx = new NotNullContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_notNull);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(475);
				match(NOT);
				setState(476);
				match(NULL);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final OperatorContext operator() throws RecognitionException
	{
		OperatorContext _localctx = new OperatorContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_operator);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(769);
				_la = _input.LA(1);
				if (!(_la == EQUALS || _la == OPERATOR))
				{
					_errHandler.recoverInline(this);
				}
				consume();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final OrderByContext orderBy() throws RecognitionException
	{
		OrderByContext _localctx = new OrderByContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_orderBy);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(690);
				match(39);
				setState(691);
				match(28);
				setState(692);
				sortKey();
				setState(697);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(693);
							match(9);
							setState(694);
							sortKey();
						}
					}
					setState(699);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final OrganizationContext organization() throws RecognitionException
	{
		OrganizationContext _localctx = new OrganizationContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_organization);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(261);
				match(ORGANIZATION);
				setState(262);
				match(8);
				setState(263);
				match(INTEGER);
				setState(268);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(264);
							match(9);
							setState(265);
							match(INTEGER);
						}
					}
					setState(270);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(271);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final PredicateContext predicate() throws RecognitionException
	{
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_predicate);
		try
		{
			setState(767);
			switch (getInterpreter().adaptivePredict(_input, 87, _ctx))
			{
				case 1:
					_localctx = new NormalPredicateContext(_localctx);
					enterOuterAlt(_localctx, 1);
				{
					{
						setState(755);
						expression(0);
						setState(756);
						operator();
						setState(757);
						expression(0);
					}
				}
					break;

				case 2:
					_localctx = new NullPredicateContext(_localctx);
					enterOuterAlt(_localctx, 2);
				{
					{
						setState(759);
						expression(0);
						setState(760);
						match(NULLOPERATOR);
					}
				}
					break;

				case 3:
					_localctx = new ExistsPredicateContext(_localctx);
					enterOuterAlt(_localctx, 3);
				{
					setState(762);
					match(4);
					setState(763);
					match(8);
					setState(764);
					subSelect();
					setState(765);
					match(52);
				}
					break;
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final PrimaryContext primary() throws RecognitionException
	{
		PrimaryContext _localctx = new PrimaryContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_primary);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(478);
				match(10);
				setState(479);
				match(38);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final PrimaryKeyContext primaryKey() throws RecognitionException
	{
		PrimaryKeyContext _localctx = new PrimaryKeyContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_primaryKey);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(462);
				match(10);
				setState(463);
				match(38);
				setState(464);
				match(8);
				setState(465);
				columnName();
				setState(470);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 9)
				{
					{
						{
							setState(466);
							match(9);
							setState(467);
							columnName();
						}
					}
					setState(472);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(473);
				match(52);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RangeExpContext rangeExp() throws RecognitionException
	{
		RangeExpContext _localctx = new RangeExpContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_rangeExp);
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(320);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 22, _ctx);
				while (_alt != 1 && _alt != -1)
				{
					if (_alt == 1 + 1)
					{
						{
							{
								setState(317);
								matchWildcard();
							}
						}
					}
					setState(322);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 22, _ctx);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RangeSetContext rangeSet() throws RecognitionException
	{
		RangeSetContext _localctx = new RangeSetContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_rangeSet);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(370);
				match(5);
				setState(371);
				rangeExp();
				setState(376);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 49)
				{
					{
						{
							setState(372);
							match(49);
							setState(373);
							rangeExp();
						}
					}
					setState(378);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(379);
				match(35);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RangeTypeContext rangeType() throws RecognitionException
	{
		RangeTypeContext _localctx = new RangeTypeContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_rangeType);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(364);
				match(RANGE);
				setState(365);
				match(9);
				setState(366);
				columnName();
				setState(367);
				match(9);
				setState(368);
				rangeSet();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RealGroupExpContext realGroupExp() throws RecognitionException
	{
		RealGroupExpContext _localctx = new RealGroupExpContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_realGroupExp);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(289);
				match(5);
				setState(290);
				groupDef();
				setState(295);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 49)
				{
					{
						{
							setState(291);
							match(49);
							setState(292);
							groupDef();
						}
					}
					setState(297);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(298);
				match(35);
				setState(304);
				_la = _input.LA(1);
				if (_la == 9)
				{
					{
						setState(299);
						match(9);
						setState(302);
						switch (_input.LA(1))
						{
							case HASH:
							{
								setState(300);
								hashExp();
							}
								break;
							case RANGE:
							{
								setState(301);
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
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RealNodeExpContext realNodeExp() throws RecognitionException
	{
		RealNodeExpContext _localctx = new RealNodeExpContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_realNodeExp);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(329);
				switch (_input.LA(1))
				{
					case ALL:
					{
						setState(327);
						match(ALL);
					}
						break;
					case 5:
					{
						setState(328);
						integerSet();
					}
						break;
					default:
						throw new NoViableAltException(this);
				}
				setState(336);
				_la = _input.LA(1);
				if (_la == 9)
				{
					{
						setState(331);
						match(9);
						setState(334);
						switch (_input.LA(1))
						{
							case HASH:
							{
								setState(332);
								hashExp();
							}
								break;
							case RANGE:
							{
								setState(333);
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
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RemainderContext remainder() throws RecognitionException
	{
		RemainderContext _localctx = new RemainderContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_remainder);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(445);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1) | (1L << 2) | (1L << 3) | (1L << 4) | (1L << 5) | (1L << 6) | (1L << 7) | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 11) | (1L << 12) | (1L << 13) | (1L << 14) | (1L << 15) | (1L << 16) | (1L << 17) | (1L << 18) | (1L << 19) | (1L << 20) | (1L << 21) | (1L << 22) | (1L << 23) | (1L << 24) | (1L << 25) | (1L << 26) | (1L << 27) | (1L << 28) | (1L << 29) | (1L << 30) | (1L << 31) | (1L << 32) | (1L << 33) | (1L << 34) | (1L << 35) | (1L << 36) | (1L << 37) | (1L << 38) | (1L << 39) | (1L << 40) | (1L << 41) | (1L << 42) | (1L << 43) | (1L << 44) | (1L << 45) | (1L << 46) | (1L << 47) | (1L << 48) | (1L << 49) | (1L << 50) | (1L << 51) | (1L << 52) | (1L << 53) | (1L << STRING) | (1L << STAR) | (1L << COUNT) | (1L << CONCAT) | (1L << NEGATIVE) | (1L << EQUALS) | (1L << OPERATOR) | (1L << NULLOPERATOR) | (1L << AND) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (COLUMN - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (COLORDER - 64)) | (1L << (ORGANIZATION - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0))
				{
					{
						{
							setState(442);
							matchWildcard();
						}
					}
					setState(447);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(448);
				match(EOF);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final RunstatsContext runstats() throws RecognitionException
	{
		RunstatsContext _localctx = new RunstatsContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_runstats);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(186);
				match(50);
				setState(187);
				match(33);
				setState(188);
				tableName();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SearchClauseContext searchClause() throws RecognitionException
	{
		SearchClauseContext _localctx = new SearchClauseContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_searchClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(746);
				_la = _input.LA(1);
				if (_la == NOT)
				{
					{
						setState(745);
						match(NOT);
					}
				}

				setState(753);
				switch (getInterpreter().adaptivePredict(_input, 86, _ctx))
				{
					case 1:
					{
						setState(748);
						predicate();
					}
						break;

					case 2:
					{
						{
							setState(749);
							match(8);
							setState(750);
							searchCondition();
							setState(751);
							match(52);
						}
					}
						break;
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SearchConditionContext searchCondition() throws RecognitionException
	{
		SearchConditionContext _localctx = new SearchConditionContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_searchCondition);
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(735);
				searchClause();
				setState(739);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 84, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						{
							{
								setState(736);
								connectedSearchClause();
							}
						}
					}
					setState(741);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 84, _ctx);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SelectContext select() throws RecognitionException
	{
		SelectContext _localctx = new SelectContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_select);
		int _la;
		try
		{
			setState(184);
			switch (getInterpreter().adaptivePredict(_input, 2, _ctx))
			{
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					{
						setState(136);
						insert();
						setState(137);
						match(EOF);
					}
				}
					break;

				case 2:
					enterOuterAlt(_localctx, 2);
				{
					{
						setState(139);
						update();
						setState(140);
						match(EOF);
					}
				}
					break;

				case 3:
					enterOuterAlt(_localctx, 3);
				{
					{
						setState(142);
						delete();
						setState(143);
						match(EOF);
					}
				}
					break;

				case 4:
					enterOuterAlt(_localctx, 4);
				{
					{
						setState(145);
						createTable();
						setState(146);
						match(EOF);
					}
				}
					break;

				case 5:
					enterOuterAlt(_localctx, 5);
				{
					{
						setState(148);
						createIndex();
						setState(149);
						match(EOF);
					}
				}
					break;

				case 6:
					enterOuterAlt(_localctx, 6);
				{
					{
						setState(151);
						createView();
						setState(152);
						match(EOF);
					}
				}
					break;

				case 7:
					enterOuterAlt(_localctx, 7);
				{
					{
						setState(154);
						dropTable();
						setState(155);
						match(EOF);
					}
				}
					break;

				case 8:
					enterOuterAlt(_localctx, 8);
				{
					{
						setState(157);
						dropIndex();
						setState(158);
						match(EOF);
					}
				}
					break;

				case 9:
					enterOuterAlt(_localctx, 9);
				{
					{
						setState(160);
						dropView();
						setState(161);
						match(EOF);
					}
				}
					break;

				case 10:
					enterOuterAlt(_localctx, 10);
				{
					{
						setState(163);
						load();
						setState(164);
						match(EOF);
					}
				}
					break;

				case 11:
					enterOuterAlt(_localctx, 11);
				{
					{
						setState(166);
						runstats();
						setState(167);
						match(EOF);
					}
				}
					break;

				case 12:
					enterOuterAlt(_localctx, 12);
				{
					{
						{
							setState(178);
							_la = _input.LA(1);
							if (_la == 41)
							{
								{
									setState(169);
									match(41);
									setState(170);
									commonTableExpression();
									setState(175);
									_errHandler.sync(this);
									_la = _input.LA(1);
									while (_la == 9)
									{
										{
											{
												setState(171);
												match(9);
												setState(172);
												commonTableExpression();
											}
										}
										setState(177);
										_errHandler.sync(this);
										_la = _input.LA(1);
									}
								}
							}

							setState(180);
							fullSelect();
						}
						setState(182);
						match(EOF);
					}
				}
					break;
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SelectClauseContext selectClause() throws RecognitionException
	{
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_selectClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(575);
				match(40);
				setState(577);
				_la = _input.LA(1);
				if (_la == DISTINCT || _la == ALL)
				{
					{
						setState(576);
						selecthow();
					}
				}

				setState(588);
				switch (_input.LA(1))
				{
					case STAR:
					{
						setState(579);
						match(STAR);
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
							setState(580);
							selectListEntry();
							setState(585);
							_errHandler.sync(this);
							_la = _input.LA(1);
							while (_la == 9)
							{
								{
									{
										setState(581);
										match(9);
										setState(582);
										selectListEntry();
									}
								}
								setState(587);
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
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SelecthowContext selecthow() throws RecognitionException
	{
		SelecthowContext _localctx = new SelecthowContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_selecthow);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(590);
				_la = _input.LA(1);
				if (!(_la == DISTINCT || _la == ALL))
				{
					_errHandler.recoverInline(this);
				}
				consume();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SelectListEntryContext selectListEntry() throws RecognitionException
	{
		SelectListEntryContext _localctx = new SelectListEntryContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_selectListEntry);
		int _la;
		try
		{
			setState(606);
			switch (getInterpreter().adaptivePredict(_input, 62, _ctx))
			{
				case 1:
					_localctx = new SelectColumnContext(_localctx);
					enterOuterAlt(_localctx, 1);
				{
					setState(592);
					columnName();
					setState(594);
					_la = _input.LA(1);
					if (_la == 27)
					{
						{
							setState(593);
							match(27);
						}
					}

					setState(597);
					_la = _input.LA(1);
					if (_la == IDENTIFIER)
					{
						{
							setState(596);
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
						setState(599);
						expression(0);
						setState(601);
						_la = _input.LA(1);
						if (_la == 27)
						{
							{
								setState(600);
								match(27);
							}
						}

						setState(604);
						_la = _input.LA(1);
						if (_la == IDENTIFIER)
						{
							{
								setState(603);
								match(IDENTIFIER);
							}
						}

					}
				}
					break;
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	@Override
	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex)
	{
		switch (ruleIndex)
		{
			case 48:
				return tableReference_sempred((TableReferenceContext)_localctx, predIndex);

			case 64:
				return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}

	public final SingleTableContext singleTable() throws RecognitionException
	{
		SingleTableContext _localctx = new SingleTableContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_singleTable);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(670);
				tableName();
				setState(672);
				switch (getInterpreter().adaptivePredict(_input, 74, _ctx))
				{
					case 1:
					{
						setState(671);
						correlationClause();
					}
						break;
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SortKeyContext sortKey() throws RecognitionException
	{
		SortKeyContext _localctx = new SortKeyContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_sortKey);
		int _la;
		try
		{
			setState(708);
			switch (_input.LA(1))
			{
				case INTEGER:
					_localctx = new SortKeyIntContext(_localctx);
					enterOuterAlt(_localctx, 1);
				{
					setState(700);
					match(INTEGER);
					setState(702);
					_la = _input.LA(1);
					if (_la == DIRECTION)
					{
						{
							setState(701);
							match(DIRECTION);
						}
					}

				}
					break;
				case IDENTIFIER:
					_localctx = new SortKeyColContext(_localctx);
					enterOuterAlt(_localctx, 2);
				{
					setState(704);
					columnName();
					setState(706);
					_la = _input.LA(1);
					if (_la == DIRECTION)
					{
						{
							setState(705);
							match(DIRECTION);
						}
					}

				}
					break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final SubSelectContext subSelect() throws RecognitionException
	{
		SubSelectContext _localctx = new SubSelectContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_subSelect);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(558);
				selectClause();
				setState(559);
				fromClause();
				setState(561);
				_la = _input.LA(1);
				if (_la == 30)
				{
					{
						setState(560);
						whereClause();
					}
				}

				setState(564);
				_la = _input.LA(1);
				if (_la == 6)
				{
					{
						setState(563);
						groupBy();
					}
				}

				setState(567);
				_la = _input.LA(1);
				if (_la == 20)
				{
					{
						setState(566);
						havingClause();
					}
				}

				setState(570);
				switch (getInterpreter().adaptivePredict(_input, 53, _ctx))
				{
					case 1:
					{
						setState(569);
						orderBy();
					}
						break;
				}
				setState(573);
				switch (getInterpreter().adaptivePredict(_input, 54, _ctx))
				{
					case 1:
					{
						setState(572);
						fetchFirst();
					}
						break;
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final TableNameContext tableName() throws RecognitionException
	{
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_tableName);
		try
		{
			setState(727);
			switch (getInterpreter().adaptivePredict(_input, 82, _ctx))
			{
				case 1:
					_localctx = new Table1PartContext(_localctx);
					enterOuterAlt(_localctx, 1);
				{
					setState(723);
					match(IDENTIFIER);
				}
					break;

				case 2:
					_localctx = new Table2PartContext(_localctx);
					enterOuterAlt(_localctx, 2);
				{
					{
						setState(724);
						match(IDENTIFIER);
						setState(725);
						match(42);
						setState(726);
						match(IDENTIFIER);
					}
				}
					break;
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final TableReferenceContext tableReference() throws RecognitionException
	{
		return tableReference(0);
	}

	public final UpdateContext update() throws RecognitionException
	{
		UpdateContext _localctx = new UpdateContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_update);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(211);
				match(15);
				setState(212);
				tableName();
				setState(213);
				match(53);
				setState(216);
				switch (_input.LA(1))
				{
					case IDENTIFIER:
					{
						setState(214);
						columnName();
					}
						break;
					case 8:
					{
						setState(215);
						colList();
					}
						break;
					default:
						throw new NoViableAltException(this);
				}
				setState(218);
				match(EQUALS);
				setState(219);
				expression(0);
				setState(221);
				_la = _input.LA(1);
				if (_la == 30)
				{
					{
						setState(220);
						whereClause();
					}
				}

			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	public final WhereClauseContext whereClause() throws RecognitionException
	{
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_whereClause);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(674);
				match(30);
				setState(675);
				searchCondition();
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			exitRule();
		}
		return _localctx;
	}

	private ExpressionContext expression(int _p) throws RecognitionException
	{
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_expression, _p);
		int _la;
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(829);
				switch (getInterpreter().adaptivePredict(_input, 92, _ctx))
				{
					case 1:
					{
						_localctx = new FunctionContext(_localctx);
						_ctx = _localctx;
						setState(772);
						identifier();
						setState(773);
						match(8);
						setState(774);
						expression(0);
						setState(779);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la == 9)
						{
							{
								{
									setState(775);
									match(9);
									setState(776);
									expression(0);
								}
							}
							setState(781);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(782);
						match(52);
					}
						break;

					case 2:
					{
						_localctx = new CountDistinctContext(_localctx);
						_ctx = _localctx;
						setState(784);
						match(COUNT);
						setState(785);
						match(8);
						setState(786);
						match(DISTINCT);
						setState(787);
						expression(0);
						setState(788);
						match(52);
					}
						break;

					case 3:
					{
						_localctx = new ListContext(_localctx);
						_ctx = _localctx;
						setState(790);
						match(8);
						setState(791);
						expression(0);
						setState(794);
						_errHandler.sync(this);
						_la = _input.LA(1);
						do
						{
							{
								{
									setState(792);
									match(9);
									setState(793);
									expression(0);
								}
							}
							setState(796);
							_errHandler.sync(this);
							_la = _input.LA(1);
						} while (_la == 9);
						setState(798);
						match(52);
					}
						break;

					case 4:
					{
						_localctx = new CountStarContext(_localctx);
						_ctx = _localctx;
						setState(800);
						match(COUNT);
						setState(801);
						match(8);
						setState(802);
						match(STAR);
						setState(803);
						match(52);
					}
						break;

					case 5:
					{
						_localctx = new IsLiteralContext(_localctx);
						_ctx = _localctx;
						setState(804);
						literal();
					}
						break;

					case 6:
					{
						_localctx = new ColLiteralContext(_localctx);
						_ctx = _localctx;
						setState(805);
						columnName();
					}
						break;

					case 7:
					{
						_localctx = new ExpSelectContext(_localctx);
						_ctx = _localctx;
						setState(806);
						match(8);
						setState(807);
						subSelect();
						setState(808);
						match(52);
					}
						break;

					case 8:
					{
						_localctx = new PExpressionContext(_localctx);
						_ctx = _localctx;
						setState(810);
						match(8);
						setState(811);
						expression(0);
						setState(812);
						match(52);
					}
						break;

					case 9:
					{
						_localctx = new NullExpContext(_localctx);
						_ctx = _localctx;
						setState(814);
						match(NULL);
					}
						break;

					case 10:
					{
						_localctx = new CaseExpContext(_localctx);
						_ctx = _localctx;
						setState(815);
						match(7);
						setState(816);
						caseCase();
						setState(820);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la == 44)
						{
							{
								{
									setState(817);
									caseCase();
								}
							}
							setState(822);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(823);
						match(29);
						setState(824);
						expression(0);
						setState(825);
						match(32);
						setState(827);
						switch (getInterpreter().adaptivePredict(_input, 91, _ctx))
						{
							case 1:
							{
								setState(826);
								match(7);
							}
								break;
						}
					}
						break;
				}
				_ctx.stop = _input.LT(-1);
				setState(842);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 94, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						if (_parseListeners != null)
						{
							triggerExitRuleEvent();
						}
						{
							setState(840);
							switch (getInterpreter().adaptivePredict(_input, 93, _ctx))
							{
								case 1:
								{
									_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_expression);
									setState(831);
									if (!(precpred(_ctx, 13)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 13)");
									}
									setState(832);
									((MulDivContext)_localctx).op = _input.LT(1);
									_la = _input.LA(1);
									if (!(_la == 24 || _la == STAR))
									{
										((MulDivContext)_localctx).op = _errHandler.recoverInline(this);
									}
									consume();
									setState(833);
									expression(14);
								}
									break;

								case 2:
								{
									_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_expression);
									setState(834);
									if (!(precpred(_ctx, 12)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 12)");
									}
									setState(835);
									((AddSubContext)_localctx).op = _input.LT(1);
									_la = _input.LA(1);
									if (!(_la == 22 || _la == NEGATIVE))
									{
										((AddSubContext)_localctx).op = _errHandler.recoverInline(this);
									}
									consume();
									setState(836);
									expression(13);
								}
									break;

								case 3:
								{
									_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_expression);
									setState(837);
									if (!(precpred(_ctx, 11)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 11)");
									}
									setState(838);
									match(CONCAT);
									setState(839);
									expression(12);
								}
									break;
							}
						}
					}
					setState(844);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 94, _ctx);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	private boolean expression_sempred(ExpressionContext _localctx, int predIndex)
	{
		switch (predIndex)
		{
			case 2:
				return precpred(_ctx, 13);

			case 3:
				return precpred(_ctx, 12);

			case 4:
				return precpred(_ctx, 11);
		}
		return true;
	}

	private TableReferenceContext tableReference(int _p) throws RecognitionException
	{
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TableReferenceContext _localctx = new TableReferenceContext(_ctx, _parentState);
		int _startState = 96;
		enterRecursionRule(_localctx, 96, RULE_tableReference, _p);
		int _la;
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(646);
				switch (getInterpreter().adaptivePredict(_input, 68, _ctx))
				{
					case 1:
					{
						_localctx = new JoinPContext(_localctx);
						_ctx = _localctx;
						setState(618);
						match(8);
						setState(619);
						tableReference(0);
						setState(621);
						_la = _input.LA(1);
						if (_la == JOINTYPE)
						{
							{
								setState(620);
								match(JOINTYPE);
							}
						}

						setState(623);
						match(34);
						setState(624);
						tableReference(0);
						setState(625);
						match(33);
						setState(626);
						searchCondition();
						setState(627);
						match(52);
						setState(629);
						switch (getInterpreter().adaptivePredict(_input, 65, _ctx))
						{
							case 1:
							{
								setState(628);
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
						setState(631);
						match(8);
						setState(632);
						tableReference(0);
						setState(633);
						match(CROSSJOIN);
						setState(634);
						tableReference(0);
						setState(635);
						match(52);
						setState(637);
						switch (getInterpreter().adaptivePredict(_input, 66, _ctx))
						{
							case 1:
							{
								setState(636);
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
						setState(639);
						match(8);
						setState(640);
						fullSelect();
						setState(641);
						match(52);
						setState(643);
						switch (getInterpreter().adaptivePredict(_input, 67, _ctx))
						{
							case 1:
							{
								setState(642);
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
						setState(645);
						singleTable();
					}
						break;
				}
				_ctx.stop = _input.LT(-1);
				setState(667);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 73, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						if (_parseListeners != null)
						{
							triggerExitRuleEvent();
						}
						{
							setState(665);
							switch (getInterpreter().adaptivePredict(_input, 72, _ctx))
							{
								case 1:
								{
									_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
									setState(648);
									if (!(precpred(_ctx, 6)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 6)");
									}
									setState(650);
									_la = _input.LA(1);
									if (_la == JOINTYPE)
									{
										{
											setState(649);
											match(JOINTYPE);
										}
									}

									setState(652);
									match(34);
									setState(653);
									tableReference(0);
									setState(654);
									match(33);
									setState(655);
									searchCondition();
									setState(657);
									switch (getInterpreter().adaptivePredict(_input, 70, _ctx))
									{
										case 1:
										{
											setState(656);
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
									setState(659);
									if (!(precpred(_ctx, 5)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 5)");
									}
									setState(660);
									match(CROSSJOIN);
									setState(661);
									tableReference(0);
									setState(663);
									switch (getInterpreter().adaptivePredict(_input, 71, _ctx))
									{
										case 1:
										{
											setState(662);
											correlationClause();
										}
											break;
									}
								}
									break;
							}
						}
					}
					setState(669);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 73, _ctx);
				}
			}
		}
		catch (RecognitionException re)
		{
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally
		{
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	private boolean tableReference_sempred(TableReferenceContext _localctx, int predIndex)
	{
		switch (predIndex)
		{
			case 0:
				return precpred(_ctx, 6);

			case 1:
				return precpred(_ctx, 5);
		}
		return true;
	}

	public static class AddSubContext extends ExpressionContext
	{
		public Token op;

		public AddSubContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitAddSub(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterAddSub(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitAddSub(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}

		public TerminalNode NEGATIVE()
		{
			return getToken(SelectParser.NEGATIVE, 0);
		}
	}

	public static class AnyContext extends ParserRuleContext
	{
		public AnyContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitAny(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterAny(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitAny(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_any;
		}
	}

	public static class CaseCaseContext extends ParserRuleContext
	{
		public CaseCaseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCaseCase(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCaseCase(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCaseCase(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_caseCase;
		}

		public SearchConditionContext searchCondition()
		{
			return getRuleContext(SearchConditionContext.class, 0);
		}
	}

	public static class CaseExpContext extends ExpressionContext
	{
		public CaseExpContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCaseExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<CaseCaseContext> caseCase()
		{
			return getRuleContexts(CaseCaseContext.class);
		}

		public CaseCaseContext caseCase(int i)
		{
			return getRuleContext(CaseCaseContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCaseExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCaseExp(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}
	}

	public static class Char2Context extends ParserRuleContext
	{
		public Char2Context(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitChar2(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterChar2(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitChar2(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_char2;
		}

		public TerminalNode INTEGER()
		{
			return getToken(SelectParser.INTEGER, 0);
		}
	}

	public static class Col1PartContext extends ColumnNameContext
	{
		public Col1PartContext(ColumnNameContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCol1Part(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCol1Part(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCol1Part(this);
			}
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}

	public static class Col2PartContext extends ColumnNameContext
	{
		public Col2PartContext(ColumnNameContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCol2Part(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCol2Part(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCol2Part(this);
			}
		}

		public List<TerminalNode> IDENTIFIER()
		{
			return getTokens(SelectParser.IDENTIFIER);
		}

		public TerminalNode IDENTIFIER(int i)
		{
			return getToken(SelectParser.IDENTIFIER, i);
		}
	}

	public static class ColDefContext extends ParserRuleContext
	{
		public ColDefContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitColDef(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		public DataTypeContext dataType()
		{
			return getRuleContext(DataTypeContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterColDef(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitColDef(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_colDef;
		}

		public NotNullContext notNull()
		{
			return getRuleContext(NotNullContext.class, 0);
		}

		public PrimaryContext primary()
		{
			return getRuleContext(PrimaryContext.class, 0);
		}
	}

	public static class ColListContext extends ParserRuleContext
	{
		public ColListContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitColList(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ColumnNameContext> columnName()
		{
			return getRuleContexts(ColumnNameContext.class);
		}

		public ColumnNameContext columnName(int i)
		{
			return getRuleContext(ColumnNameContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterColList(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitColList(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_colList;
		}
	}

	public static class ColLiteralContext extends ExpressionContext
	{
		public ColLiteralContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitColLiteral(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterColLiteral(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitColLiteral(this);
			}
		}
	}

	public static class ColOrderContext extends ParserRuleContext
	{
		public ColOrderContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitColOrder(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode COLORDER()
		{
			return getToken(SelectParser.COLORDER, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterColOrder(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitColOrder(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_colOrder;
		}

		public List<TerminalNode> INTEGER()
		{
			return getTokens(SelectParser.INTEGER);
		}

		public TerminalNode INTEGER(int i)
		{
			return getToken(SelectParser.INTEGER, i);
		}
	}

	public static class ColumnNameContext extends ParserRuleContext
	{
		public ColumnNameContext()
		{
		}

		public ColumnNameContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(ColumnNameContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_columnName;
		}
	}

	public static class ColumnSetContext extends ParserRuleContext
	{
		public ColumnSetContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitColumnSet(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ColumnNameContext> columnName()
		{
			return getRuleContexts(ColumnNameContext.class);
		}

		public ColumnNameContext columnName(int i)
		{
			return getRuleContext(ColumnNameContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterColumnSet(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitColumnSet(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_columnSet;
		}
	}

	public static class CommonTableExpressionContext extends ParserRuleContext
	{
		public CommonTableExpressionContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCommonTableExpression(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ColumnNameContext> columnName()
		{
			return getRuleContexts(ColumnNameContext.class);
		}

		public ColumnNameContext columnName(int i)
		{
			return getRuleContext(ColumnNameContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCommonTableExpression(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCommonTableExpression(this);
			}
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_commonTableExpression;
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}

	public static class ConcatContext extends ExpressionContext
	{
		public ConcatContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitConcat(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode CONCAT()
		{
			return getToken(SelectParser.CONCAT, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterConcat(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitConcat(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}
	}

	public static class ConnectedSearchClauseContext extends ParserRuleContext
	{
		public ConnectedSearchClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitConnectedSearchClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode AND()
		{
			return getToken(SelectParser.AND, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterConnectedSearchClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitConnectedSearchClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_connectedSearchClause;
		}

		public TerminalNode OR()
		{
			return getToken(SelectParser.OR, 0);
		}

		public SearchClauseContext searchClause()
		{
			return getRuleContext(SearchClauseContext.class, 0);
		}
	}

	public static class ConnectedSelectContext extends ParserRuleContext
	{
		public ConnectedSelectContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitConnectedSelect(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterConnectedSelect(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitConnectedSelect(this);
			}
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_connectedSelect;
		}

		public SubSelectContext subSelect()
		{
			return getRuleContext(SubSelectContext.class, 0);
		}

		public TerminalNode TABLECOMBINATION()
		{
			return getToken(SelectParser.TABLECOMBINATION, 0);
		}
	}
	public static class CorrelationClauseContext extends ParserRuleContext
	{
		public CorrelationClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCorrelationClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCorrelationClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCorrelationClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_correlationClause;
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}
	public static class CountDistinctContext extends ExpressionContext
	{
		public CountDistinctContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCountDistinct(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode COUNT()
		{
			return getToken(SelectParser.COUNT, 0);
		}

		public TerminalNode DISTINCT()
		{
			return getToken(SelectParser.DISTINCT, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCountDistinct(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCountDistinct(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}
	}

	public static class CountStarContext extends ExpressionContext
	{
		public CountStarContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCountStar(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode COUNT()
		{
			return getToken(SelectParser.COUNT, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCountStar(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCountStar(this);
			}
		}

		public TerminalNode STAR()
		{
			return getToken(SelectParser.STAR, 0);
		}
	}

	public static class CreateIndexContext extends ParserRuleContext
	{
		public CreateIndexContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCreateIndex(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCreateIndex(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCreateIndex(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_createIndex;
		}

		public List<IndexDefContext> indexDef()
		{
			return getRuleContexts(IndexDefContext.class);
		}

		public IndexDefContext indexDef(int i)
		{
			return getRuleContext(IndexDefContext.class, i);
		}

		public List<TableNameContext> tableName()
		{
			return getRuleContexts(TableNameContext.class);
		}

		public TableNameContext tableName(int i)
		{
			return getRuleContext(TableNameContext.class, i);
		}

		public TerminalNode UNIQUE()
		{
			return getToken(SelectParser.UNIQUE, 0);
		}
	}

	public static class CreateTableContext extends ParserRuleContext
	{
		public CreateTableContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCreateTable(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ColDefContext> colDef()
		{
			return getRuleContexts(ColDefContext.class);
		}

		public ColDefContext colDef(int i)
		{
			return getRuleContext(ColDefContext.class, i);
		}

		public ColOrderContext colOrder()
		{
			return getRuleContext(ColOrderContext.class, 0);
		}

		public TerminalNode COLUMN()
		{
			return getToken(SelectParser.COLUMN, 0);
		}

		public DeviceExpContext deviceExp()
		{
			return getRuleContext(DeviceExpContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCreateTable(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCreateTable(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_createTable;
		}

		public GroupExpContext groupExp()
		{
			return getRuleContext(GroupExpContext.class, 0);
		}

		public NodeExpContext nodeExp()
		{
			return getRuleContext(NodeExpContext.class, 0);
		}

		public OrganizationContext organization()
		{
			return getRuleContext(OrganizationContext.class, 0);
		}

		public PrimaryKeyContext primaryKey()
		{
			return getRuleContext(PrimaryKeyContext.class, 0);
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class CreateViewContext extends ParserRuleContext
	{
		public CreateViewContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCreateView(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCreateView(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCreateView(this);
			}
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_createView;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}
	public static class CrossJoinContext extends TableReferenceContext
	{
		public CrossJoinContext(TableReferenceContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCrossJoin(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public CorrelationClauseContext correlationClause()
		{
			return getRuleContext(CorrelationClauseContext.class, 0);
		}

		public TerminalNode CROSSJOIN()
		{
			return getToken(SelectParser.CROSSJOIN, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCrossJoin(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCrossJoin(this);
			}
		}

		public List<TableReferenceContext> tableReference()
		{
			return getRuleContexts(TableReferenceContext.class);
		}

		public TableReferenceContext tableReference(int i)
		{
			return getRuleContext(TableReferenceContext.class, i);
		}
	}
	public static class CrossJoinPContext extends TableReferenceContext
	{
		public CrossJoinPContext(TableReferenceContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitCrossJoinP(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public CorrelationClauseContext correlationClause()
		{
			return getRuleContext(CorrelationClauseContext.class, 0);
		}

		public TerminalNode CROSSJOIN()
		{
			return getToken(SelectParser.CROSSJOIN, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterCrossJoinP(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitCrossJoinP(this);
			}
		}

		public List<TableReferenceContext> tableReference()
		{
			return getRuleContexts(TableReferenceContext.class);
		}

		public TableReferenceContext tableReference(int i)
		{
			return getRuleContext(TableReferenceContext.class, i);
		}
	}
	public static class DataTypeContext extends ParserRuleContext
	{
		public DataTypeContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDataType(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public Char2Context char2()
		{
			return getRuleContext(Char2Context.class, 0);
		}

		public Date2Context date2()
		{
			return getRuleContext(Date2Context.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDataType(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDataType(this);
			}
		}

		public Float2Context float2()
		{
			return getRuleContext(Float2Context.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_dataType;
		}

		public Int2Context int2()
		{
			return getRuleContext(Int2Context.class, 0);
		}

		public Long2Context long2()
		{
			return getRuleContext(Long2Context.class, 0);
		}
	}
	public static class Date2Context extends ParserRuleContext
	{
		public Date2Context(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDate2(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode DATE()
		{
			return getToken(SelectParser.DATE, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDate2(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDate2(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_date2;
		}
	}
	public static class DeleteContext extends ParserRuleContext
	{
		public DeleteContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDelete(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDelete(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDelete(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_delete;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}

		public WhereClauseContext whereClause()
		{
			return getRuleContext(WhereClauseContext.class, 0);
		}
	}
	public static class DeviceExpContext extends ParserRuleContext
	{
		public DeviceExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDeviceExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode ALL()
		{
			return getToken(SelectParser.ALL, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDeviceExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDeviceExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_deviceExp;
		}

		public HashExpContext hashExp()
		{
			return getRuleContext(HashExpContext.class, 0);
		}

		public IntegerSetContext integerSet()
		{
			return getRuleContext(IntegerSetContext.class, 0);
		}

		public RangeExpContext rangeExp()
		{
			return getRuleContext(RangeExpContext.class, 0);
		}
	}

	public static class DropIndexContext extends ParserRuleContext
	{
		public DropIndexContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDropIndex(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDropIndex(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDropIndex(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_dropIndex;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class DropTableContext extends ParserRuleContext
	{
		public DropTableContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDropTable(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDropTable(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDropTable(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_dropTable;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class DropViewContext extends ParserRuleContext
	{
		public DropViewContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitDropView(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterDropView(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitDropView(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_dropView;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class ExistsPredicateContext extends PredicateContext
	{
		public ExistsPredicateContext(PredicateContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitExistsPredicate(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterExistsPredicate(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitExistsPredicate(this);
			}
		}

		public SubSelectContext subSelect()
		{
			return getRuleContext(SubSelectContext.class, 0);
		}
	}

	public static class ExpressionContext extends ParserRuleContext
	{
		public ExpressionContext()
		{
		}

		public ExpressionContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(ExpressionContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_expression;
		}
	}

	public static class ExpSelectContext extends ExpressionContext
	{
		public ExpSelectContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitExpSelect(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterExpSelect(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitExpSelect(this);
			}
		}

		public SubSelectContext subSelect()
		{
			return getRuleContext(SubSelectContext.class, 0);
		}
	}

	public static class FetchFirstContext extends ParserRuleContext
	{
		public FetchFirstContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitFetchFirst(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterFetchFirst(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitFetchFirst(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_fetchFirst;
		}

		public TerminalNode INTEGER()
		{
			return getToken(SelectParser.INTEGER, 0);
		}
	}

	public static class Float2Context extends ParserRuleContext
	{
		public Float2Context(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitFloat2(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterFloat2(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitFloat2(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_float2;
		}
	}

	public static class FromClauseContext extends ParserRuleContext
	{
		public FromClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitFromClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterFromClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitFromClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_fromClause;
		}

		public List<TableReferenceContext> tableReference()
		{
			return getRuleContexts(TableReferenceContext.class);
		}

		public TableReferenceContext tableReference(int i)
		{
			return getRuleContext(TableReferenceContext.class, i);
		}
	}

	public static class FullSelectContext extends ParserRuleContext
	{
		public FullSelectContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitFullSelect(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ConnectedSelectContext> connectedSelect()
		{
			return getRuleContexts(ConnectedSelectContext.class);
		}

		public ConnectedSelectContext connectedSelect(int i)
		{
			return getRuleContext(ConnectedSelectContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterFullSelect(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitFullSelect(this);
			}
		}

		public FetchFirstContext fetchFirst()
		{
			return getRuleContext(FetchFirstContext.class, 0);
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_fullSelect;
		}

		public OrderByContext orderBy()
		{
			return getRuleContext(OrderByContext.class, 0);
		}

		public SubSelectContext subSelect()
		{
			return getRuleContext(SubSelectContext.class, 0);
		}
	}

	public static class FunctionContext extends ExpressionContext
	{
		public FunctionContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitFunction(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterFunction(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitFunction(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}

		public IdentifierContext identifier()
		{
			return getRuleContext(IdentifierContext.class, 0);
		}
	}

	public static class GroupByContext extends ParserRuleContext
	{
		public GroupByContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitGroupBy(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ColumnNameContext> columnName()
		{
			return getRuleContexts(ColumnNameContext.class);
		}

		public ColumnNameContext columnName(int i)
		{
			return getRuleContext(ColumnNameContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterGroupBy(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitGroupBy(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_groupBy;
		}
	}

	public static class GroupDefContext extends ParserRuleContext
	{
		public GroupDefContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitGroupDef(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterGroupDef(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitGroupDef(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_groupDef;
		}

		public List<TerminalNode> INTEGER()
		{
			return getTokens(SelectParser.INTEGER);
		}

		public TerminalNode INTEGER(int i)
		{
			return getToken(SelectParser.INTEGER, i);
		}
	}
	public static class GroupExpContext extends ParserRuleContext
	{
		public GroupExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitGroupExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterGroupExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitGroupExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_groupExp;
		}

		public TerminalNode NONE()
		{
			return getToken(SelectParser.NONE, 0);
		}

		public RealGroupExpContext realGroupExp()
		{
			return getRuleContext(RealGroupExpContext.class, 0);
		}
	}
	public static class HashExpContext extends ParserRuleContext
	{
		public HashExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitHashExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnSetContext columnSet()
		{
			return getRuleContext(ColumnSetContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterHashExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitHashExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_hashExp;
		}

		public TerminalNode HASH()
		{
			return getToken(SelectParser.HASH, 0);
		}
	}

	public static class HavingClauseContext extends ParserRuleContext
	{
		public HavingClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitHavingClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterHavingClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitHavingClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_havingClause;
		}

		public SearchConditionContext searchCondition()
		{
			return getRuleContext(SearchConditionContext.class, 0);
		}
	}

	public static class IdentifierContext extends ParserRuleContext
	{
		public IdentifierContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitIdentifier(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode COUNT()
		{
			return getToken(SelectParser.COUNT, 0);
		}

		public TerminalNode DATE()
		{
			return getToken(SelectParser.DATE, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterIdentifier(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitIdentifier(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_identifier;
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}

	public static class IndexDefContext extends ParserRuleContext
	{
		public IndexDefContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitIndexDef(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		public TerminalNode DIRECTION()
		{
			return getToken(SelectParser.DIRECTION, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterIndexDef(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitIndexDef(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_indexDef;
		}
	}

	public static class InsertContext extends ParserRuleContext
	{
		public InsertContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitInsert(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterInsert(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitInsert(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_insert;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class Int2Context extends ParserRuleContext
	{
		public Int2Context(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitInt2(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterInt2(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitInt2(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_int2;
		}
	}

	public static class IntegerSetContext extends ParserRuleContext
	{
		public IntegerSetContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitIntegerSet(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterIntegerSet(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitIntegerSet(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_integerSet;
		}

		public List<TerminalNode> INTEGER()
		{
			return getTokens(SelectParser.INTEGER);
		}

		public TerminalNode INTEGER(int i)
		{
			return getToken(SelectParser.INTEGER, i);
		}
	}
	public static class IsLiteralContext extends ExpressionContext
	{
		public IsLiteralContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitIsLiteral(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterIsLiteral(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitIsLiteral(this);
			}
		}

		public LiteralContext literal()
		{
			return getRuleContext(LiteralContext.class, 0);
		}
	}
	public static class IsSingleTableContext extends TableReferenceContext
	{
		public IsSingleTableContext(TableReferenceContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitIsSingleTable(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterIsSingleTable(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitIsSingleTable(this);
			}
		}

		public SingleTableContext singleTable()
		{
			return getRuleContext(SingleTableContext.class, 0);
		}
	}

	public static class JoinContext extends TableReferenceContext
	{
		public JoinContext(TableReferenceContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitJoin(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public CorrelationClauseContext correlationClause()
		{
			return getRuleContext(CorrelationClauseContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterJoin(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitJoin(this);
			}
		}

		public TerminalNode JOINTYPE()
		{
			return getToken(SelectParser.JOINTYPE, 0);
		}

		public SearchConditionContext searchCondition()
		{
			return getRuleContext(SearchConditionContext.class, 0);
		}

		public List<TableReferenceContext> tableReference()
		{
			return getRuleContexts(TableReferenceContext.class);
		}

		public TableReferenceContext tableReference(int i)
		{
			return getRuleContext(TableReferenceContext.class, i);
		}
	}

	public static class JoinPContext extends TableReferenceContext
	{
		public JoinPContext(TableReferenceContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitJoinP(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public CorrelationClauseContext correlationClause()
		{
			return getRuleContext(CorrelationClauseContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterJoinP(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitJoinP(this);
			}
		}

		public TerminalNode JOINTYPE()
		{
			return getToken(SelectParser.JOINTYPE, 0);
		}

		public SearchConditionContext searchCondition()
		{
			return getRuleContext(SearchConditionContext.class, 0);
		}

		public List<TableReferenceContext> tableReference()
		{
			return getRuleContexts(TableReferenceContext.class);
		}

		public TableReferenceContext tableReference(int i)
		{
			return getRuleContext(TableReferenceContext.class, i);
		}
	}
	public static class ListContext extends ExpressionContext
	{
		public ListContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitList(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterList(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitList(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}
	}
	public static class LiteralContext extends ParserRuleContext
	{
		public LiteralContext()
		{
		}

		public LiteralContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(LiteralContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_literal;
		}
	}

	public static class LoadContext extends ParserRuleContext
	{
		public LoadContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitLoad(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public AnyContext any()
		{
			return getRuleContext(AnyContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterLoad(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitLoad(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_load;
		}

		public RemainderContext remainder()
		{
			return getRuleContext(RemainderContext.class, 0);
		}

		public TerminalNode REPLACE()
		{
			return getToken(SelectParser.REPLACE, 0);
		}

		public TerminalNode RESUME()
		{
			return getToken(SelectParser.RESUME, 0);
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class Long2Context extends ParserRuleContext
	{
		public Long2Context(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitLong2(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterLong2(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitLong2(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_long2;
		}
	}

	public static class MulDivContext extends ExpressionContext
	{
		public Token op;

		public MulDivContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitMulDiv(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterMulDiv(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitMulDiv(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}

		public TerminalNode STAR()
		{
			return getToken(SelectParser.STAR, 0);
		}
	}

	public static class NestedTableContext extends TableReferenceContext
	{
		public NestedTableContext(TableReferenceContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNestedTable(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public CorrelationClauseContext correlationClause()
		{
			return getRuleContext(CorrelationClauseContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNestedTable(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNestedTable(this);
			}
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}
	}

	public static class NodeExpContext extends ParserRuleContext
	{
		public NodeExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNodeExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode ANYTEXT()
		{
			return getToken(SelectParser.ANYTEXT, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNodeExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNodeExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_nodeExp;
		}

		public RealNodeExpContext realNodeExp()
		{
			return getRuleContext(RealNodeExpContext.class, 0);
		}
	}

	public static class NormalPredicateContext extends PredicateContext
	{
		public NormalPredicateContext(PredicateContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNormalPredicate(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNormalPredicate(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNormalPredicate(this);
			}
		}

		public List<ExpressionContext> expression()
		{
			return getRuleContexts(ExpressionContext.class);
		}

		public ExpressionContext expression(int i)
		{
			return getRuleContext(ExpressionContext.class, i);
		}

		public OperatorContext operator()
		{
			return getRuleContext(OperatorContext.class, 0);
		}
	}

	public static class NotNullContext extends ParserRuleContext
	{
		public NotNullContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNotNull(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNotNull(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNotNull(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_notNull;
		}

		public TerminalNode NOT()
		{
			return getToken(SelectParser.NOT, 0);
		}

		public TerminalNode NULL()
		{
			return getToken(SelectParser.NULL, 0);
		}
	}

	public static class NullExpContext extends ExpressionContext
	{
		public NullExpContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNullExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNullExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNullExp(this);
			}
		}

		public TerminalNode NULL()
		{
			return getToken(SelectParser.NULL, 0);
		}
	}
	public static class NullPredicateContext extends PredicateContext
	{
		public NullPredicateContext(PredicateContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNullPredicate(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNullPredicate(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNullPredicate(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}

		public TerminalNode NULLOPERATOR()
		{
			return getToken(SelectParser.NULLOPERATOR, 0);
		}
	}
	public static class NumericLiteralContext extends LiteralContext
	{
		public NumericLiteralContext(LiteralContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitNumericLiteral(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterNumericLiteral(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitNumericLiteral(this);
			}
		}

		public List<TerminalNode> INTEGER()
		{
			return getTokens(SelectParser.INTEGER);
		}

		public TerminalNode INTEGER(int i)
		{
			return getToken(SelectParser.INTEGER, i);
		}

		public TerminalNode NEGATIVE()
		{
			return getToken(SelectParser.NEGATIVE, 0);
		}
	}
	public static class OperatorContext extends ParserRuleContext
	{
		public OperatorContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitOperator(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterOperator(this);
			}
		}

		public TerminalNode EQUALS()
		{
			return getToken(SelectParser.EQUALS, 0);
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitOperator(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_operator;
		}

		public TerminalNode OPERATOR()
		{
			return getToken(SelectParser.OPERATOR, 0);
		}
	}

	public static class OrderByContext extends ParserRuleContext
	{
		public OrderByContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitOrderBy(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterOrderBy(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitOrderBy(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_orderBy;
		}

		public List<SortKeyContext> sortKey()
		{
			return getRuleContexts(SortKeyContext.class);
		}

		public SortKeyContext sortKey(int i)
		{
			return getRuleContext(SortKeyContext.class, i);
		}
	}

	public static class OrganizationContext extends ParserRuleContext
	{
		public OrganizationContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitOrganization(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterOrganization(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitOrganization(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_organization;
		}

		public List<TerminalNode> INTEGER()
		{
			return getTokens(SelectParser.INTEGER);
		}

		public TerminalNode INTEGER(int i)
		{
			return getToken(SelectParser.INTEGER, i);
		}

		public TerminalNode ORGANIZATION()
		{
			return getToken(SelectParser.ORGANIZATION, 0);
		}
	}

	public static class PExpressionContext extends ExpressionContext
	{
		public PExpressionContext(ExpressionContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitPExpression(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterPExpression(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitPExpression(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}
	}

	public static class PredicateContext extends ParserRuleContext
	{
		public PredicateContext()
		{
		}

		public PredicateContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(PredicateContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_predicate;
		}
	}
	public static class PrimaryContext extends ParserRuleContext
	{
		public PrimaryContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitPrimary(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterPrimary(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitPrimary(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_primary;
		}
	}
	public static class PrimaryKeyContext extends ParserRuleContext
	{
		public PrimaryKeyContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitPrimaryKey(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ColumnNameContext> columnName()
		{
			return getRuleContexts(ColumnNameContext.class);
		}

		public ColumnNameContext columnName(int i)
		{
			return getRuleContext(ColumnNameContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterPrimaryKey(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitPrimaryKey(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_primaryKey;
		}
	}
	public static class RangeExpContext extends ParserRuleContext
	{
		public RangeExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRangeExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRangeExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRangeExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_rangeExp;
		}
	}
	public static class RangeSetContext extends ParserRuleContext
	{
		public RangeSetContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRangeSet(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRangeSet(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRangeSet(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_rangeSet;
		}

		public List<RangeExpContext> rangeExp()
		{
			return getRuleContexts(RangeExpContext.class);
		}

		public RangeExpContext rangeExp(int i)
		{
			return getRuleContext(RangeExpContext.class, i);
		}
	}
	public static class RangeTypeContext extends ParserRuleContext
	{
		public RangeTypeContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRangeType(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRangeType(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRangeType(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_rangeType;
		}

		public TerminalNode RANGE()
		{
			return getToken(SelectParser.RANGE, 0);
		}

		public RangeSetContext rangeSet()
		{
			return getRuleContext(RangeSetContext.class, 0);
		}
	}
	public static class RealGroupExpContext extends ParserRuleContext
	{
		public RealGroupExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRealGroupExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRealGroupExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRealGroupExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_realGroupExp;
		}

		public List<GroupDefContext> groupDef()
		{
			return getRuleContexts(GroupDefContext.class);
		}

		public GroupDefContext groupDef(int i)
		{
			return getRuleContext(GroupDefContext.class, i);
		}

		public HashExpContext hashExp()
		{
			return getRuleContext(HashExpContext.class, 0);
		}

		public RangeTypeContext rangeType()
		{
			return getRuleContext(RangeTypeContext.class, 0);
		}
	}
	public static class RealNodeExpContext extends ParserRuleContext
	{
		public RealNodeExpContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRealNodeExp(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode ALL()
		{
			return getToken(SelectParser.ALL, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRealNodeExp(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRealNodeExp(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_realNodeExp;
		}

		public HashExpContext hashExp()
		{
			return getRuleContext(HashExpContext.class, 0);
		}

		public IntegerSetContext integerSet()
		{
			return getRuleContext(IntegerSetContext.class, 0);
		}

		public RangeTypeContext rangeType()
		{
			return getRuleContext(RangeTypeContext.class, 0);
		}
	}
	public static class RemainderContext extends ParserRuleContext
	{
		public RemainderContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRemainder(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRemainder(this);
			}
		}

		public TerminalNode EOF()
		{
			return getToken(Recognizer.EOF, 0);
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRemainder(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_remainder;
		}
	}
	public static class RunstatsContext extends ParserRuleContext
	{
		public RunstatsContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitRunstats(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterRunstats(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitRunstats(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_runstats;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}
	public static class SearchClauseContext extends ParserRuleContext
	{
		public SearchClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSearchClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSearchClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSearchClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_searchClause;
		}

		public TerminalNode NOT()
		{
			return getToken(SelectParser.NOT, 0);
		}

		public PredicateContext predicate()
		{
			return getRuleContext(PredicateContext.class, 0);
		}

		public SearchConditionContext searchCondition()
		{
			return getRuleContext(SearchConditionContext.class, 0);
		}
	}
	public static class SearchConditionContext extends ParserRuleContext
	{
		public SearchConditionContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSearchCondition(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<ConnectedSearchClauseContext> connectedSearchClause()
		{
			return getRuleContexts(ConnectedSearchClauseContext.class);
		}

		public ConnectedSearchClauseContext connectedSearchClause(int i)
		{
			return getRuleContext(ConnectedSearchClauseContext.class, i);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSearchCondition(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSearchCondition(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_searchCondition;
		}

		public SearchClauseContext searchClause()
		{
			return getRuleContext(SearchClauseContext.class, 0);
		}
	}
	public static class SelectClauseContext extends ParserRuleContext
	{
		public SelectClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSelectClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSelectClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSelectClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_selectClause;
		}

		public SelecthowContext selecthow()
		{
			return getRuleContext(SelecthowContext.class, 0);
		}

		public List<SelectListEntryContext> selectListEntry()
		{
			return getRuleContexts(SelectListEntryContext.class);
		}

		public SelectListEntryContext selectListEntry(int i)
		{
			return getRuleContext(SelectListEntryContext.class, i);
		}

		public TerminalNode STAR()
		{
			return getToken(SelectParser.STAR, 0);
		}
	}
	public static class SelectColumnContext extends SelectListEntryContext
	{
		public SelectColumnContext(SelectListEntryContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSelectColumn(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSelectColumn(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSelectColumn(this);
			}
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}

	public static class SelectContext extends ParserRuleContext
	{
		public SelectContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSelect(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public List<CommonTableExpressionContext> commonTableExpression()
		{
			return getRuleContexts(CommonTableExpressionContext.class);
		}

		public CommonTableExpressionContext commonTableExpression(int i)
		{
			return getRuleContext(CommonTableExpressionContext.class, i);
		}

		public CreateIndexContext createIndex()
		{
			return getRuleContext(CreateIndexContext.class, 0);
		}

		public CreateTableContext createTable()
		{
			return getRuleContext(CreateTableContext.class, 0);
		}

		public CreateViewContext createView()
		{
			return getRuleContext(CreateViewContext.class, 0);
		}

		public DeleteContext delete()
		{
			return getRuleContext(DeleteContext.class, 0);
		}

		public DropIndexContext dropIndex()
		{
			return getRuleContext(DropIndexContext.class, 0);
		}

		public DropTableContext dropTable()
		{
			return getRuleContext(DropTableContext.class, 0);
		}

		public DropViewContext dropView()
		{
			return getRuleContext(DropViewContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSelect(this);
			}
		}

		public TerminalNode EOF()
		{
			return getToken(Recognizer.EOF, 0);
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSelect(this);
			}
		}

		public FullSelectContext fullSelect()
		{
			return getRuleContext(FullSelectContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_select;
		}

		public InsertContext insert()
		{
			return getRuleContext(InsertContext.class, 0);
		}

		public LoadContext load()
		{
			return getRuleContext(LoadContext.class, 0);
		}

		public RunstatsContext runstats()
		{
			return getRuleContext(RunstatsContext.class, 0);
		}

		public UpdateContext update()
		{
			return getRuleContext(UpdateContext.class, 0);
		}
	}

	public static class SelectExpressionContext extends SelectListEntryContext
	{
		public SelectExpressionContext(SelectListEntryContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSelectExpression(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSelectExpression(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSelectExpression(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}

	public static class SelecthowContext extends ParserRuleContext
	{
		public SelecthowContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSelecthow(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode ALL()
		{
			return getToken(SelectParser.ALL, 0);
		}

		public TerminalNode DISTINCT()
		{
			return getToken(SelectParser.DISTINCT, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSelecthow(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSelecthow(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_selecthow;
		}
	}

	public static class SelectListEntryContext extends ParserRuleContext
	{
		public SelectListEntryContext()
		{
		}

		public SelectListEntryContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(SelectListEntryContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_selectListEntry;
		}
	}

	public static class SingleTableContext extends ParserRuleContext
	{
		public SingleTableContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSingleTable(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public CorrelationClauseContext correlationClause()
		{
			return getRuleContext(CorrelationClauseContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSingleTable(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSingleTable(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_singleTable;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}
	}

	public static class SortKeyColContext extends SortKeyContext
	{
		public SortKeyColContext(SortKeyContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSortKeyCol(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		public TerminalNode DIRECTION()
		{
			return getToken(SelectParser.DIRECTION, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSortKeyCol(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSortKeyCol(this);
			}
		}
	}

	public static class SortKeyContext extends ParserRuleContext
	{
		public SortKeyContext()
		{
		}

		public SortKeyContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(SortKeyContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_sortKey;
		}
	}
	public static class SortKeyIntContext extends SortKeyContext
	{
		public SortKeyIntContext(SortKeyContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSortKeyInt(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public TerminalNode DIRECTION()
		{
			return getToken(SelectParser.DIRECTION, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSortKeyInt(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSortKeyInt(this);
			}
		}

		public TerminalNode INTEGER()
		{
			return getToken(SelectParser.INTEGER, 0);
		}
	}
	public static class StringLiteralContext extends LiteralContext
	{
		public StringLiteralContext(LiteralContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitStringLiteral(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterStringLiteral(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitStringLiteral(this);
			}
		}

		public TerminalNode STRING()
		{
			return getToken(SelectParser.STRING, 0);
		}
	}

	public static class SubSelectContext extends ParserRuleContext
	{
		public SubSelectContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitSubSelect(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterSubSelect(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitSubSelect(this);
			}
		}

		public FetchFirstContext fetchFirst()
		{
			return getRuleContext(FetchFirstContext.class, 0);
		}

		public FromClauseContext fromClause()
		{
			return getRuleContext(FromClauseContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_subSelect;
		}

		public GroupByContext groupBy()
		{
			return getRuleContext(GroupByContext.class, 0);
		}

		public HavingClauseContext havingClause()
		{
			return getRuleContext(HavingClauseContext.class, 0);
		}

		public OrderByContext orderBy()
		{
			return getRuleContext(OrderByContext.class, 0);
		}

		public SelectClauseContext selectClause()
		{
			return getRuleContext(SelectClauseContext.class, 0);
		}

		public WhereClauseContext whereClause()
		{
			return getRuleContext(WhereClauseContext.class, 0);
		}
	}

	public static class Table1PartContext extends TableNameContext
	{
		public Table1PartContext(TableNameContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitTable1Part(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterTable1Part(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitTable1Part(this);
			}
		}

		public TerminalNode IDENTIFIER()
		{
			return getToken(SelectParser.IDENTIFIER, 0);
		}
	}
	public static class Table2PartContext extends TableNameContext
	{
		public Table2PartContext(TableNameContext ctx)
		{
			copyFrom(ctx);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitTable2Part(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterTable2Part(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitTable2Part(this);
			}
		}

		public List<TerminalNode> IDENTIFIER()
		{
			return getTokens(SelectParser.IDENTIFIER);
		}

		public TerminalNode IDENTIFIER(int i)
		{
			return getToken(SelectParser.IDENTIFIER, i);
		}
	}
	public static class TableNameContext extends ParserRuleContext
	{
		public TableNameContext()
		{
		}

		public TableNameContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(TableNameContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_tableName;
		}
	}

	public static class TableReferenceContext extends ParserRuleContext
	{
		public TableReferenceContext()
		{
		}

		public TableReferenceContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		public void copyFrom(TableReferenceContext ctx)
		{
			super.copyFrom(ctx);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_tableReference;
		}
	}
	public static class UpdateContext extends ParserRuleContext
	{
		public UpdateContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitUpdate(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		public ColListContext colList()
		{
			return getRuleContext(ColListContext.class, 0);
		}

		public ColumnNameContext columnName()
		{
			return getRuleContext(ColumnNameContext.class, 0);
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterUpdate(this);
			}
		}

		public TerminalNode EQUALS()
		{
			return getToken(SelectParser.EQUALS, 0);
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitUpdate(this);
			}
		}

		public ExpressionContext expression()
		{
			return getRuleContext(ExpressionContext.class, 0);
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_update;
		}

		public TableNameContext tableName()
		{
			return getRuleContext(TableNameContext.class, 0);
		}

		public WhereClauseContext whereClause()
		{
			return getRuleContext(WhereClauseContext.class, 0);
		}
	}
	public static class WhereClauseContext extends ParserRuleContext
	{
		public WhereClauseContext(ParserRuleContext parent, int invokingState)
		{
			super(parent, invokingState);
		}

		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor)
		{
			if (visitor instanceof SelectVisitor)
			{
				return ((SelectVisitor<? extends T>)visitor).visitWhereClause(this);
			}
			else
			{
				return visitor.visitChildren(this);
			}
		}

		@Override
		public void enterRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).enterWhereClause(this);
			}
		}

		@Override
		public void exitRule(ParseTreeListener listener)
		{
			if (listener instanceof SelectListener)
			{
				((SelectListener)listener).exitWhereClause(this);
			}
		}

		@Override
		public int getRuleIndex()
		{
			return RULE_whereClause;
		}

		public SearchConditionContext searchCondition()
		{
			return getRuleContext(SearchConditionContext.class, 0);
		}
	}
}