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
	public static final int T__52 = 1, T__51 = 2, T__50 = 3, T__49 = 4, T__48 = 5, T__47 = 6, T__46 = 7, T__45 = 8, T__44 = 9, T__43 = 10, T__42 = 11, T__41 = 12, T__40 = 13, T__39 = 14, T__38 = 15, T__37 = 16, T__36 = 17, T__35 = 18, T__34 = 19, T__33 = 20, T__32 = 21, T__31 = 22, T__30 = 23, T__29 = 24, T__28 = 25, T__27 = 26, T__26 = 27, T__25 = 28, T__24 = 29, T__23 = 30, T__22 = 31, T__21 = 32, T__20 = 33, T__19 = 34, T__18 = 35, T__17 = 36, T__16 = 37, T__15 = 38, T__14 = 39, T__13 = 40, T__12 = 41, T__11 = 42, T__10 = 43, T__9 = 44, T__8 = 45, T__7 = 46, T__6 = 47, T__5 = 48, T__4 = 49, T__3 = 50, T__2 = 51, T__1 = 52, T__0 = 53, STRING = 54, STAR = 55, COUNT = 56, CONCAT = 57, NEGATIVE = 58, EQUALS = 59, OPERATOR = 60, NULLOPERATOR = 61, AND = 62, OR = 63, NOT = 64, NULL = 65, DIRECTION = 66, JOINTYPE = 67, CROSSJOIN = 68, TABLECOMBINATION = 69, DISTINCT = 70, INTEGER = 71, WS = 72, UNIQUE = 73, REPLACE = 74, RESUME = 75, NONE = 76, ALL = 77, ANYTEXT = 78, HASH = 79, RANGE = 80, DATE = 81, IDENTIFIER = 82, ANY = 83;
	public static final String[] tokenNames = { "<INVALID>", "'GROUP'", "'INDEX'", "'WHERE'", "'DELETE'", "'END'", "'AS'", "'ELSE'", "'LOAD'", "'PRIMARY'", "'}'", "'CASE'", "'ORDER'", "'SET'", "')'", "'CHAR'", "'TABLE'", "'RUNSTATS'", "'ROW'", "'FLOAT'", "'DROP'", "'FROM'", "'INSERT'", "'|'", "'SELECT'", "'INTEGER'", "'THEN'", "'EXISTS'", "'BY'", "','", "'HAVING'", "'('", "'VIEW'", "'{'", "'BIGINT'", "'VALUES'", "'FIRST'", "'JOIN'", "'VARCHAR'", "'INTO'", "'.'", "'+'", "'UPDATE'", "'ONLY'", "'KEY'", "'CREATE'", "'FETCH'", "'ROWS'", "'ON'", "'DOUBLE'", "'DELIMITER'", "'WHEN'", "'WITH'", "'/'", "STRING", "'*'", "'COUNT'", "'||'", "'-'", "'='", "OPERATOR", "NULLOPERATOR", "'AND'", "'OR'", "'NOT'", "'NULL'", "DIRECTION", "JOINTYPE", "'CROSS JOIN'", "TABLECOMBINATION", "'DISTINCT'", "INTEGER", "WS", "'UNIQUE'", "'REPLACE'", "'RESUME'", "'NONE'", "'ALL'", "'ANY'", "'HASH'", "'RANGE'", "'DATE'", "IDENTIFIER", "ANY" };
	public static final int RULE_select = 0, RULE_runstats = 1, RULE_insert = 2, RULE_update = 3, RULE_delete = 4, RULE_createTable = 5, RULE_groupExp = 6, RULE_realGroupExp = 7, RULE_groupDef = 8, RULE_rangeExp = 9, RULE_nodeExp = 10, RULE_realNodeExp = 11, RULE_integerSet = 12, RULE_hashExp = 13, RULE_columnSet = 14, RULE_rangeType = 15, RULE_rangeSet = 16, RULE_deviceExp = 17, RULE_dropTable = 18, RULE_createView = 19, RULE_dropView = 20, RULE_createIndex = 21, RULE_dropIndex = 22, RULE_load = 23, RULE_any = 24, RULE_remainder = 25, RULE_indexDef = 26, RULE_colDef = 27, RULE_primaryKey = 28, RULE_notNull = 29, RULE_primary = 30, RULE_dataType = 31, RULE_char2 = 32, RULE_int2 = 33, RULE_long2 = 34, RULE_date2 = 35, RULE_float2 = 36, RULE_colList = 37, RULE_commonTableExpression = 38, RULE_fullSelect = 39, RULE_connectedSelect = 40, RULE_subSelect = 41, RULE_selectClause = 42, RULE_selecthow = 43, RULE_selectListEntry = 44, RULE_fromClause = 45, RULE_tableReference = 46, RULE_singleTable = 47, RULE_whereClause = 48, RULE_groupBy = 49, RULE_havingClause = 50, RULE_orderBy = 51, RULE_sortKey = 52, RULE_correlationClause = 53, RULE_fetchFirst = 54, RULE_tableName = 55, RULE_columnName = 56, RULE_searchCondition = 57, RULE_connectedSearchClause = 58, RULE_searchClause = 59, RULE_predicate = 60, RULE_operator = 61, RULE_expression = 62, RULE_caseCase = 63, RULE_identifier = 64, RULE_literal = 65;
	public static final String[] ruleNames = { "select", "runstats", "insert", "update", "delete", "createTable", "groupExp", "realGroupExp", "groupDef", "rangeExp", "nodeExp", "realNodeExp", "integerSet", "hashExp", "columnSet", "rangeType", "rangeSet", "deviceExp", "dropTable", "createView", "dropView", "createIndex", "dropIndex", "load", "any", "remainder", "indexDef", "colDef", "primaryKey", "notNull", "primary", "dataType", "char2", "int2", "long2", "date2", "float2", "colList", "commonTableExpression", "fullSelect", "connectedSelect", "subSelect", "selectClause", "selecthow", "selectListEntry", "fromClause", "tableReference", "singleTable", "whereClause", "groupBy", "havingClause", "orderBy", "sortKey", "correlationClause", "fetchFirst", "tableName", "columnName", "searchCondition", "connectedSearchClause", "searchClause", "predicate", "operator", "expression", "caseCase", "identifier", "literal" };

	public static final String _serializedATN = "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3U\u0324\4\2\t\2\4" + "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t" + "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22" + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31" + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!" + "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4" + ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t" + "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t=" + "\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3" + "\2\3\2\3\2\3\2\3\2\3\2\3\2\7\2\u0096\n\2\f\2\16\2\u0099\13\2\5\2\u009b" + "\n\2\3\2\5\2\u009e\n\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\5\4\u00a8\n\4\3" + "\4\3\4\3\4\3\4\3\4\3\4\7\4\u00b0\n\4\f\4\16\4\u00b3\13\4\3\4\3\4\5\4\u00b7" + "\n\4\3\5\3\5\3\5\3\5\3\5\5\5\u00be\n\5\3\5\3\5\3\5\5\5\u00c3\n\5\3\6\3" + "\6\3\6\3\6\5\6\u00c9\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\7\7\u00d2\n\7\f\7" + "\16\7\u00d5\13\7\3\7\3\7\5\7\u00d9\n\7\3\7\3\7\5\7\u00dd\n\7\3\7\3\7\3" + "\7\3\b\3\b\5\b\u00e4\n\b\3\t\3\t\3\t\3\t\7\t\u00ea\n\t\f\t\16\t\u00ed" + "\13\t\3\t\3\t\3\t\3\t\5\t\u00f3\n\t\5\t\u00f5\n\t\3\n\3\n\3\n\3\n\7\n" + "\u00fb\n\n\f\n\16\n\u00fe\13\n\3\n\3\n\3\13\7\13\u0103\n\13\f\13\16\13" + "\u0106\13\13\3\f\3\f\5\f\u010a\n\f\3\r\3\r\5\r\u010e\n\r\3\r\3\r\3\r\5" + "\r\u0113\n\r\5\r\u0115\n\r\3\16\3\16\3\16\3\16\7\16\u011b\n\16\f\16\16" + "\16\u011e\13\16\3\16\3\16\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\7\20" + "\u012a\n\20\f\20\16\20\u012d\13\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21" + "\3\21\3\22\3\22\3\22\3\22\7\22\u013b\n\22\f\22\16\22\u013e\13\22\3\22" + "\3\22\3\23\3\23\5\23\u0144\n\23\3\23\3\23\3\23\5\23\u0149\n\23\5\23\u014b" + "\n\23\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26" + "\3\26\3\27\3\27\5\27\u015d\n\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27" + "\7\27\u0167\n\27\f\27\16\27\u016a\13\27\3\27\3\27\3\30\3\30\3\30\3\30" + "\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u0178\n\31\3\31\3\31\3\31\3\32\3\32" + "\3\33\7\33\u0180\n\33\f\33\16\33\u0183\13\33\3\33\3\33\3\34\3\34\5\34" + "\u0189\n\34\3\35\3\35\3\35\5\35\u018e\n\35\3\35\5\35\u0191\n\35\3\36\3" + "\36\3\36\3\36\3\36\3\36\7\36\u0199\n\36\f\36\16\36\u019c\13\36\3\36\3" + "\36\3\37\3\37\3\37\3 \3 \3 \3!\3!\3!\3!\3!\5!\u01ab\n!\3\"\3\"\3\"\3\"" + "\3\"\3#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3\'\3\'\7\'\u01be\n\'\f\'\16\'\u01c1" + "\13\'\3\'\3\'\3(\3(\3(\3(\3(\7(\u01ca\n(\f(\16(\u01cd\13(\3(\3(\5(\u01d1" + "\n(\3(\3(\3(\3(\3(\3)\3)\3)\3)\3)\5)\u01dd\n)\3)\7)\u01e0\n)\f)\16)\u01e3" + "\13)\3)\5)\u01e6\n)\3)\5)\u01e9\n)\3*\3*\3*\3*\3*\3*\5*\u01f1\n*\3+\3" + "+\3+\5+\u01f6\n+\3+\5+\u01f9\n+\3+\5+\u01fc\n+\3+\5+\u01ff\n+\3+\5+\u0202" + "\n+\3,\3,\5,\u0206\n,\3,\3,\3,\3,\7,\u020c\n,\f,\16,\u020f\13,\5,\u0211" + "\n,\3-\3-\3.\3.\5.\u0217\n.\3.\5.\u021a\n.\3.\3.\5.\u021e\n.\3.\5.\u0221" + "\n.\5.\u0223\n.\3/\3/\3/\3/\7/\u0229\n/\f/\16/\u022c\13/\3\60\3\60\3\60" + "\3\60\5\60\u0232\n\60\3\60\3\60\3\60\3\60\3\60\3\60\5\60\u023a\n\60\3" + "\60\3\60\3\60\3\60\3\60\3\60\5\60\u0242\n\60\3\60\3\60\3\60\3\60\5\60" + "\u0248\n\60\3\60\5\60\u024b\n\60\3\60\3\60\5\60\u024f\n\60\3\60\3\60\3" + "\60\3\60\3\60\5\60\u0256\n\60\3\60\3\60\3\60\3\60\5\60\u025c\n\60\7\60" + "\u025e\n\60\f\60\16\60\u0261\13\60\3\61\3\61\5\61\u0265\n\61\3\62\3\62" + "\3\62\3\63\3\63\3\63\3\63\3\63\7\63\u026f\n\63\f\63\16\63\u0272\13\63" + "\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\7\65\u027c\n\65\f\65\16\65\u027f" + "\13\65\3\66\3\66\5\66\u0283\n\66\3\66\3\66\5\66\u0287\n\66\5\66\u0289" + "\n\66\3\67\5\67\u028c\n\67\3\67\3\67\38\38\38\58\u0293\n8\38\38\38\39" + "\39\39\39\59\u029c\n9\3:\3:\3:\3:\5:\u02a2\n:\3;\3;\7;\u02a6\n;\f;\16" + ";\u02a9\13;\3<\3<\3<\3=\5=\u02af\n=\3=\3=\3=\3=\3=\5=\u02b6\n=\3>\3>\3" + ">\3>\3>\3>\3>\3>\3>\3>\3>\3>\5>\u02c4\n>\3?\3?\3@\3@\3@\3@\3@\3@\7@\u02ce" + "\n@\f@\16@\u02d1\13@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\6@\u02df\n@\r" + "@\16@\u02e0\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3" + "@\7@\u02f7\n@\f@\16@\u02fa\13@\3@\3@\3@\3@\5@\u0300\n@\5@\u0302\n@\3@" + "\3@\3@\3@\3@\3@\3@\3@\3@\7@\u030d\n@\f@\16@\u0310\13@\3A\3A\3A\3A\3A\3" + "B\3B\3C\5C\u031a\nC\3C\3C\3C\5C\u031f\nC\3C\5C\u0322\nC\3C\3\u0104\4^" + "~D\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BD" + "FHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\2\f\3\2LM\4\2\21\21(" + "(\4\2\25\25\63\63\4\2HHOO\4\2\24\24\61\61\3\2@A\3\2=>\4\2\67\6799\4\2" + "++<<\4\2::ST\u0357\2\u009d\3\2\2\2\4\u009f\3\2\2\2\6\u00a3\3\2\2\2\b\u00b8" + "\3\2\2\2\n\u00c4\3\2\2\2\f\u00ca\3\2\2\2\16\u00e3\3\2\2\2\20\u00e5\3\2" + "\2\2\22\u00f6\3\2\2\2\24\u0104\3\2\2\2\26\u0109\3\2\2\2\30\u010d\3\2\2" + "\2\32\u0116\3\2\2\2\34\u0121\3\2\2\2\36\u0125\3\2\2\2 \u0130\3\2\2\2\"" + "\u0136\3\2\2\2$\u0143\3\2\2\2&\u014c\3\2\2\2(\u0150\3\2\2\2*\u0156\3\2" + "\2\2,\u015a\3\2\2\2.\u016d\3\2\2\2\60\u0171\3\2\2\2\62\u017c\3\2\2\2\64" + "\u0181\3\2\2\2\66\u0186\3\2\2\28\u018a\3\2\2\2:\u0192\3\2\2\2<\u019f\3" + "\2\2\2>\u01a2\3\2\2\2@\u01aa\3\2\2\2B\u01ac\3\2\2\2D\u01b1\3\2\2\2F\u01b3" + "\3\2\2\2H\u01b5\3\2\2\2J\u01b7\3\2\2\2L\u01b9\3\2\2\2N\u01c4\3\2\2\2P" + "\u01dc\3\2\2\2R\u01ea\3\2\2\2T\u01f2\3\2\2\2V\u0203\3\2\2\2X\u0212\3\2" + "\2\2Z\u0222\3\2\2\2\\\u0224\3\2\2\2^\u024a\3\2\2\2`\u0262\3\2\2\2b\u0266" + "\3\2\2\2d\u0269\3\2\2\2f\u0273\3\2\2\2h\u0276\3\2\2\2j\u0288\3\2\2\2l" + "\u028b\3\2\2\2n\u028f\3\2\2\2p\u029b\3\2\2\2r\u02a1\3\2\2\2t\u02a3\3\2" + "\2\2v\u02aa\3\2\2\2x\u02ae\3\2\2\2z\u02c3\3\2\2\2|\u02c5\3\2\2\2~\u0301" + "\3\2\2\2\u0080\u0311\3\2\2\2\u0082\u0316\3\2\2\2\u0084\u0321\3\2\2\2\u0086" + "\u009e\5\6\4\2\u0087\u009e\5\b\5\2\u0088\u009e\5\n\6\2\u0089\u009e\5\f" + "\7\2\u008a\u009e\5,\27\2\u008b\u009e\5(\25\2\u008c\u009e\5&\24\2\u008d" + "\u009e\5.\30\2\u008e\u009e\5*\26\2\u008f\u009e\5\60\31\2\u0090\u009e\5" + "\4\3\2\u0091\u0092\7\66\2\2\u0092\u0097\5N(\2\u0093\u0094\7\37\2\2\u0094" + "\u0096\5N(\2\u0095\u0093\3\2\2\2\u0096\u0099\3\2\2\2\u0097\u0095\3\2\2" + "\2\u0097\u0098\3\2\2\2\u0098\u009b\3\2\2\2\u0099\u0097\3\2\2\2\u009a\u0091" + "\3\2\2\2\u009a\u009b\3\2\2\2\u009b\u009c\3\2\2\2\u009c\u009e\5P)\2\u009d" + "\u0086\3\2\2\2\u009d\u0087\3\2\2\2\u009d\u0088\3\2\2\2\u009d\u0089\3\2" + "\2\2\u009d\u008a\3\2\2\2\u009d\u008b\3\2\2\2\u009d\u008c\3\2\2\2\u009d" + "\u008d\3\2\2\2\u009d\u008e\3\2\2\2\u009d\u008f\3\2\2\2\u009d\u0090\3\2" + "\2\2\u009d\u009a\3\2\2\2\u009e\3\3\2\2\2\u009f\u00a0\7\23\2\2\u00a0\u00a1" + "\7\62\2\2\u00a1\u00a2\5p9\2\u00a2\5\3\2\2\2\u00a3\u00a4\7\30\2\2\u00a4" + "\u00a5\7)\2\2\u00a5\u00b6\5p9\2\u00a6\u00a8\7\27\2\2\u00a7\u00a6\3\2\2" + "\2\u00a7\u00a8\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00b7\5P)\2\u00aa\u00ab" + "\7%\2\2\u00ab\u00ac\7!\2\2\u00ac\u00b1\5~@\2\u00ad\u00ae\7\37\2\2\u00ae" + "\u00b0\5~@\2\u00af\u00ad\3\2\2\2\u00b0\u00b3\3\2\2\2\u00b1\u00af\3\2\2" + "\2\u00b1\u00b2\3\2\2\2\u00b2\u00b4\3\2\2\2\u00b3\u00b1\3\2\2\2\u00b4\u00b5" + "\7\20\2\2\u00b5\u00b7\3\2\2\2\u00b6\u00a7\3\2\2\2\u00b6\u00aa\3\2\2\2" + "\u00b7\7\3\2\2\2\u00b8\u00b9\7,\2\2\u00b9\u00ba\5p9\2\u00ba\u00bd\7\17" + "\2\2\u00bb\u00be\5r:\2\u00bc\u00be\5L\'\2\u00bd\u00bb\3\2\2\2\u00bd\u00bc" + "\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00c0\7=\2\2\u00c0\u00c2\5~@\2\u00c1" + "\u00c3\5b\62\2\u00c2\u00c1\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\t\3\2\2\2" + "\u00c4\u00c5\7\6\2\2\u00c5\u00c6\7\27\2\2\u00c6\u00c8\5p9\2\u00c7\u00c9" + "\5b\62\2\u00c8\u00c7\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9\13\3\2\2\2\u00ca" + "\u00cb\7/\2\2\u00cb\u00cc\7\22\2\2\u00cc\u00cd\5p9\2\u00cd\u00ce\7!\2" + "\2\u00ce\u00d3\58\35\2\u00cf\u00d0\7\37\2\2\u00d0\u00d2\58\35\2\u00d1" + "\u00cf\3\2\2\2\u00d2\u00d5\3\2\2\2\u00d3\u00d1\3\2\2\2\u00d3\u00d4\3\2" + "\2\2\u00d4\u00d8\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d6\u00d7\7\37\2\2\u00d7" + "\u00d9\5:\36\2\u00d8\u00d6\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d9\u00da\3\2" + "\2\2\u00da\u00dc\7\20\2\2\u00db\u00dd\5\16\b\2\u00dc\u00db\3\2\2\2\u00dc" + "\u00dd\3\2\2\2\u00dd\u00de\3\2\2\2\u00de\u00df\5\26\f\2\u00df\u00e0\5" + "$\23\2\u00e0\r\3\2\2\2\u00e1\u00e4\7N\2\2\u00e2\u00e4\5\20\t\2\u00e3\u00e1" + "\3\2\2\2\u00e3\u00e2\3\2\2\2\u00e4\17\3\2\2\2\u00e5\u00e6\7#\2\2\u00e6" + "\u00eb\5\22\n\2\u00e7\u00e8\7\31\2\2\u00e8\u00ea\5\22\n\2\u00e9\u00e7" + "\3\2\2\2\u00ea\u00ed\3\2\2\2\u00eb\u00e9\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec" + "\u00ee\3\2\2\2\u00ed\u00eb\3\2\2\2\u00ee\u00f4\7\f\2\2\u00ef\u00f2\7\37" + "\2\2\u00f0\u00f3\5\34\17\2\u00f1\u00f3\5 \21\2\u00f2\u00f0\3\2\2\2\u00f2" + "\u00f1\3\2\2\2\u00f3\u00f5\3\2\2\2\u00f4\u00ef\3\2\2\2\u00f4\u00f5\3\2" + "\2\2\u00f5\21\3\2\2\2\u00f6\u00f7\7#\2\2\u00f7\u00fc\7I\2\2\u00f8\u00f9" + "\7\31\2\2\u00f9\u00fb\7I\2\2\u00fa\u00f8\3\2\2\2\u00fb\u00fe\3\2\2\2\u00fc" + "\u00fa\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00ff\3\2\2\2\u00fe\u00fc\3\2" + "\2\2\u00ff\u0100\7\f\2\2\u0100\23\3\2\2\2\u0101\u0103\13\2\2\2\u0102\u0101" + "\3\2\2\2\u0103\u0106\3\2\2\2\u0104\u0105\3\2\2\2\u0104\u0102\3\2\2\2\u0105" + "\25\3\2\2\2\u0106\u0104\3\2\2\2\u0107\u010a\7P\2\2\u0108\u010a\5\30\r" + "\2\u0109\u0107\3\2\2\2\u0109\u0108\3\2\2\2\u010a\27\3\2\2\2\u010b\u010e" + "\7O\2\2\u010c\u010e\5\32\16\2\u010d\u010b\3\2\2\2\u010d\u010c\3\2\2\2" + "\u010e\u0114\3\2\2\2\u010f\u0112\7\37\2\2\u0110\u0113\5\34\17\2\u0111" + "\u0113\5 \21\2\u0112\u0110\3\2\2\2\u0112\u0111\3\2\2\2\u0113\u0115\3\2" + "\2\2\u0114\u010f\3\2\2\2\u0114\u0115\3\2\2\2\u0115\31\3\2\2\2\u0116\u0117" + "\7#\2\2\u0117\u011c\7I\2\2\u0118\u0119\7\31\2\2\u0119\u011b\7I\2\2\u011a" + "\u0118\3\2\2\2\u011b\u011e\3\2\2\2\u011c\u011a\3\2\2\2\u011c\u011d\3\2" + "\2\2\u011d\u011f\3\2\2\2\u011e\u011c\3\2\2\2\u011f\u0120\7\f\2\2\u0120" + "\33\3\2\2\2\u0121\u0122\7Q\2\2\u0122\u0123\7\37\2\2\u0123\u0124\5\36\20" + "\2\u0124\35\3\2\2\2\u0125\u0126\7#\2\2\u0126\u012b\5r:\2\u0127\u0128\7" + "\31\2\2\u0128\u012a\5r:\2\u0129\u0127\3\2\2\2\u012a\u012d\3\2\2\2\u012b" + "\u0129\3\2\2\2\u012b\u012c\3\2\2\2\u012c\u012e\3\2\2\2\u012d\u012b\3\2" + "\2\2\u012e\u012f\7\f\2\2\u012f\37\3\2\2\2\u0130\u0131\7R\2\2\u0131\u0132" + "\7\37\2\2\u0132\u0133\5r:\2\u0133\u0134\7\37\2\2\u0134\u0135\5\"\22\2" + "\u0135!\3\2\2\2\u0136\u0137\7#\2\2\u0137\u013c\5\24\13\2\u0138\u0139\7" + "\31\2\2\u0139\u013b\5\24\13\2\u013a\u0138\3\2\2\2\u013b\u013e\3\2\2\2" + "\u013c\u013a\3\2\2\2\u013c\u013d\3\2\2\2\u013d\u013f\3\2\2\2\u013e\u013c" + "\3\2\2\2\u013f\u0140\7\f\2\2\u0140#\3\2\2\2\u0141\u0144\7O\2\2\u0142\u0144" + "\5\32\16\2\u0143\u0141\3\2\2\2\u0143\u0142\3\2\2\2\u0144\u014a\3\2\2\2" + "\u0145\u0148\7\37\2\2\u0146\u0149\5\34\17\2\u0147\u0149\5\24\13\2\u0148" + "\u0146\3\2\2\2\u0148\u0147\3\2\2\2\u0149\u014b\3\2\2\2\u014a\u0145\3\2" + "\2\2\u014a\u014b\3\2\2\2\u014b%\3\2\2\2\u014c\u014d\7\26\2\2\u014d\u014e" + "\7\22\2\2\u014e\u014f\5p9\2\u014f\'\3\2\2\2\u0150\u0151\7/\2\2\u0151\u0152" + "\7\"\2\2\u0152\u0153\5p9\2\u0153\u0154\7\b\2\2\u0154\u0155\5P)\2\u0155" + ")\3\2\2\2\u0156\u0157\7\26\2\2\u0157\u0158\7\"\2\2\u0158\u0159\5p9\2\u0159" + "+\3\2\2\2\u015a\u015c\7/\2\2\u015b\u015d\7K\2\2\u015c\u015b\3\2\2\2\u015c" + "\u015d\3\2\2\2\u015d\u015e\3\2\2\2\u015e\u015f\7\4\2\2\u015f\u0160\5p" + "9\2\u0160\u0161\7\62\2\2\u0161\u0162\5p9\2\u0162\u0163\7!\2\2\u0163\u0168" + "\5\66\34\2\u0164\u0165\7\37\2\2\u0165\u0167\5\66\34\2\u0166\u0164\3\2" + "\2\2\u0167\u016a\3\2\2\2\u0168\u0166\3\2\2\2\u0168\u0169\3\2\2\2\u0169" + "\u016b\3\2\2\2\u016a\u0168\3\2\2\2\u016b\u016c\7\20\2\2\u016c-\3\2\2\2" + "\u016d\u016e\7\26\2\2\u016e\u016f\7\4\2\2\u016f\u0170\5p9\2\u0170/\3\2" + "\2\2\u0171\u0172\7\n\2\2\u0172\u0173\t\2\2\2\u0173\u0174\7)\2\2\u0174" + "\u0177\5p9\2\u0175\u0176\7\64\2\2\u0176\u0178\5\62\32\2\u0177\u0175\3" + "\2\2\2\u0177\u0178\3\2\2\2\u0178\u0179\3\2\2\2\u0179\u017a\7\27\2\2\u017a" + "\u017b\5\64\33\2\u017b\61\3\2\2\2\u017c\u017d\13\2\2\2\u017d\63\3\2\2" + "\2\u017e\u0180\13\2\2\2\u017f\u017e\3\2\2\2\u0180\u0183\3\2\2\2\u0181" + "\u017f\3\2\2\2\u0181\u0182\3\2\2\2\u0182\u0184\3\2\2\2\u0183\u0181\3\2" + "\2\2\u0184\u0185\7\2\2\3\u0185\65\3\2\2\2\u0186\u0188\5r:\2\u0187\u0189" + "\7D\2\2\u0188\u0187\3\2\2\2\u0188\u0189\3\2\2\2\u0189\67\3\2\2\2\u018a" + "\u018b\5r:\2\u018b\u018d\5@!\2\u018c\u018e\5<\37\2\u018d\u018c\3\2\2\2" + "\u018d\u018e\3\2\2\2\u018e\u0190\3\2\2\2\u018f\u0191\5> \2\u0190\u018f" + "\3\2\2\2\u0190\u0191\3\2\2\2\u01919\3\2\2\2\u0192\u0193\7\13\2\2\u0193" + "\u0194\7.\2\2\u0194\u0195\7!\2\2\u0195\u019a\5r:\2\u0196\u0197\7\37\2" + "\2\u0197\u0199\5r:\2\u0198\u0196\3\2\2\2\u0199\u019c\3\2\2\2\u019a\u0198" + "\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019d\3\2\2\2\u019c\u019a\3\2\2\2\u019d" + "\u019e\7\20\2\2\u019e;\3\2\2\2\u019f\u01a0\7B\2\2\u01a0\u01a1\7C\2\2\u01a1" + "=\3\2\2\2\u01a2\u01a3\7\13\2\2\u01a3\u01a4\7.\2\2\u01a4?\3\2\2\2\u01a5" + "\u01ab\5B\"\2\u01a6\u01ab\5D#\2\u01a7\u01ab\5F$\2\u01a8\u01ab\5H%\2\u01a9" + "\u01ab\5J&\2\u01aa\u01a5\3\2\2\2\u01aa\u01a6\3\2\2\2\u01aa\u01a7\3\2\2" + "\2\u01aa\u01a8\3\2\2\2\u01aa\u01a9\3\2\2\2\u01abA\3\2\2\2\u01ac\u01ad" + "\t\3\2\2\u01ad\u01ae\7!\2\2\u01ae\u01af\7I\2\2\u01af\u01b0\7\20\2\2\u01b0" + "C\3\2\2\2\u01b1\u01b2\7\33\2\2\u01b2E\3\2\2\2\u01b3\u01b4\7$\2\2\u01b4" + "G\3\2\2\2\u01b5\u01b6\7S\2\2\u01b6I\3\2\2\2\u01b7\u01b8\t\4\2\2\u01b8" + "K\3\2\2\2\u01b9\u01ba\7!\2\2\u01ba\u01bf\5r:\2\u01bb\u01bc\7\37\2\2\u01bc" + "\u01be\5r:\2\u01bd\u01bb\3\2\2\2\u01be\u01c1\3\2\2\2\u01bf\u01bd\3\2\2" + "\2\u01bf\u01c0\3\2\2\2\u01c0\u01c2\3\2\2\2\u01c1\u01bf\3\2\2\2\u01c2\u01c3" + "\7\20\2\2\u01c3M\3\2\2\2\u01c4\u01d0\7T\2\2\u01c5\u01c6\7!\2\2\u01c6\u01cb" + "\5r:\2\u01c7\u01c8\7\37\2\2\u01c8\u01ca\5r:\2\u01c9\u01c7\3\2\2\2\u01ca" + "\u01cd\3\2\2\2\u01cb\u01c9\3\2\2\2\u01cb\u01cc\3\2\2\2\u01cc\u01ce\3\2" + "\2\2\u01cd\u01cb\3\2\2\2\u01ce\u01cf\7\20\2\2\u01cf\u01d1\3\2\2\2\u01d0" + "\u01c5\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2\u01d3\7\b" + "\2\2\u01d3\u01d4\7!\2\2\u01d4\u01d5\5P)\2\u01d5\u01d6\7\20\2\2\u01d6O" + "\3\2\2\2\u01d7\u01dd\5T+\2\u01d8\u01d9\7!\2\2\u01d9\u01da\5P)\2\u01da" + "\u01db\7\20\2\2\u01db\u01dd\3\2\2\2\u01dc\u01d7\3\2\2\2\u01dc\u01d8\3" + "\2\2\2\u01dd\u01e1\3\2\2\2\u01de\u01e0\5R*\2\u01df\u01de\3\2\2\2\u01e0" + "\u01e3\3\2\2\2\u01e1\u01df\3\2\2\2\u01e1\u01e2\3\2\2\2\u01e2\u01e5\3\2" + "\2\2\u01e3\u01e1\3\2\2\2\u01e4\u01e6\5h\65\2\u01e5\u01e4\3\2\2\2\u01e5" + "\u01e6\3\2\2\2\u01e6\u01e8\3\2\2\2\u01e7\u01e9\5n8\2\u01e8\u01e7\3\2\2" + "\2\u01e8\u01e9\3\2\2\2\u01e9Q\3\2\2\2\u01ea\u01f0\7G\2\2\u01eb\u01f1\5" + "T+\2\u01ec\u01ed\7!\2\2\u01ed\u01ee\5P)\2\u01ee\u01ef\7\20\2\2\u01ef\u01f1" + "\3\2\2\2\u01f0\u01eb\3\2\2\2\u01f0\u01ec\3\2\2\2\u01f1S\3\2\2\2\u01f2" + "\u01f3\5V,\2\u01f3\u01f5\5\\/\2\u01f4\u01f6\5b\62\2\u01f5\u01f4\3\2\2" + "\2\u01f5\u01f6\3\2\2\2\u01f6\u01f8\3\2\2\2\u01f7\u01f9\5d\63\2\u01f8\u01f7" + "\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9\u01fb\3\2\2\2\u01fa\u01fc\5f\64\2\u01fb" + "\u01fa\3\2\2\2\u01fb\u01fc\3\2\2\2\u01fc\u01fe\3\2\2\2\u01fd\u01ff\5h" + "\65\2\u01fe\u01fd\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u0201\3\2\2\2\u0200" + "\u0202\5n8\2\u0201\u0200\3\2\2\2\u0201\u0202\3\2\2\2\u0202U\3\2\2\2\u0203" + "\u0205\7\32\2\2\u0204\u0206\5X-\2\u0205\u0204\3\2\2\2\u0205\u0206\3\2" + "\2\2\u0206\u0210\3\2\2\2\u0207\u0211\79\2\2\u0208\u020d\5Z.\2\u0209\u020a" + "\7\37\2\2\u020a\u020c\5Z.\2\u020b\u0209\3\2\2\2\u020c\u020f\3\2\2\2\u020d" + "\u020b\3\2\2\2\u020d\u020e\3\2\2\2\u020e\u0211\3\2\2\2\u020f\u020d\3\2" + "\2\2\u0210\u0207\3\2\2\2\u0210\u0208\3\2\2\2\u0211W\3\2\2\2\u0212\u0213" + "\t\5\2\2\u0213Y\3\2\2\2\u0214\u0216\5r:\2\u0215\u0217\7\b\2\2\u0216\u0215" + "\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u0219\3\2\2\2\u0218\u021a\7T\2\2\u0219" + "\u0218\3\2\2\2\u0219\u021a\3\2\2\2\u021a\u0223\3\2\2\2\u021b\u021d\5~" + "@\2\u021c\u021e\7\b\2\2\u021d\u021c\3\2\2\2\u021d\u021e\3\2\2\2\u021e" + "\u0220\3\2\2\2\u021f\u0221\7T\2\2\u0220\u021f\3\2\2\2\u0220\u0221\3\2" + "\2\2\u0221\u0223\3\2\2\2\u0222\u0214\3\2\2\2\u0222\u021b\3\2\2\2\u0223" + "[\3\2\2\2\u0224\u0225\7\27\2\2\u0225\u022a\5^\60\2\u0226\u0227\7\37\2" + "\2\u0227\u0229\5^\60\2\u0228\u0226\3\2\2\2\u0229\u022c\3\2\2\2\u022a\u0228" + "\3\2\2\2\u022a\u022b\3\2\2\2\u022b]\3\2\2\2\u022c\u022a\3\2\2\2\u022d" + "\u022e\b\60\1\2\u022e\u022f\7!\2\2\u022f\u0231\5^\60\2\u0230\u0232\7E" + "\2\2\u0231\u0230\3\2\2\2\u0231\u0232\3\2\2\2\u0232\u0233\3\2\2\2\u0233" + "\u0234\7\'\2\2\u0234\u0235\5^\60\2\u0235\u0236\7\62\2\2\u0236\u0237\5" + "t;\2\u0237\u0239\7\20\2\2\u0238\u023a\5l\67\2\u0239\u0238\3\2\2\2\u0239" + "\u023a\3\2\2\2\u023a\u024b\3\2\2\2\u023b\u023c\7!\2\2\u023c\u023d\5^\60" + "\2\u023d\u023e\7F\2\2\u023e\u023f\5^\60\2\u023f\u0241\7\20\2\2\u0240\u0242" + "\5l\67\2\u0241\u0240\3\2\2\2\u0241\u0242\3\2\2\2\u0242\u024b\3\2\2\2\u0243" + "\u0244\7!\2\2\u0244\u0245\5P)\2\u0245\u0247\7\20\2\2\u0246\u0248\5l\67" + "\2\u0247\u0246\3\2\2\2\u0247\u0248\3\2\2\2\u0248\u024b\3\2\2\2\u0249\u024b" + "\5`\61\2\u024a\u022d\3\2\2\2\u024a\u023b\3\2\2\2\u024a\u0243\3\2\2\2\u024a" + "\u0249\3\2\2\2\u024b\u025f\3\2\2\2\u024c\u024e\f\b\2\2\u024d\u024f\7E" + "\2\2\u024e\u024d\3\2\2\2\u024e\u024f\3\2\2\2\u024f\u0250\3\2\2\2\u0250" + "\u0251\7\'\2\2\u0251\u0252\5^\60\2\u0252\u0253\7\62\2\2\u0253\u0255\5" + "t;\2\u0254\u0256\5l\67\2\u0255\u0254\3\2\2\2\u0255\u0256\3\2\2\2\u0256" + "\u025e\3\2\2\2\u0257\u0258\f\7\2\2\u0258\u0259\7F\2\2\u0259\u025b\5^\60" + "\2\u025a\u025c\5l\67\2\u025b\u025a\3\2\2\2\u025b\u025c\3\2\2\2\u025c\u025e" + "\3\2\2\2\u025d\u024c\3\2\2\2\u025d\u0257\3\2\2\2\u025e\u0261\3\2\2\2\u025f" + "\u025d\3\2\2\2\u025f\u0260\3\2\2\2\u0260_\3\2\2\2\u0261\u025f\3\2\2\2" + "\u0262\u0264\5p9\2\u0263\u0265\5l\67\2\u0264\u0263\3\2\2\2\u0264\u0265" + "\3\2\2\2\u0265a\3\2\2\2\u0266\u0267\7\5\2\2\u0267\u0268\5t;\2\u0268c\3" + "\2\2\2\u0269\u026a\7\3\2\2\u026a\u026b\7\36\2\2\u026b\u0270\5r:\2\u026c" + "\u026d\7\37\2\2\u026d\u026f\5r:\2\u026e\u026c\3\2\2\2\u026f\u0272\3\2" + "\2\2\u0270\u026e\3\2\2\2\u0270\u0271\3\2\2\2\u0271e\3\2\2\2\u0272\u0270" + "\3\2\2\2\u0273\u0274\7 \2\2\u0274\u0275\5t;\2\u0275g\3\2\2\2\u0276\u0277" + "\7\16\2\2\u0277\u0278\7\36\2\2\u0278\u027d\5j\66\2\u0279\u027a\7\37\2" + "\2\u027a\u027c\5j\66\2\u027b\u0279\3\2\2\2\u027c\u027f\3\2\2\2\u027d\u027b" + "\3\2\2\2\u027d\u027e\3\2\2\2\u027ei\3\2\2\2\u027f\u027d\3\2\2\2\u0280" + "\u0282\7I\2\2\u0281\u0283\7D\2\2\u0282\u0281\3\2\2\2\u0282\u0283\3\2\2" + "\2\u0283\u0289\3\2\2\2\u0284\u0286\5r:\2\u0285\u0287\7D\2\2\u0286\u0285" + "\3\2\2\2\u0286\u0287\3\2\2\2\u0287\u0289\3\2\2\2\u0288\u0280\3\2\2\2\u0288" + "\u0284\3\2\2\2\u0289k\3\2\2\2\u028a\u028c\7\b\2\2\u028b\u028a\3\2\2\2" + "\u028b\u028c\3\2\2\2\u028c\u028d\3\2\2\2\u028d\u028e\7T\2\2\u028em\3\2" + "\2\2\u028f\u0290\7\60\2\2\u0290\u0292\7&\2\2\u0291\u0293\7I\2\2\u0292" + "\u0291\3\2\2\2\u0292\u0293\3\2\2\2\u0293\u0294\3\2\2\2\u0294\u0295\t\6" + "\2\2\u0295\u0296\7-\2\2\u0296o\3\2\2\2\u0297\u029c\7T\2\2\u0298\u0299" + "\7T\2\2\u0299\u029a\7*\2\2\u029a\u029c\7T\2\2\u029b\u0297\3\2\2\2\u029b" + "\u0298\3\2\2\2\u029cq\3\2\2\2\u029d\u02a2\7T\2\2\u029e\u029f\7T\2\2\u029f" + "\u02a0\7*\2\2\u02a0\u02a2\7T\2\2\u02a1\u029d\3\2\2\2\u02a1\u029e\3\2\2" + "\2\u02a2s\3\2\2\2\u02a3\u02a7\5x=\2\u02a4\u02a6\5v<\2\u02a5\u02a4\3\2" + "\2\2\u02a6\u02a9\3\2\2\2\u02a7\u02a5\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8" + "u\3\2\2\2\u02a9\u02a7\3\2\2\2\u02aa\u02ab\t\7\2\2\u02ab\u02ac\5x=\2\u02ac" + "w\3\2\2\2\u02ad\u02af\7B\2\2\u02ae\u02ad\3\2\2\2\u02ae\u02af\3\2\2\2\u02af" + "\u02b5\3\2\2\2\u02b0\u02b6\5z>\2\u02b1\u02b2\7!\2\2\u02b2\u02b3\5t;\2" + "\u02b3\u02b4\7\20\2\2\u02b4\u02b6\3\2\2\2\u02b5\u02b0\3\2\2\2\u02b5\u02b1" + "\3\2\2\2\u02b6y\3\2\2\2\u02b7\u02b8\5~@\2\u02b8\u02b9\5|?\2\u02b9\u02ba" + "\5~@\2\u02ba\u02c4\3\2\2\2\u02bb\u02bc\5~@\2\u02bc\u02bd\7?\2\2\u02bd" + "\u02c4\3\2\2\2\u02be\u02bf\7\35\2\2\u02bf\u02c0\7!\2\2\u02c0\u02c1\5T" + "+\2\u02c1\u02c2\7\20\2\2\u02c2\u02c4\3\2\2\2\u02c3\u02b7\3\2\2\2\u02c3" + "\u02bb\3\2\2\2\u02c3\u02be\3\2\2\2\u02c4{\3\2\2\2\u02c5\u02c6\t\b\2\2" + "\u02c6}\3\2\2\2\u02c7\u02c8\b@\1\2\u02c8\u02c9\5\u0082B\2\u02c9\u02ca" + "\7!\2\2\u02ca\u02cf\5~@\2\u02cb\u02cc\7\37\2\2\u02cc\u02ce\5~@\2\u02cd" + "\u02cb\3\2\2\2\u02ce\u02d1\3\2\2\2\u02cf\u02cd\3\2\2\2\u02cf\u02d0\3\2" + "\2\2\u02d0\u02d2\3\2\2\2\u02d1\u02cf\3\2\2\2\u02d2\u02d3\7\20\2\2\u02d3" + "\u0302\3\2\2\2\u02d4\u02d5\7:\2\2\u02d5\u02d6\7!\2\2\u02d6\u02d7\7H\2" + "\2\u02d7\u02d8\5~@\2\u02d8\u02d9\7\20\2\2\u02d9\u0302\3\2\2\2\u02da\u02db" + "\7!\2\2\u02db\u02de\5~@\2\u02dc\u02dd\7\37\2\2\u02dd\u02df\5~@\2\u02de" + "\u02dc\3\2\2\2\u02df\u02e0\3\2\2\2\u02e0\u02de\3\2\2\2\u02e0\u02e1\3\2" + "\2\2\u02e1\u02e2\3\2\2\2\u02e2\u02e3\7\20\2\2\u02e3\u0302\3\2\2\2\u02e4" + "\u02e5\7:\2\2\u02e5\u02e6\7!\2\2\u02e6\u02e7\79\2\2\u02e7\u0302\7\20\2" + "\2\u02e8\u0302\5\u0084C\2\u02e9\u0302\5r:\2\u02ea\u02eb\7!\2\2\u02eb\u02ec" + "\5T+\2\u02ec\u02ed\7\20\2\2\u02ed\u0302\3\2\2\2\u02ee\u02ef\7!\2\2\u02ef" + "\u02f0\5~@\2\u02f0\u02f1\7\20\2\2\u02f1\u0302\3\2\2\2\u02f2\u0302\7C\2" + "\2\u02f3\u02f4\7\r\2\2\u02f4\u02f8\5\u0080A\2\u02f5\u02f7\5\u0080A\2\u02f6" + "\u02f5\3\2\2\2\u02f7\u02fa\3\2\2\2\u02f8\u02f6\3\2\2\2\u02f8\u02f9\3\2" + "\2\2\u02f9\u02fb\3\2\2\2\u02fa\u02f8\3\2\2\2\u02fb\u02fc\7\t\2\2\u02fc" + "\u02fd\5~@\2\u02fd\u02ff\7\7\2\2\u02fe\u0300\7\r\2\2\u02ff\u02fe\3\2\2" + "\2\u02ff\u0300\3\2\2\2\u0300\u0302\3\2\2\2\u0301\u02c7\3\2\2\2\u0301\u02d4" + "\3\2\2\2\u0301\u02da\3\2\2\2\u0301\u02e4\3\2\2\2\u0301\u02e8\3\2\2\2\u0301" + "\u02e9\3\2\2\2\u0301\u02ea\3\2\2\2\u0301\u02ee\3\2\2\2\u0301\u02f2\3\2" + "\2\2\u0301\u02f3\3\2\2\2\u0302\u030e\3\2\2\2\u0303\u0304\f\17\2\2\u0304" + "\u0305\t\t\2\2\u0305\u030d\5~@\20\u0306\u0307\f\16\2\2\u0307\u0308\t\n" + "\2\2\u0308\u030d\5~@\17\u0309\u030a\f\r\2\2\u030a\u030b\7;\2\2\u030b\u030d" + "\5~@\16\u030c\u0303\3\2\2\2\u030c\u0306\3\2\2\2\u030c\u0309\3\2\2\2\u030d" + "\u0310\3\2\2\2\u030e\u030c\3\2\2\2\u030e\u030f\3\2\2\2\u030f\177\3\2\2" + "\2\u0310\u030e\3\2\2\2\u0311\u0312\7\65\2\2\u0312\u0313\5t;\2\u0313\u0314" + "\7\34\2\2\u0314\u0315\5~@\2\u0315\u0081\3\2\2\2\u0316\u0317\t\13\2\2\u0317" + "\u0083\3\2\2\2\u0318\u031a\7<\2\2\u0319\u0318\3\2\2\2\u0319\u031a\3\2" + "\2\2\u031a\u031b\3\2\2\2\u031b\u031e\7I\2\2\u031c\u031d\7*\2\2\u031d\u031f" + "\7I\2\2\u031e\u031c\3\2\2\2\u031e\u031f\3\2\2\2\u031f\u0322\3\2\2\2\u0320" + "\u0322\78\2\2\u0321\u0319\3\2\2\2\u0321\u0320\3\2\2\2\u0322\u0085\3\2" + "\2\2_\u0097\u009a\u009d\u00a7\u00b1\u00b6\u00bd\u00c2\u00c8\u00d3\u00d8" + "\u00dc\u00e3\u00eb\u00f2\u00f4\u00fc\u0104\u0109\u010d\u0112\u0114\u011c" + "\u012b\u013c\u0143\u0148\u014a\u015c\u0168\u0177\u0181\u0188\u018d\u0190" + "\u019a\u01aa\u01bf\u01cb\u01d0\u01dc\u01e1\u01e5\u01e8\u01f0\u01f5\u01f8" + "\u01fb\u01fe\u0201\u0205\u020d\u0210\u0216\u0219\u021d\u0220\u0222\u022a" + "\u0231\u0239\u0241\u0247\u024a\u024e\u0255\u025b\u025d\u025f\u0264\u0270" + "\u027d\u0282\u0286\u0288\u028b\u0292\u029b\u02a1\u02a7\u02ae\u02b5\u02c3" + "\u02cf\u02e0\u02f8\u02ff\u0301\u030c\u030e\u0319\u031e\u0321";

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
		enterRule(_localctx, 48, RULE_any);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(378);
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
		enterRule(_localctx, 126, RULE_caseCase);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(783);
				match(51);
				setState(784);
				searchCondition();
				setState(785);
				match(26);
				setState(786);
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
		enterRule(_localctx, 64, RULE_char2);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(426);
				_la = _input.LA(1);
				if (!(_la == 15 || _la == 38))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(427);
				match(31);
				setState(428);
				match(INTEGER);
				setState(429);
				match(14);
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
		enterRule(_localctx, 54, RULE_colDef);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(392);
				columnName();
				setState(393);
				dataType();
				setState(395);
				_la = _input.LA(1);
				if (_la == NOT)
				{
					{
						setState(394);
						notNull();
					}
				}

				setState(398);
				_la = _input.LA(1);
				if (_la == 9)
				{
					{
						setState(397);
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
		enterRule(_localctx, 74, RULE_colList);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(439);
				match(31);
				setState(440);
				columnName();
				setState(445);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 29)
				{
					{
						{
							setState(441);
							match(29);
							setState(442);
							columnName();
						}
					}
					setState(447);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(448);
				match(14);
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
		enterRule(_localctx, 112, RULE_columnName);
		try
		{
			setState(671);
			switch (getInterpreter().adaptivePredict(_input, 78, _ctx))
			{
				case 1:
					_localctx = new Col1PartContext(_localctx);
					enterOuterAlt(_localctx, 1);
					{
						setState(667);
						match(IDENTIFIER);
					}
					break;

				case 2:
					_localctx = new Col2PartContext(_localctx);
					enterOuterAlt(_localctx, 2);
					{
						{
							setState(668);
							match(IDENTIFIER);
							setState(669);
							match(40);
							setState(670);
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
		enterRule(_localctx, 28, RULE_columnSet);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(291);
				match(33);
				setState(292);
				columnName();
				setState(297);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 23)
				{
					{
						{
							setState(293);
							match(23);
							setState(294);
							columnName();
						}
					}
					setState(299);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(300);
				match(10);
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
		enterRule(_localctx, 76, RULE_commonTableExpression);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(450);
				match(IDENTIFIER);
				setState(462);
				_la = _input.LA(1);
				if (_la == 31)
				{
					{
						setState(451);
						match(31);
						setState(452);
						columnName();
						setState(457);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la == 29)
						{
							{
								{
									setState(453);
									match(29);
									setState(454);
									columnName();
								}
							}
							setState(459);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(460);
						match(14);
					}
				}

				setState(464);
				match(6);
				setState(465);
				match(31);
				setState(466);
				fullSelect();
				setState(467);
				match(14);
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
		enterRule(_localctx, 116, RULE_connectedSearchClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(680);
				_la = _input.LA(1);
				if (!(_la == AND || _la == OR))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(681);
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
		enterRule(_localctx, 80, RULE_connectedSelect);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(488);
				match(TABLECOMBINATION);
				setState(494);
				switch (_input.LA(1))
				{
					case 24:
					{
						setState(489);
						subSelect();
					}
					break;
					case 31:
					{
						setState(490);
						match(31);
						setState(491);
						fullSelect();
						setState(492);
						match(14);
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
		enterRule(_localctx, 106, RULE_correlationClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(649);
				_la = _input.LA(1);
				if (_la == 6)
				{
					{
						setState(648);
						match(6);
					}
				}

				setState(651);
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
		enterRule(_localctx, 42, RULE_createIndex);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(344);
				match(45);
				setState(346);
				_la = _input.LA(1);
				if (_la == UNIQUE)
				{
					{
						setState(345);
						match(UNIQUE);
					}
				}

				setState(348);
				match(2);
				setState(349);
				tableName();
				setState(350);
				match(48);
				setState(351);
				tableName();
				setState(352);
				match(31);
				setState(353);
				indexDef();
				setState(358);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 29)
				{
					{
						{
							setState(354);
							match(29);
							setState(355);
							indexDef();
						}
					}
					setState(360);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(361);
				match(14);
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
				setState(200);
				match(45);
				setState(201);
				match(16);
				setState(202);
				tableName();
				setState(203);
				match(31);
				setState(204);
				colDef();
				setState(209);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 9, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						{
							{
								setState(205);
								match(29);
								setState(206);
								colDef();
							}
						}
					}
					setState(211);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 9, _ctx);
				}
				setState(214);
				_la = _input.LA(1);
				if (_la == 29)
				{
					{
						setState(212);
						match(29);
						setState(213);
						primaryKey();
					}
				}

				setState(216);
				match(14);
				setState(218);
				switch (getInterpreter().adaptivePredict(_input, 11, _ctx))
				{
					case 1:
					{
						setState(217);
						groupExp();
					}
					break;
				}
				setState(220);
				nodeExp();
				setState(221);
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
		enterRule(_localctx, 38, RULE_createView);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(334);
				match(45);
				setState(335);
				match(32);
				setState(336);
				tableName();
				setState(337);
				match(6);
				setState(338);
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
		enterRule(_localctx, 62, RULE_dataType);
		try
		{
			setState(424);
			switch (_input.LA(1))
			{
				case 15:
				case 38:
					enterOuterAlt(_localctx, 1);
					{
						setState(419);
						char2();
					}
					break;
				case 25:
					enterOuterAlt(_localctx, 2);
					{
						setState(420);
						int2();
					}
					break;
				case 34:
					enterOuterAlt(_localctx, 3);
					{
						setState(421);
						long2();
					}
					break;
				case DATE:
					enterOuterAlt(_localctx, 4);
					{
						setState(422);
						date2();
					}
					break;
				case 19:
				case 49:
					enterOuterAlt(_localctx, 5);
					{
						setState(423);
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
		enterRule(_localctx, 70, RULE_date2);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(435);
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
				setState(194);
				match(4);
				setState(195);
				match(21);
				setState(196);
				tableName();
				setState(198);
				_la = _input.LA(1);
				if (_la == 3)
				{
					{
						setState(197);
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
		enterRule(_localctx, 34, RULE_deviceExp);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(321);
				switch (_input.LA(1))
				{
					case ALL:
					{
						setState(319);
						match(ALL);
					}
					break;
					case 33:
					{
						setState(320);
						integerSet();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				setState(328);
				_la = _input.LA(1);
				if (_la == 29)
				{
					{
						setState(323);
						match(29);
						setState(326);
						switch (getInterpreter().adaptivePredict(_input, 26, _ctx))
						{
							case 1:
							{
								setState(324);
								hashExp();
							}
							break;

							case 2:
							{
								setState(325);
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
		enterRule(_localctx, 44, RULE_dropIndex);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(363);
				match(20);
				setState(364);
				match(2);
				setState(365);
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
		enterRule(_localctx, 36, RULE_dropTable);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(330);
				match(20);
				setState(331);
				match(16);
				setState(332);
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
		enterRule(_localctx, 40, RULE_dropView);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(340);
				match(20);
				setState(341);
				match(32);
				setState(342);
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
		enterRule(_localctx, 108, RULE_fetchFirst);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(653);
				match(46);
				setState(654);
				match(36);
				setState(656);
				_la = _input.LA(1);
				if (_la == INTEGER)
				{
					{
						setState(655);
						match(INTEGER);
					}
				}

				setState(658);
				_la = _input.LA(1);
				if (!(_la == 18 || _la == 47))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(659);
				match(43);
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
		enterRule(_localctx, 72, RULE_float2);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(437);
				_la = _input.LA(1);
				if (!(_la == 19 || _la == 49))
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
		enterRule(_localctx, 90, RULE_fromClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(546);
				match(21);
				setState(547);
				tableReference(0);
				setState(552);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 29)
				{
					{
						{
							setState(548);
							match(29);
							setState(549);
							tableReference(0);
						}
					}
					setState(554);
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
		enterRule(_localctx, 78, RULE_fullSelect);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(474);
				switch (_input.LA(1))
				{
					case 24:
					{
						setState(469);
						subSelect();
					}
					break;
					case 31:
					{
						setState(470);
						match(31);
						setState(471);
						fullSelect();
						setState(472);
						match(14);
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				setState(479);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == TABLECOMBINATION)
				{
					{
						{
							setState(476);
							connectedSelect();
						}
					}
					setState(481);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(483);
				_la = _input.LA(1);
				if (_la == 12)
				{
					{
						setState(482);
						orderBy();
					}
				}

				setState(486);
				_la = _input.LA(1);
				if (_la == 46)
				{
					{
						setState(485);
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
		enterRule(_localctx, 98, RULE_groupBy);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(615);
				match(1);
				setState(616);
				match(28);
				setState(617);
				columnName();
				setState(622);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 29)
				{
					{
						{
							setState(618);
							match(29);
							setState(619);
							columnName();
						}
					}
					setState(624);
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
		enterRule(_localctx, 16, RULE_groupDef);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(244);
				match(33);
				setState(245);
				match(INTEGER);
				setState(250);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 23)
				{
					{
						{
							setState(246);
							match(23);
							setState(247);
							match(INTEGER);
						}
					}
					setState(252);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(253);
				match(10);
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
		enterRule(_localctx, 12, RULE_groupExp);
		try
		{
			setState(225);
			switch (_input.LA(1))
			{
				case NONE:
					enterOuterAlt(_localctx, 1);
					{
						setState(223);
						match(NONE);
					}
					break;
				case 33:
					enterOuterAlt(_localctx, 2);
					{
						setState(224);
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
		enterRule(_localctx, 26, RULE_hashExp);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(287);
				match(HASH);
				setState(288);
				match(29);
				setState(289);
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
		enterRule(_localctx, 100, RULE_havingClause);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(625);
				match(30);
				setState(626);
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
		enterRule(_localctx, 128, RULE_identifier);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(788);
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
		enterRule(_localctx, 52, RULE_indexDef);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(388);
				columnName();
				setState(390);
				_la = _input.LA(1);
				if (_la == DIRECTION)
				{
					{
						setState(389);
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
				setState(161);
				match(22);
				setState(162);
				match(39);
				setState(163);
				tableName();
				setState(180);
				switch (_input.LA(1))
				{
					case 21:
					case 24:
					case 31:
					{
						{
							setState(165);
							_la = _input.LA(1);
							if (_la == 21)
							{
								{
									setState(164);
									match(21);
								}
							}

							setState(167);
							fullSelect();
						}
					}
					break;
					case 35:
					{
						{
							setState(168);
							match(35);
							setState(169);
							match(31);
							setState(170);
							expression(0);
							setState(175);
							_errHandler.sync(this);
							_la = _input.LA(1);
							while (_la == 29)
							{
								{
									{
										setState(171);
										match(29);
										setState(172);
										expression(0);
									}
								}
								setState(177);
								_errHandler.sync(this);
								_la = _input.LA(1);
							}
							setState(178);
							match(14);
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
		enterRule(_localctx, 66, RULE_int2);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(431);
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

	public final IntegerSetContext integerSet() throws RecognitionException
	{
		IntegerSetContext _localctx = new IntegerSetContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_integerSet);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(276);
				match(33);
				setState(277);
				match(INTEGER);
				setState(282);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 23)
				{
					{
						{
							setState(278);
							match(23);
							setState(279);
							match(INTEGER);
						}
					}
					setState(284);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(285);
				match(10);
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
		enterRule(_localctx, 130, RULE_literal);
		int _la;
		try
		{
			setState(799);
			switch (_input.LA(1))
			{
				case NEGATIVE:
				case INTEGER:
					_localctx = new NumericLiteralContext(_localctx);
					enterOuterAlt(_localctx, 1);
					{
						setState(791);
						_la = _input.LA(1);
						if (_la == NEGATIVE)
						{
							{
								setState(790);
								match(NEGATIVE);
							}
						}

						setState(793);
						match(INTEGER);
						setState(796);
						switch (getInterpreter().adaptivePredict(_input, 91, _ctx))
						{
							case 1:
							{
								setState(794);
								match(40);
								setState(795);
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
						setState(798);
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
		enterRule(_localctx, 46, RULE_load);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(367);
				match(8);
				setState(368);
				_la = _input.LA(1);
				if (!(_la == REPLACE || _la == RESUME))
				{
					_errHandler.recoverInline(this);
				}
				consume();
				setState(369);
				match(39);
				setState(370);
				tableName();
				setState(373);
				_la = _input.LA(1);
				if (_la == 50)
				{
					{
						setState(371);
						match(50);
						setState(372);
						any();
					}
				}

				setState(375);
				match(21);
				setState(376);
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
		enterRule(_localctx, 68, RULE_long2);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(433);
				match(34);
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
		enterRule(_localctx, 20, RULE_nodeExp);
		try
		{
			setState(263);
			switch (_input.LA(1))
			{
				case ANYTEXT:
					enterOuterAlt(_localctx, 1);
					{
						setState(261);
						match(ANYTEXT);
					}
					break;
				case 33:
				case ALL:
					enterOuterAlt(_localctx, 2);
					{
						setState(262);
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
		enterRule(_localctx, 58, RULE_notNull);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(413);
				match(NOT);
				setState(414);
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
		enterRule(_localctx, 122, RULE_operator);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(707);
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
		enterRule(_localctx, 102, RULE_orderBy);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(628);
				match(12);
				setState(629);
				match(28);
				setState(630);
				sortKey();
				setState(635);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 29)
				{
					{
						{
							setState(631);
							match(29);
							setState(632);
							sortKey();
						}
					}
					setState(637);
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

	public final PredicateContext predicate() throws RecognitionException
	{
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_predicate);
		try
		{
			setState(705);
			switch (getInterpreter().adaptivePredict(_input, 82, _ctx))
			{
				case 1:
					_localctx = new NormalPredicateContext(_localctx);
					enterOuterAlt(_localctx, 1);
					{
						{
							setState(693);
							expression(0);
							setState(694);
							operator();
							setState(695);
							expression(0);
						}
					}
					break;

				case 2:
					_localctx = new NullPredicateContext(_localctx);
					enterOuterAlt(_localctx, 2);
					{
						{
							setState(697);
							expression(0);
							setState(698);
							match(NULLOPERATOR);
						}
					}
					break;

				case 3:
					_localctx = new ExistsPredicateContext(_localctx);
					enterOuterAlt(_localctx, 3);
					{
						setState(700);
						match(27);
						setState(701);
						match(31);
						setState(702);
						subSelect();
						setState(703);
						match(14);
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
		enterRule(_localctx, 60, RULE_primary);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(416);
				match(9);
				setState(417);
				match(44);
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
		enterRule(_localctx, 56, RULE_primaryKey);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(400);
				match(9);
				setState(401);
				match(44);
				setState(402);
				match(31);
				setState(403);
				columnName();
				setState(408);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 29)
				{
					{
						{
							setState(404);
							match(29);
							setState(405);
							columnName();
						}
					}
					setState(410);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(411);
				match(14);
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
		enterRule(_localctx, 18, RULE_rangeExp);
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(258);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 17, _ctx);
				while (_alt != 1 && _alt != -1)
				{
					if (_alt == 1 + 1)
					{
						{
							{
								setState(255);
								matchWildcard();
							}
						}
					}
					setState(260);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 17, _ctx);
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
		enterRule(_localctx, 32, RULE_rangeSet);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(308);
				match(33);
				setState(309);
				rangeExp();
				setState(314);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 23)
				{
					{
						{
							setState(310);
							match(23);
							setState(311);
							rangeExp();
						}
					}
					setState(316);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(317);
				match(10);
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
		enterRule(_localctx, 30, RULE_rangeType);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(302);
				match(RANGE);
				setState(303);
				match(29);
				setState(304);
				columnName();
				setState(305);
				match(29);
				setState(306);
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
		enterRule(_localctx, 14, RULE_realGroupExp);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(227);
				match(33);
				setState(228);
				groupDef();
				setState(233);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la == 23)
				{
					{
						{
							setState(229);
							match(23);
							setState(230);
							groupDef();
						}
					}
					setState(235);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(236);
				match(10);
				setState(242);
				_la = _input.LA(1);
				if (_la == 29)
				{
					{
						setState(237);
						match(29);
						setState(240);
						switch (_input.LA(1))
						{
							case HASH:
							{
								setState(238);
								hashExp();
							}
							break;
							case RANGE:
							{
								setState(239);
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
		enterRule(_localctx, 22, RULE_realNodeExp);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(267);
				switch (_input.LA(1))
				{
					case ALL:
					{
						setState(265);
						match(ALL);
					}
					break;
					case 33:
					{
						setState(266);
						integerSet();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				setState(274);
				_la = _input.LA(1);
				if (_la == 29)
				{
					{
						setState(269);
						match(29);
						setState(272);
						switch (_input.LA(1))
						{
							case HASH:
							{
								setState(270);
								hashExp();
							}
							break;
							case RANGE:
							{
								setState(271);
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
		enterRule(_localctx, 50, RULE_remainder);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(383);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1) | (1L << 2) | (1L << 3) | (1L << 4) | (1L << 5) | (1L << 6) | (1L << 7) | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 11) | (1L << 12) | (1L << 13) | (1L << 14) | (1L << 15) | (1L << 16) | (1L << 17) | (1L << 18) | (1L << 19) | (1L << 20) | (1L << 21) | (1L << 22) | (1L << 23) | (1L << 24) | (1L << 25) | (1L << 26) | (1L << 27) | (1L << 28) | (1L << 29) | (1L << 30) | (1L << 31) | (1L << 32) | (1L << 33) | (1L << 34) | (1L << 35) | (1L << 36) | (1L << 37) | (1L << 38) | (1L << 39) | (1L << 40) | (1L << 41) | (1L << 42) | (1L << 43) | (1L << 44) | (1L << 45) | (1L << 46) | (1L << 47) | (1L << 48) | (1L << 49) | (1L << 50) | (1L << 51) | (1L << 52) | (1L << 53) | (1L << STRING) | (1L << STAR) | (1L << COUNT) | (1L << CONCAT) | (1L << NEGATIVE) | (1L << EQUALS) | (1L << OPERATOR) | (1L << NULLOPERATOR) | (1L << AND) | (1L << OR))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (NOT - 64)) | (1L << (NULL - 64)) | (1L << (DIRECTION - 64)) | (1L << (JOINTYPE - 64)) | (1L << (CROSSJOIN - 64)) | (1L << (TABLECOMBINATION - 64)) | (1L << (DISTINCT - 64)) | (1L << (INTEGER - 64)) | (1L << (WS - 64)) | (1L << (UNIQUE - 64)) | (1L << (REPLACE - 64)) | (1L << (RESUME - 64)) | (1L << (NONE - 64)) | (1L << (ALL - 64)) | (1L << (ANYTEXT - 64)) | (1L << (HASH - 64)) | (1L << (RANGE - 64)) | (1L << (DATE - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (ANY - 64)))) != 0))
				{
					{
						{
							setState(380);
							matchWildcard();
						}
					}
					setState(385);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(386);
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
				setState(157);
				match(17);
				setState(158);
				match(48);
				setState(159);
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
		enterRule(_localctx, 118, RULE_searchClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(684);
				_la = _input.LA(1);
				if (_la == NOT)
				{
					{
						setState(683);
						match(NOT);
					}
				}

				setState(691);
				switch (getInterpreter().adaptivePredict(_input, 81, _ctx))
				{
					case 1:
					{
						setState(686);
						predicate();
					}
					break;

					case 2:
					{
						{
							setState(687);
							match(31);
							setState(688);
							searchCondition();
							setState(689);
							match(14);
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
		enterRule(_localctx, 114, RULE_searchCondition);
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(673);
				searchClause();
				setState(677);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 79, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						{
							{
								setState(674);
								connectedSearchClause();
							}
						}
					}
					setState(679);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 79, _ctx);
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
			setState(155);
			switch (getInterpreter().adaptivePredict(_input, 2, _ctx))
			{
				case 1:
					enterOuterAlt(_localctx, 1);
					{
						setState(132);
						insert();
					}
					break;

				case 2:
					enterOuterAlt(_localctx, 2);
					{
						setState(133);
						update();
					}
					break;

				case 3:
					enterOuterAlt(_localctx, 3);
					{
						setState(134);
						delete();
					}
					break;

				case 4:
					enterOuterAlt(_localctx, 4);
					{
						setState(135);
						createTable();
					}
					break;

				case 5:
					enterOuterAlt(_localctx, 5);
					{
						setState(136);
						createIndex();
					}
					break;

				case 6:
					enterOuterAlt(_localctx, 6);
					{
						setState(137);
						createView();
					}
					break;

				case 7:
					enterOuterAlt(_localctx, 7);
					{
						setState(138);
						dropTable();
					}
					break;

				case 8:
					enterOuterAlt(_localctx, 8);
					{
						setState(139);
						dropIndex();
					}
					break;

				case 9:
					enterOuterAlt(_localctx, 9);
					{
						setState(140);
						dropView();
					}
					break;

				case 10:
					enterOuterAlt(_localctx, 10);
					{
						setState(141);
						load();
					}
					break;

				case 11:
					enterOuterAlt(_localctx, 11);
					{
						setState(142);
						runstats();
					}
					break;

				case 12:
					enterOuterAlt(_localctx, 12);
					{
						{
							setState(152);
							_la = _input.LA(1);
							if (_la == 52)
							{
								{
									setState(143);
									match(52);
									setState(144);
									commonTableExpression();
									setState(149);
									_errHandler.sync(this);
									_la = _input.LA(1);
									while (_la == 29)
									{
										{
											{
												setState(145);
												match(29);
												setState(146);
												commonTableExpression();
											}
										}
										setState(151);
										_errHandler.sync(this);
										_la = _input.LA(1);
									}
								}
							}

							setState(154);
							fullSelect();
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
		enterRule(_localctx, 84, RULE_selectClause);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(513);
				match(24);
				setState(515);
				_la = _input.LA(1);
				if (_la == DISTINCT || _la == ALL)
				{
					{
						setState(514);
						selecthow();
					}
				}

				setState(526);
				switch (_input.LA(1))
				{
					case STAR:
					{
						setState(517);
						match(STAR);
					}
					break;
					case 11:
					case 31:
					case STRING:
					case COUNT:
					case NEGATIVE:
					case NULL:
					case INTEGER:
					case DATE:
					case IDENTIFIER:
					{
						{
							setState(518);
							selectListEntry();
							setState(523);
							_errHandler.sync(this);
							_la = _input.LA(1);
							while (_la == 29)
							{
								{
									{
										setState(519);
										match(29);
										setState(520);
										selectListEntry();
									}
								}
								setState(525);
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
		enterRule(_localctx, 86, RULE_selecthow);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(528);
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
		enterRule(_localctx, 88, RULE_selectListEntry);
		int _la;
		try
		{
			setState(544);
			switch (getInterpreter().adaptivePredict(_input, 57, _ctx))
			{
				case 1:
					_localctx = new SelectColumnContext(_localctx);
					enterOuterAlt(_localctx, 1);
					{
						setState(530);
						columnName();
						setState(532);
						_la = _input.LA(1);
						if (_la == 6)
						{
							{
								setState(531);
								match(6);
							}
						}

						setState(535);
						_la = _input.LA(1);
						if (_la == IDENTIFIER)
						{
							{
								setState(534);
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
							setState(537);
							expression(0);
							setState(539);
							_la = _input.LA(1);
							if (_la == 6)
							{
								{
									setState(538);
									match(6);
								}
							}

							setState(542);
							_la = _input.LA(1);
							if (_la == IDENTIFIER)
							{
								{
									setState(541);
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
			case 46:
				return tableReference_sempred((TableReferenceContext)_localctx, predIndex);

			case 62:
				return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}

	public final SingleTableContext singleTable() throws RecognitionException
	{
		SingleTableContext _localctx = new SingleTableContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_singleTable);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(608);
				tableName();
				setState(610);
				switch (getInterpreter().adaptivePredict(_input, 69, _ctx))
				{
					case 1:
					{
						setState(609);
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
		enterRule(_localctx, 104, RULE_sortKey);
		int _la;
		try
		{
			setState(646);
			switch (_input.LA(1))
			{
				case INTEGER:
					_localctx = new SortKeyIntContext(_localctx);
					enterOuterAlt(_localctx, 1);
					{
						setState(638);
						match(INTEGER);
						setState(640);
						_la = _input.LA(1);
						if (_la == DIRECTION)
						{
							{
								setState(639);
								match(DIRECTION);
							}
						}

					}
					break;
				case IDENTIFIER:
					_localctx = new SortKeyColContext(_localctx);
					enterOuterAlt(_localctx, 2);
					{
						setState(642);
						columnName();
						setState(644);
						_la = _input.LA(1);
						if (_la == DIRECTION)
						{
							{
								setState(643);
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
		enterRule(_localctx, 82, RULE_subSelect);
		int _la;
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(496);
				selectClause();
				setState(497);
				fromClause();
				setState(499);
				_la = _input.LA(1);
				if (_la == 3)
				{
					{
						setState(498);
						whereClause();
					}
				}

				setState(502);
				_la = _input.LA(1);
				if (_la == 1)
				{
					{
						setState(501);
						groupBy();
					}
				}

				setState(505);
				_la = _input.LA(1);
				if (_la == 30)
				{
					{
						setState(504);
						havingClause();
					}
				}

				setState(508);
				switch (getInterpreter().adaptivePredict(_input, 48, _ctx))
				{
					case 1:
					{
						setState(507);
						orderBy();
					}
					break;
				}
				setState(511);
				switch (getInterpreter().adaptivePredict(_input, 49, _ctx))
				{
					case 1:
					{
						setState(510);
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
		enterRule(_localctx, 110, RULE_tableName);
		try
		{
			setState(665);
			switch (getInterpreter().adaptivePredict(_input, 77, _ctx))
			{
				case 1:
					_localctx = new Table1PartContext(_localctx);
					enterOuterAlt(_localctx, 1);
					{
						setState(661);
						match(IDENTIFIER);
					}
					break;

				case 2:
					_localctx = new Table2PartContext(_localctx);
					enterOuterAlt(_localctx, 2);
					{
						{
							setState(662);
							match(IDENTIFIER);
							setState(663);
							match(40);
							setState(664);
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
				setState(182);
				match(42);
				setState(183);
				tableName();
				setState(184);
				match(13);
				setState(187);
				switch (_input.LA(1))
				{
					case IDENTIFIER:
					{
						setState(185);
						columnName();
					}
					break;
					case 31:
					{
						setState(186);
						colList();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				setState(189);
				match(EQUALS);
				setState(190);
				expression(0);
				setState(192);
				_la = _input.LA(1);
				if (_la == 3)
				{
					{
						setState(191);
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
		enterRule(_localctx, 96, RULE_whereClause);
		try
		{
			enterOuterAlt(_localctx, 1);
			{
				setState(612);
				match(3);
				setState(613);
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
		int _startState = 124;
		enterRecursionRule(_localctx, 124, RULE_expression, _p);
		int _la;
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(767);
				switch (getInterpreter().adaptivePredict(_input, 87, _ctx))
				{
					case 1:
					{
						_localctx = new FunctionContext(_localctx);
						_ctx = _localctx;
						setState(710);
						identifier();
						setState(711);
						match(31);
						setState(712);
						expression(0);
						setState(717);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la == 29)
						{
							{
								{
									setState(713);
									match(29);
									setState(714);
									expression(0);
								}
							}
							setState(719);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(720);
						match(14);
					}
					break;

					case 2:
					{
						_localctx = new CountDistinctContext(_localctx);
						_ctx = _localctx;
						setState(722);
						match(COUNT);
						setState(723);
						match(31);
						setState(724);
						match(DISTINCT);
						setState(725);
						expression(0);
						setState(726);
						match(14);
					}
					break;

					case 3:
					{
						_localctx = new ListContext(_localctx);
						_ctx = _localctx;
						setState(728);
						match(31);
						setState(729);
						expression(0);
						setState(732);
						_errHandler.sync(this);
						_la = _input.LA(1);
						do
						{
							{
								{
									setState(730);
									match(29);
									setState(731);
									expression(0);
								}
							}
							setState(734);
							_errHandler.sync(this);
							_la = _input.LA(1);
						} while (_la == 29);
						setState(736);
						match(14);
					}
					break;

					case 4:
					{
						_localctx = new CountStarContext(_localctx);
						_ctx = _localctx;
						setState(738);
						match(COUNT);
						setState(739);
						match(31);
						setState(740);
						match(STAR);
						setState(741);
						match(14);
					}
					break;

					case 5:
					{
						_localctx = new IsLiteralContext(_localctx);
						_ctx = _localctx;
						setState(742);
						literal();
					}
					break;

					case 6:
					{
						_localctx = new ColLiteralContext(_localctx);
						_ctx = _localctx;
						setState(743);
						columnName();
					}
					break;

					case 7:
					{
						_localctx = new ExpSelectContext(_localctx);
						_ctx = _localctx;
						setState(744);
						match(31);
						setState(745);
						subSelect();
						setState(746);
						match(14);
					}
					break;

					case 8:
					{
						_localctx = new PExpressionContext(_localctx);
						_ctx = _localctx;
						setState(748);
						match(31);
						setState(749);
						expression(0);
						setState(750);
						match(14);
					}
					break;

					case 9:
					{
						_localctx = new NullExpContext(_localctx);
						_ctx = _localctx;
						setState(752);
						match(NULL);
					}
					break;

					case 10:
					{
						_localctx = new CaseExpContext(_localctx);
						_ctx = _localctx;
						setState(753);
						match(11);
						setState(754);
						caseCase();
						setState(758);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la == 51)
						{
							{
								{
									setState(755);
									caseCase();
								}
							}
							setState(760);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(761);
						match(7);
						setState(762);
						expression(0);
						setState(763);
						match(5);
						setState(765);
						switch (getInterpreter().adaptivePredict(_input, 86, _ctx))
						{
							case 1:
							{
								setState(764);
								match(11);
							}
							break;
						}
					}
					break;
				}
				_ctx.stop = _input.LT(-1);
				setState(780);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 89, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						if (_parseListeners != null)
						{
							triggerExitRuleEvent();
						}
						{
							setState(778);
							switch (getInterpreter().adaptivePredict(_input, 88, _ctx))
							{
								case 1:
								{
									_localctx = new MulDivContext(new ExpressionContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_expression);
									setState(769);
									if (!(precpred(_ctx, 13)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 13)");
									}
									setState(770);
									((MulDivContext)_localctx).op = _input.LT(1);
									_la = _input.LA(1);
									if (!(_la == 53 || _la == STAR))
									{
										((MulDivContext)_localctx).op = _errHandler.recoverInline(this);
									}
									consume();
									setState(771);
									expression(14);
								}
								break;

								case 2:
								{
									_localctx = new AddSubContext(new ExpressionContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_expression);
									setState(772);
									if (!(precpred(_ctx, 12)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 12)");
									}
									setState(773);
									((AddSubContext)_localctx).op = _input.LT(1);
									_la = _input.LA(1);
									if (!(_la == 41 || _la == NEGATIVE))
									{
										((AddSubContext)_localctx).op = _errHandler.recoverInline(this);
									}
									consume();
									setState(774);
									expression(13);
								}
								break;

								case 3:
								{
									_localctx = new ConcatContext(new ExpressionContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_expression);
									setState(775);
									if (!(precpred(_ctx, 11)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 11)");
									}
									setState(776);
									match(CONCAT);
									setState(777);
									expression(12);
								}
								break;
							}
						}
					}
					setState(782);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 89, _ctx);
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
		int _startState = 92;
		enterRecursionRule(_localctx, 92, RULE_tableReference, _p);
		int _la;
		try
		{
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(584);
				switch (getInterpreter().adaptivePredict(_input, 63, _ctx))
				{
					case 1:
					{
						_localctx = new JoinPContext(_localctx);
						_ctx = _localctx;
						setState(556);
						match(31);
						setState(557);
						tableReference(0);
						setState(559);
						_la = _input.LA(1);
						if (_la == JOINTYPE)
						{
							{
								setState(558);
								match(JOINTYPE);
							}
						}

						setState(561);
						match(37);
						setState(562);
						tableReference(0);
						setState(563);
						match(48);
						setState(564);
						searchCondition();
						setState(565);
						match(14);
						setState(567);
						switch (getInterpreter().adaptivePredict(_input, 60, _ctx))
						{
							case 1:
							{
								setState(566);
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
						setState(569);
						match(31);
						setState(570);
						tableReference(0);
						setState(571);
						match(CROSSJOIN);
						setState(572);
						tableReference(0);
						setState(573);
						match(14);
						setState(575);
						switch (getInterpreter().adaptivePredict(_input, 61, _ctx))
						{
							case 1:
							{
								setState(574);
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
						setState(577);
						match(31);
						setState(578);
						fullSelect();
						setState(579);
						match(14);
						setState(581);
						switch (getInterpreter().adaptivePredict(_input, 62, _ctx))
						{
							case 1:
							{
								setState(580);
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
						setState(583);
						singleTable();
					}
					break;
				}
				_ctx.stop = _input.LT(-1);
				setState(605);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input, 68, _ctx);
				while (_alt != 2 && _alt != -1)
				{
					if (_alt == 1)
					{
						if (_parseListeners != null)
						{
							triggerExitRuleEvent();
						}
						{
							setState(603);
							switch (getInterpreter().adaptivePredict(_input, 67, _ctx))
							{
								case 1:
								{
									_localctx = new JoinContext(new TableReferenceContext(_parentctx, _parentState));
									pushNewRecursionContext(_localctx, _startState, RULE_tableReference);
									setState(586);
									if (!(precpred(_ctx, 6)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 6)");
									}
									setState(588);
									_la = _input.LA(1);
									if (_la == JOINTYPE)
									{
										{
											setState(587);
											match(JOINTYPE);
										}
									}

									setState(590);
									match(37);
									setState(591);
									tableReference(0);
									setState(592);
									match(48);
									setState(593);
									searchCondition();
									setState(595);
									switch (getInterpreter().adaptivePredict(_input, 65, _ctx))
									{
										case 1:
										{
											setState(594);
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
									setState(597);
									if (!(precpred(_ctx, 5)))
									{
										throw new FailedPredicateException(this, "precpred(_ctx, 5)");
									}
									setState(598);
									match(CROSSJOIN);
									setState(599);
									tableReference(0);
									setState(601);
									switch (getInterpreter().adaptivePredict(_input, 66, _ctx))
									{
										case 1:
										{
											setState(600);
											correlationClause();
										}
										break;
									}
								}
								break;
							}
						}
					}
					setState(607);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input, 68, _ctx);
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