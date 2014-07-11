grammar select;

select : insert | update | delete | createTable | createIndex | createView | dropTable | dropIndex | dropView | load | runstats | (('WITH' commonTableExpression (',' commonTableExpression)*)? fullSelect) ;
runstats : 'RUNSTATS ON' tableName ;
insert : 'INSERT INTO' tableName (('FROM'? fullSelect) | ('VALUES(' expression (',' expression)* ')')) ;
update : 'UPDATE' tableName 'SET' (columnName | colList) '=' expression whereClause? ;
delete : 'DELETE FROM' tableName whereClause? ;
createTable : 'CREATE TABLE' tableName '(' colDef (',' colDef)* (',' primaryKey)? ')' groupExp? nodeExp deviceExp ;
groupExp : NONE | ('{' groupDef ('|' groupDef)* '}' (',' (HASH ',' '{' columnName ('|' columnName)* '}') | (RANGE ',' columnName ',' '{' rangeExp ('|' rangeExp)* '}'))?) ;
groupDef : '{' INTEGER ('|' INTEGER)* '}' ;
rangeExp : ANY* ;
nodeExp : ANYTEXT | (ALL | ('{' INTEGER ('|' INTEGER)* '}') (',' (HASH ',' '{' columnName ('|' columnName)* '}') | (RANGE ',' columnName ',' '{' rangeExp ('|' rangeExp)* '}'))?) ;
deviceExp : (ALL | ('{' INTEGER ('|' INTEGER)* '}') (',' (HASH ',' '{' columnName ('|' columnName)* '}') | (RANGE ',' columnName ',' '{' rangeExp ('|' rangeExp)* '}'))?) ;
dropTable : 'DROP TABLE' tableName ;
createView : 'CREATE VIEW' tableName 'AS' fullSelect ;
dropView : 'DROP VIEW' tableName ;
createIndex : 'CREATE' UNIQUE? 'INDEX' tableName 'ON' tableName '(' indexDef (',' indexDef)* ')' ;
dropIndex : 'DROP INDEX' tableName ;
load : 'LOAD' (REPLACE | RESUME) 'INTO' tableName ('DELIMITER' ANY)? 'FROM' remainder ;
remainder : ANY* EOF ;
indexDef : columnName (dir=('ASC' | 'DESC'))? ;
colDef : columnName dataType notNull? primary? ;
primaryKey : 'PRIMARY KEY' '(' columnName (',' columnName)* ')' ;
notNull : NOT NULL ;
primary : 'PRIMARY KEY' ;
dataType : char2 | int2 | long2 | date2 | float2 ;
char2 : ('CHAR' | 'VARCHAR') '(' INTEGER ')' ;
int2 : 'INTEGER' ;
long2 : 'BIGINT' ;
date2 : 'DATE' ;
float2 : 'FLOAT' | 'DOUBLE' ;
colList : '(' columnName (',' columnName)* ')' ;
commonTableExpression : IDENTIFIER ('(' columnName (',' columnName)* ')')? 'AS' '(' fullSelect ')' ;
fullSelect : (subSelect	| '(' fullSelect ')') (connectedSelect)* (orderBy)? (fetchFirst)? ;
connectedSelect : TABLECOMBINATION (subSelect | '(' fullSelect ')') ;
subSelect : selectClause fromClause (whereClause)? (groupBy)? (havingClause)? (orderBy)? (fetchFirst)? ;
selectClause : 'SELECT' (SELECTHOW)? (STAR | (selectListEntry (',' selectListEntry)*)) ;
selectListEntry : columnName ('AS')? (IDENTIFIER)?			# SelectColumn
				| (expression ('AS')? (IDENTIFIER)?) 		# SelectExpression ;
fromClause : 'FROM' tableReference (',' tableReference)* ;
tableReference : tableReference (JOINTYPE)? 'JOIN' tableReference 'ON' searchCondition (correlationClause)?	# Join
				| tableReference CROSSJOIN tableReference (correlationClause)?						# CrossJoin
				| '(' tableReference (JOINTYPE)? 'JOIN' tableReference 'ON' searchCondition ')' (correlationClause)?	# JoinP
				| '(' tableReference CROSSJOIN tableReference ')' (correlationClause)?				# CrossJoinP
				| '(' fullSelect ')' (correlationClause)?											# NestedTable
				| singleTable 																		# IsSingleTable ;
singleTable : tableName (correlationClause)? ;
whereClause : 'WHERE' searchCondition ;
groupBy : 'GROUP BY' columnName (',' columnName)* ;
havingClause : 'HAVING' searchCondition ;
orderBy : 'ORDER BY' sortKey (',' sortKey)* ;
sortKey : INTEGER (DIRECTION)?		# SortKeyInt 
		| columnName (DIRECTION)?	# SortKeyCol ;
correlationClause : ('AS')? IDENTIFIER ;
fetchFirst : 'FETCH FIRST' (INTEGER)? ('ROW' | 'ROWS') 'ONLY' ;
tableName : IDENTIFIER  					# Table1Part
			| (IDENTIFIER '.' IDENTIFIER) 	# Table2Part ;
columnName : IDENTIFIER						# Col1Part 
			| (IDENTIFIER '.' IDENTIFIER) 	# Col2Part ;
searchCondition : searchClause (connectedSearchClause)* ;
connectedSearchClause : (AND | OR) searchClause ;
searchClause : (NOT)? (predicate | ('(' searchCondition ')')) ;
predicate : (expression OPERATOR expression)	# NormalPredicate 
			| (expression NULLOPERATOR) 		# NullPredicate 
			| 'EXISTS' '(' subSelect ')'        # ExistsPredicate ;
expression : expression op=(STAR | '/') expression				# MulDiv
			| expression op=('+'| NEGATIVE) expression			# AddSub
			| expression CONCAT expression						# Concat
			| IDENTIFIER '(' expression (',' expression)* ')'	# Function
			| COUNT '(' 'DISTINCT' expression ')'               # CountDistinct
			| '(' expression (',' expression)* ')'				# List
			| COUNT '(' STAR ')'								# CountStar
			| literal											# IsLiteral
			| columnName										# ColLiteral
			| '(' subSelect ')'									# ExpSelect
			| '(' expression ')' 								# PExpression
			| NULL												# NullExp;
literal : (NEGATIVE)? INTEGER ('.' INTEGER)?	# NumericLiteral
			| STRING 							# StringLiteral ;

STRING : '\'' ( ESC | . )*? '\'' ;
fragment ESC : '\\\'' ;
STAR : '*' ;
COUNT : 'COUNT' ;
CONCAT : '||' ;
NEGATIVE : '-' ;
OPERATOR : '='
		| '<>'
		| '!='
		| '<='
		| '<'
		| '>='
		| '>'
		| 'EQUALS'
		| 'NOT EQUALS'
		| 'LIKE'
		| 'NOT LIKE' 
		| 'IN'
		| 'NOT IN' ;
NULLOPERATOR : 'IS NOT NULL' | 'IS NULL' ;
AND : 'AND' ;
OR : 'OR' ;
NOT : 'NOT' ;
NULL : 'NULL' ;
DIRECTION : 'ASC'
			| 'DESC' ;
JOINTYPE : 'INNER'
		| 'LEFT'
		| 'LEFT OUTER'
		| 'RIGHT'
		| 'RIGHT OUTER'
		| 'FULL'
		| 'FULL OUTER' ;
CROSSJOIN : 'CROSS JOIN' ;
TABLECOMBINATION : 'UNION ALL'
				| 'UNION'
				| 'INTERSECT'
				| 'EXCEPT' ;
SELECTHOW : 'ALL'
		| 'DISTINCT' ;
IDENTIFIER : [A-Z]([A-Z] | [0-9] | '_')* ;
INTEGER : [0-9]+ ;
WS : [ \t\n\r]+ -> skip ;
UNIQUE : 'UNIQUE' ;
ANY : . ;
REPLACE : 'REPLACE' ;
RESUME : 'RESUME' ;
NONE : 'NONE' ;
ALL : 'ALL' ;
ANYTEXT : 'ANY' ;
HASH : 'HASH' ;
RANGE : 'RANGE' ;