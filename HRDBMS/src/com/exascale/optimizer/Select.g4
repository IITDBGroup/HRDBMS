grammar Select;

select : (insert EOF) | (update EOF) | (delete EOF) | (createExternalTable EOF) | (createTable EOF) | (createIndex EOF) | (createView EOF) | (dropTable EOF) | (dropIndex EOF) | (dropView EOF) | (load EOF) | (runstats EOF) | ((('WITH' commonTableExpression (',' commonTableExpression)*)? fullSelect) EOF);
runstats : 'RUNSTATS' 'ON' tableName ;
insert : 'INSERT' 'INTO' tableName (('FROM'? fullSelect) | ('VALUES' '(' expression (',' expression)* ')')) ;
update : 'UPDATE' tableName 'SET' (columnName | colList) EQUALS expression whereClause? ;
delete : 'DELETE' 'FROM' tableName whereClause? ;
createTable : 'CREATE' COLUMN? 'TABLE' tableName '(' colDef (',' colDef)* (',' primaryKey)? ')' colOrder? organization? groupExp? nodeExp deviceExp ;
organization : ORGANIZATION '(' INTEGER (',' INTEGER)* ')' ;
createExternalTable : 'CREATE' 'EXTERNAL' 'TABLE' tableName '(' colDef (',' colDef)* ')' (generalExtTableSpec | javaClassExtTableSpec) ;
generalExtTableSpec: 'IMPORT' 'FROM' sourceList 'FIELDS' 'DELIMITED' 'BY' anything 'ROWS' 'DELIMITED' 'BY' anything 'FILE' 'PATH' FILEPATHIDENTIFIER ;
javaClassExtTableSpec: 'USING' javaClassName 'WITH' 'PARAMETERS' '(' keyValueList ')' ;
javaClassName: JAVACLASSNAMEIDENTIFIER ('.' JAVACLASSNAMEIDENTIFIER)* '.java' ;
keyValueList: anything ':' anything (',' anything ':' anything)*;
anything : . ;
sourceList : 'LOCAL' | 'HDFS' | 'S3' ;
colOrder : COLORDER '(' INTEGER (',' INTEGER)* ')' ;
groupExp : NONE | realGroupExp ;
realGroupExp :  '{' groupDef ('|' groupDef)* '}' (',' (hashExp | rangeType))? ;
groupDef : '{' INTEGER ('|' INTEGER)* '}' ;
rangeExp : .*? ;
nodeExp : ANYTEXT | realNodeExp ;
realNodeExp : (ALL | integerSet) (',' (hashExp | rangeType))? ;
integerSet :  '{' INTEGER ('|' INTEGER)* '}' ;
hashExp : HASH ',' columnSet ;
columnSet : '{' columnName ('|' columnName)* '}' ;
rangeType : RANGE ',' columnName ',' rangeSet ;
rangeSet : '{' rangeExp ('|' rangeExp)* '}' ;
deviceExp : (ALL | integerSet) (',' (hashExp | rangeExp))? ; 
dropTable : 'DROP' 'TABLE' tableName ;
createView : 'CREATE' 'VIEW' tableName 'AS' fullSelect ;
dropView : 'DROP' 'VIEW' tableName ;
createIndex : 'CREATE' UNIQUE? 'INDEX' tableName 'ON' tableName '(' indexDef (',' indexDef)* ')' ;
dropIndex : 'DROP' 'INDEX' tableName ;
load : 'LOAD' (REPLACE | RESUME) 'INTO' tableName ('DELIMITER' any)? 'FROM' remainder ;
any : . ;
remainder : .* EOF ;
indexDef : columnName (DIRECTION)? ;
colDef : columnName dataType notNull? primary? ;
primaryKey : 'PRIMARY' 'KEY' '(' columnName (',' columnName)* ')' ;
notNull : NOT NULL ;
primary : 'PRIMARY' 'KEY' ;
dataType : char2 | int2 | long2 | date2 | float2 ;
char2 : ('CHAR' | 'VARCHAR') '(' INTEGER ')' ;
int2 : 'INTEGER' ;
long2 : 'BIGINT' ;
date2 : DATE ;
float2 : 'FLOAT' | 'DOUBLE' ;
colList : '(' columnName (',' columnName)* ')' ;
commonTableExpression : IDENTIFIER ('(' columnName (',' columnName)* ')')? 'AS' '(' fullSelect ')' ;
fullSelect : (subSelect	| '(' fullSelect ')') (connectedSelect)* (orderBy)? (fetchFirst)? ;
connectedSelect : TABLECOMBINATION (subSelect | '(' fullSelect ')') ;
subSelect : selectClause fromClause (whereClause)? (groupBy)? (havingClause)? (orderBy)? (fetchFirst)? ;
selectClause : 'SELECT' (selecthow)? (STAR | (selectListEntry (',' selectListEntry)*)) ;
selecthow : ALL | DISTINCT ;
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
groupBy : 'GROUP' 'BY' columnName (',' columnName)* ;
havingClause : 'HAVING' searchCondition ;
orderBy : 'ORDER' 'BY' sortKey (',' sortKey)* ;
sortKey : INTEGER (DIRECTION)?		# SortKeyInt 
		| columnName (DIRECTION)?	# SortKeyCol ;
correlationClause : ('AS')? IDENTIFIER ;
fetchFirst : 'FETCH' 'FIRST' (INTEGER)? ('ROW' | 'ROWS') 'ONLY' ;
tableName : IDENTIFIER  					# Table1Part
			| (IDENTIFIER '.' IDENTIFIER) 	# Table2Part ;
columnName : IDENTIFIER						# Col1Part 
			| (IDENTIFIER '.' IDENTIFIER) 	# Col2Part ;
searchCondition : searchClause (connectedSearchClause)* ;
connectedSearchClause : (AND | OR) searchClause ;
searchClause : (NOT)? (predicate | ('(' searchCondition ')')) ;
predicate : (expression operator expression)	# NormalPredicate 
			| (expression NULLOPERATOR) 		# NullPredicate 
			| 'EXISTS' '(' subSelect ')'        # ExistsPredicate ;
operator : OPERATOR | EQUALS ;
expression : expression op=(STAR | '/') expression				# MulDiv
			| expression op=('+'| NEGATIVE) expression			# AddSub
			| expression CONCAT expression						# Concat
			| identifier '(' expression (',' expression)* ')'	# Function
			| COUNT '(' DISTINCT expression ')'                 # CountDistinct
			| '(' expression (',' expression)+ ')'				# List
			| COUNT '(' STAR ')'								# CountStar
			| literal											# IsLiteral
			| columnName										# ColLiteral
			| '(' subSelect ')'									# ExpSelect
			| '(' expression ')' 								# PExpression
			| NULL												# NullExp
			| 'CASE' caseCase (caseCase)* 'ELSE' expression 'END' ('CASE')? #CaseExp;
caseCase : 'WHEN' searchCondition 'THEN' expression;
identifier : COUNT | DATE | IDENTIFIER ;
literal : (NEGATIVE)? INTEGER ('.' INTEGER)?	# NumericLiteral
			| STRING 							# StringLiteral ;

STRING : '\'' ( ESC | . )*? '\'' ;
fragment ESC : '\\\'' ;
STAR : '*' ;
COUNT : 'COUNT' ;
CONCAT : '||' ;
NEGATIVE : '-' ;
EQUALS : '=' ;
OPERATOR : '<>'
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
COLUMN : 'COLUMN' ;
DISTINCT : 'DISTINCT' ;
INTEGER : [0-9]+ ;
WS : [ \t\n\r]+ -> skip ;
UNIQUE : 'UNIQUE' ;
REPLACE : 'REPLACE' ;
RESUME : 'RESUME' ;
NONE : 'NONE' ;
ALL : 'ALL' ;
ANYTEXT : 'ANY' ;
HASH : 'HASH' ;
RANGE : 'RANGE' ;
DATE : 'DATE' ;
COLORDER : 'COLORDER' ;
ORGANIZATION : 'ORGANIZATION' ;
IDENTIFIER : [A-Z]([A-Z] | [0-9] | '_')* ;
JAVACLASSNAMEIDENTIFIER : ([a-z] | [A-Z] | '_' | '$') ([a-z] | [A-Z] | [0-9] | '_' | '$')* ;
FILEPATHIDENTIFIER : ANY ('/' ANY)* ;
ANY : . ;