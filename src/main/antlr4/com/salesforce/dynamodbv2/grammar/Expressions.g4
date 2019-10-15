grammar Expressions;

updateExpression
    : SET setAction
    ;

setAction
    : setFieldToValue
    | setAction COMMA setAction
    ;

setFieldToValue
    : id '=' literal
    ;

keyConditionExpression
    : keyConditionPart
    | keyConditionPart AND keyConditionPart
    ;

keyConditionPart
    : id comparator literal
    | id BETWEEN literal AND literal
    | LPAREN keyConditionPart RPAREN
    ;

// general filter expressions -- not currently used
condition
    : operand comparator operand
    | operand BETWEEN operand AND operand
    | condition AND condition
    | condition OR condition
    | NOT condition
    | LPAREN condition RPAREN
    ;

comparator
    : '='
    | '<>'
    | '<'
    | '<='
    | '>'
    | '>='
    ;

operand
    : path
    | literal
    ;

// not supported: complex paths paths containing '.' or '[n]'
path
    : id
    ;

literal
    : ':'ALPHANUM
    ;

id
    : '#'ALPHANUM
    | ALPHANUM
    ;

SET
    : [sS][eE][tT]
    ;

BETWEEN
    : [bB][eE][tT][wW][eE][eE][nN]
    ;

AND
    : [aA][nN][dD]
    ;

OR
    : [oO][rR]
    ;

NOT
    : [nN][oO][tT]
    ;

LPAREN
    : '('
    ;

RPAREN
    : ')'
    ;

COMMA
    : ','
    ;

ALPHANUM
    : [0-9a-zA-Z_-]+
    ;

WS
    :  [ \t]+ -> skip
    ;