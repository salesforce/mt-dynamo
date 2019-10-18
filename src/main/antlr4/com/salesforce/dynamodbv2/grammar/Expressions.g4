grammar Expressions;

// not supported: REMOVE, ADD, DELETE
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

// not supported: begins_with
keyConditionPart
    : id comparator literal
    | id BETWEEN literal AND literal
    | LPAREN keyConditionPart RPAREN
    ;

/*condition
    : operand comparator operand
    | operand BETWEEN operand AND operand
    | operand IN LPAREN inValues RPAREN
    | function
    | condition AND condition
    | condition OR condition
    | NOT condition
    | LPAREN condition RPAREN
    ;

inValues
    : operand
    | inValues COMMA operand
    ;*/

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
    : VALUE_PLACEHOLDER
    ;

id
    : FIELD_PLACEHOLDER
    | ALPHANUM
    ;

VALUE_PLACEHOLDER
    : ':'ALPHANUM
    ;

FIELD_PLACEHOLDER
    : '#'ALPHANUM
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