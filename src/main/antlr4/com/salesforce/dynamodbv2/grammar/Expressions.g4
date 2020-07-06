grammar Expressions;

// not supported: REMOVE, DELETE
updateExpression
    : setSection EOF
    | addSection EOF
    | setSection addSection EOF
    ;

setSection
    : SET setAction (COMMA setAction)*
    ;

addSection
    : ADD addAction (COMMA addAction)*
    ;

// not supported: path = setValue + setValue, path = setValue - setValue
setAction
    : path '=' setValue
    ;

addAction
    : path addValue
    ;

// not supported: non-literal value, if_not_exists, list_append
setValue
    : literal
    ;

addValue
    : literal
    ;

keyConditionExpression
    : keyCondition EOF
    ;

// not supported: begins_with
keyCondition
    : id comparator literal
    | id BETWEEN literal AND literal
    | keyCondition AND keyCondition
    | LPAREN keyCondition RPAREN
    ;

/*condition
    : operand comparator operand
    | operand BETWEEN operand AND operand
    | function
    | condition AND condition
    | condition OR condition
    | NOT condition
    | LPAREN condition RPAREN
    ;*/

comparator
    : '='
    | '<>'
    | '<'
    | '<='
    | '>'
    | '>='
    ;

/*operand
    : path
    | literal
    ;*/

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

ADD
    : [aA][dD][dD]
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