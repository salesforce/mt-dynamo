package com.salesforce.dynamodbv2.testsupport;

import com.amazon.dynamodb.grammar.DynamoDbExpressionParser;
import com.amazonaws.services.dynamodbv2.dbenv.DbEnv;
import com.amazonaws.services.dynamodbv2.local.shared.dataaccess.LocalDocumentFactory;
import com.amazonaws.services.dynamodbv2.local.shared.env.LocalDBEnv;
import com.amazonaws.services.dynamodbv2.parser.DynamoDbParser;
import com.amazonaws.services.dynamodbv2.parser.ExpressionErrorListener;
import com.amazonaws.services.dynamodbv2.rr.ExpressionWrapper;
import org.antlr.v4.runtime.tree.ParseTree;

public class Main {

    public static void main(String[] args) {
        DbEnv dbEnv = new LocalDBEnv();
        ExpressionErrorListener errorListener = new ExpressionErrorListener(dbEnv);
        ParseTree tree = DynamoDbExpressionParser.parseCondition("attribute_not_exists(id)", errorListener);

        System.out.println(tree);

        ExpressionWrapper expressionWrapper = DynamoDbParser
            .parseExpression("attribute_not_exists(id)",
                null,
                null,
                dbEnv,
                new LocalDocumentFactory());


        System.out.println(expressionWrapper.getExpression());

    }
}
