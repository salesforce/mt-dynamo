/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.grammar;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.dynamodb.grammar.DynamoDbExpressionParser;
import com.amazon.dynamodb.grammar.DynamoDbGrammarParser;
import com.amazonaws.services.dynamodbv2.local.shared.env.LocalDBEnv;
import com.amazonaws.services.dynamodbv2.parser.ExpressionErrorListener;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.KeyConditionExpressionContext;
import com.salesforce.dynamodbv2.grammar.ExpressionsParser.UpdateExpressionContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Utils;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.Tree;
import org.antlr.v4.runtime.tree.Trees;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * These tests are helpful for developing request expression grammars / parsing, but don't actually assert anything,
 * and are therefore disabled.
 */
class ExpressionsTest {

    /*@ParameterizedTest
    @ValueSource(strings = {
        "Score > Score2 AND Time = Time2",
        "#field1 = :value1",
        ":value1 bEtWeEn foo and bar"
    })
    void testParseCondition(String expression) {
        // our grammar
        ExpressionsLexer lexer = new ExpressionsLexer(CharStreams.fromString(expression));
        ExpressionsParser parser = new ExpressionsParser(new CommonTokenStream(lexer));
        ConditionContext context = parser.condition();

        System.out.println("Our tree: " + TreeUtils.toPrettyTree(context, Arrays.asList(ExpressionsParser.ruleNames)));

        // dynamodb local grammar
        ANTLRErrorListener listener = new ExpressionErrorListener(new LocalDBEnv());
        ParseTree tree = DynamoDbExpressionParser.parseCondition(expression, listener);

        System.out.println("Their tree: "
            + TreeUtils.toPrettyTree(tree, Arrays.asList(DynamoDbGrammarParser.ruleNames)));
    }*/

    @ParameterizedTest
    @ValueSource(strings = {
        "SET #field1 = :value1",
        "sEt Score = :value1, CreatedBy = :value2",
        "add Score :value2",
        "Set Score = :value1 add Score :value2",
        "set #hkField = :hkValue, #rkField = :rkValue",

        "ADD Score :value2, CreatedBy :value1", // add multiple
        "sEt Score = :value1, CreatedBy = :value2 ADD Score :value2", // set multiple, add once
        "sEt Score = :value1, CreatedBy = :value2 ADD Score :value2, CreatedBy :value1" // set multiple, add multiple
    })
    void testParseUpdateExpression(String expression) {
        // our grammar
        ExpressionsParser parser
            = new ExpressionsParser(new CommonTokenStream(new ExpressionsLexer(CharStreams.fromString(expression))));
        UpdateExpressionContext context = parser.updateExpression();

        // dynamodb local grammar
        ANTLRErrorListener listener = new ExpressionErrorListener(new LocalDBEnv());
        ParseTree tree = DynamoDbExpressionParser.parseUpdate(expression, listener);

        String dynamodbTree = TreeUtils.toPrettyTree(tree, Arrays.asList(DynamoDbGrammarParser.ruleNames));
        String ourTree = TreeUtils.toPrettyTree(context, Arrays.asList(ExpressionsParser.ruleNames));

        /*  filter out tree levels that can be ignored
            - operand is only present in dynamodb tree level before a set value level
            - update is present in both trees but dynamodb shows update for two levels and
                our tree shows updateExpression
            - addValue is only present in our tree to describe the value being added
         */
        List<String> dynamodbTreeLevels = Arrays.stream(dynamodbTree.split("\\r?\\n"))
            .filter(x -> !x.contains("operand") && !x.contains("update"))
            .collect(Collectors.toList());

        List<String> ourTreeLevels = Arrays.stream(ourTree.split("\\r?\\n"))
            .filter(x -> !x.contains("operand") && !x.contains("update") && !x.contains("addValue"))
            .collect(Collectors.toList());

        // assert both trees match
        int level = 0;
        for (String ourTreeLevel: ourTreeLevels) {
            String dynamodbTreeLevel = dynamodbTreeLevels.get(level);

            if (dynamodbTreeLevel.contains("_section") || dynamodbTreeLevel.contains("_action")
                || dynamodbTreeLevel.contains("_value")) {
                // dynamodb and our tree contain the same text but differ in the delimiter used and case sensitivity
                dynamodbTreeLevel.replace("_", "").toLowerCase();
                ourTreeLevel.toLowerCase();
            } else if (dynamodbTreeLevel.contains("EOF") || dynamodbTreeLevel.contains("literal")) {
                // for a SET update value, the dynamodb tree has an `operand` level right before the update value,
                // causing the additional indentation, for add there is no additional indentation
                assertEquals(dynamodbTreeLevel.trim(), ourTreeLevel.trim());
            } else {
                assertEquals(dynamodbTreeLevels.get(level), "  " + ourTreeLevel);
            }
            level++;
        }
    }

    @Disabled // no assertions present
    @ParameterizedTest
    @ValueSource(strings = {
        "#hk = :hk",
        "((HashKey = :hk) AND (RangeKey > :rk))",
        "HashKey = :hk AND #rk BETWEEN :value1 AND :value2"
    })
    void testParseKeyConditionExpression(String expression) {
        // our grammar
        ExpressionsParser parser
            = new ExpressionsParser(new CommonTokenStream(new ExpressionsLexer(CharStreams.fromString(expression))));
        KeyConditionExpressionContext context = parser.keyConditionExpression();

        System.out.println("Our tree: " + TreeUtils.toPrettyTree(context, Arrays.asList(ExpressionsParser.ruleNames)));

        // dynamodb local grammar (they don't have a separate parser for key condition expressions)
        ANTLRErrorListener listener = new ExpressionErrorListener(new LocalDBEnv());
        ParseTree tree = DynamoDbExpressionParser.parseCondition(expression, listener);

        System.out.println("Their tree: "
            + TreeUtils.toPrettyTree(tree, Arrays.asList(DynamoDbGrammarParser.ruleNames)));
    }

    @Disabled // no assertions present
    @ParameterizedTest
    @ValueSource(strings = {
        "#hk = :hk",
        "HashKey = :hk AND #rk > :rk AND SomeField < :rk",
        "HashKey = :hk AND #rk BETWEEN :value1 AND :value2",
        "HashKey = :hk AND attribute_not_exists(#field)"
    })
    void testTokenizePlaceholders(String expression) {
        List<? extends Token> tokens = new ExpressionsLexer(CharStreams.fromString(expression)).getAllTokens();
        Set<String> fieldPlaceholders = new HashSet<>();
        Set<String> valuePlaceholders = new HashSet<>();
        for (Token token : tokens) {
            if (token.getType() == ExpressionsLexer.FIELD_PLACEHOLDER) {
                fieldPlaceholders.add(token.getText());
            } else if (token.getType() == ExpressionsLexer.VALUE_PLACEHOLDER) {
                valuePlaceholders.add(token.getText());
            }
        }

        System.out.println(String.format("Placeholders in '%s': fields = %s, values = %s", expression,
            fieldPlaceholders.toString(), valuePlaceholders.toString()));
    }

    // source: https://stackoverflow.com/questions/50064110/antlr4-java-pretty-print-parse-tree-to-stdout
    static class TreeUtils {

        /**
         * Platform dependent end-of-line marker.
         */
        static final String EOL = System.lineSeparator();
        /**
         * The literal indent char(s) used for pretty-printing.
         */
        static final String INDENTS = "  ";
        private static int level;

        private TreeUtils() {
        }

        /**
         * Pretty print a whole tree. {@link Trees#getNodeText} is used on the node payloads to get the text for the
         * nodes. (Derived from {@link Trees#toStringTree}.)
         */
        static String toPrettyTree(final Tree t, final List<String> ruleNames) {
            level = 0;
            return process(t, ruleNames).replaceAll("(?m)^\\s+$", "").replaceAll("\\r?\\n\\r?\\n", EOL);
        }

        private static String process(final Tree t, final List<String> ruleNames) {
            if (t.getChildCount() == 0) {
                return Utils.escapeWhitespace(Trees.getNodeText(t, ruleNames), false);
            }
            StringBuilder sb = new StringBuilder();
            sb.append(lead(level));
            level++;
            String s = Utils.escapeWhitespace(Trees.getNodeText(t, ruleNames), false);
            sb.append(s).append(' ');
            for (int i = 0; i < t.getChildCount(); i++) {
                sb.append(process(t.getChild(i), ruleNames));
            }
            level--;
            sb.append(lead(level));
            return sb.toString();
        }

        private static String lead(int level) {
            StringBuilder sb = new StringBuilder();
            if (level > 0) {
                sb.append(EOL);
                sb.append(INDENTS.repeat(level));
            }
            return sb.toString();
        }
    }

}
