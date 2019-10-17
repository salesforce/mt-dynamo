/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.grammar;

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
@Disabled
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
        "sEt CreatedBy = if_not_exists(CreatedBy, :value)",
        "set Score = :value1, CreatedBy = if_not_exists(CreatedBy, :value2)"
    })
    void testParseUpdateExpression(String expression) {
        // our grammar
        ExpressionsParser parser
            = new ExpressionsParser(new CommonTokenStream(new ExpressionsLexer(CharStreams.fromString(expression))));
        UpdateExpressionContext context = parser.updateExpression();

        System.out.println("Our tree: " + TreeUtils.toPrettyTree(context, Arrays.asList(ExpressionsParser.ruleNames)));

        // dynamodb local grammar
        ANTLRErrorListener listener = new ExpressionErrorListener(new LocalDBEnv());
        ParseTree tree = DynamoDbExpressionParser.parseUpdate(expression, listener);

        System.out.println("Their tree: "
            + TreeUtils.toPrettyTree(tree, Arrays.asList(DynamoDbGrammarParser.ruleNames)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "#hk = :hk",
        "HashKey = :hk AND RangeKey > :rk",
        "HashKey = :hk AND #rk BETWEEN :value1 AND :value2"
    })
    void testParseKeyConditionExpression(String expression) {
        // our grammar
        ExpressionsParser parser
            = new ExpressionsParser(new CommonTokenStream(new ExpressionsLexer(CharStreams.fromString(expression))));
        KeyConditionExpressionContext context = parser.keyConditionExpression();

        System.out.println("Our tree: " + TreeUtils.toPrettyTree(context, Arrays.asList(ExpressionsParser.ruleNames)));
    }

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
        static final String Eol = System.lineSeparator();
        /**
         * The literal indent char(s) used for pretty-printing.
         */
        static final String Indents = "  ";
        private static int level;

        private TreeUtils() {
        }

        /**
         * Pretty print a whole tree. {@link Trees#getNodeText} is used on the node payloads to get the text for the
         * nodes. (Derived from {@link Trees#toStringTree}.)
         */
        static String toPrettyTree(final Tree t, final List<String> ruleNames) {
            level = 0;
            return process(t, ruleNames).replaceAll("(?m)^\\s+$", "").replaceAll("\\r?\\n\\r?\\n", Eol);
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
                sb.append(Eol);
                sb.append(Indents.repeat(level));
            }
            return sb.toString();
        }
    }

}
