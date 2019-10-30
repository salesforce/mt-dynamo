/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.salesforce.dynamodbv2.grammar.ExpressionsLexer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;

class MappingUtils {

    static Set<String> getFieldPlaceholders(String expression) {
        return getTokensOfType(expression, ExpressionsLexer.FIELD_PLACEHOLDER);
    }

    static Set<String> getValuePlaceholders(String expression) {
        return getTokensOfType(expression, ExpressionsLexer.VALUE_PLACEHOLDER);
    }

    private static Set<String> getTokensOfType(String expression, int type) {
        if (expression == null) {
            return Collections.emptySet();
        }
        List<? extends Token> tokens = new ExpressionsLexer(CharStreams.fromString(expression)).getAllTokens();
        return tokens.stream()
            .filter(t -> t.getType() == type)
            .map(Token::getText)
            .collect(Collectors.toSet());
    }

    /**
     * Generates a field name placeholder starting with '#field' and suffixed with an incrementing number skipping
     * any reference that already exists in the expressionAttributeNames map.
     */
    static String getNextPlaceholder(Map<String, ?> existingPlaceholders, String prefix) {
        int counter = 1;
        String placeholderCandidate = prefix + counter;
        while (existingPlaceholders != null && existingPlaceholders.containsKey(placeholderCandidate)) {
            counter++;
            placeholderCandidate = prefix + counter;
        }
        return placeholderCandidate;
    }

    static String getNextFieldPlaceholder(RequestWrapper request) {
        return getNextPlaceholder(request.getExpressionAttributeNames(), "#field");
    }

    static String getNextValuePlaceholder(RequestWrapper request) {
        return getNextPlaceholder(request.getExpressionAttributeValues(), ":value");
    }
}
