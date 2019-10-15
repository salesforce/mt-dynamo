/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import java.util.Map;

class MappingUtils {

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
