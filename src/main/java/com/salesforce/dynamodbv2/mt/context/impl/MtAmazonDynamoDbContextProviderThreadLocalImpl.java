/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.context.impl;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbContextProviderThreadLocalImpl implements MtAmazonDynamoDbContextProvider {

    private static final String CONTEXT_KEY = "multitenant-context";
    public static final String BASE_CONTEXT = "";
    private static final ThreadLocal<Map<String, String>> CONTEXT_MAP_THREAD_LOCAL = new ThreadLocal<>();

    @Override
    public void setContext(String tenantId) {
        getContextMap().put(CONTEXT_KEY, tenantId);
    }

    @Override
    public String getContext() {
        final String value = getContextMap().get(CONTEXT_KEY);
        return value == null || value.trim().isEmpty() ? BASE_CONTEXT : value;
    }

    private Map<String, String> getContextMap() {
        Map<String, String> context = CONTEXT_MAP_THREAD_LOCAL.get();
        if (context == null) {
            context = new HashMap<>();
            CONTEXT_MAP_THREAD_LOCAL.set(context);
        }
        return context;
    }

}