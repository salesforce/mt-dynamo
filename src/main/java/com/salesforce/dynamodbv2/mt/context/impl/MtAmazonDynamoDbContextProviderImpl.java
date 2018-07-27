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
public class MtAmazonDynamoDbContextProviderImpl implements MtAmazonDynamoDbContextProvider {

    private static final String CONTEXT_KEY = "multitenant-context";
    private final ThreadLocal<Object> threadLocal = new ThreadLocal<>();

    @Override
    public void setContext(String tenantId) {
        getContextMap().put(CONTEXT_KEY, tenantId);
    }

    @Override
    public String getContext() {
        String value = getContextMap().get(CONTEXT_KEY);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("no context available");
        } else {
            return value;
        }
    }

    private Map<String, String> getContextMap() {
        Map<String, String> context = (Map<String, String>) threadLocal.get();
        if (context == null) {
            context = new HashMap<>();
            threadLocal.set(context);
        }
        return context;
    }

}