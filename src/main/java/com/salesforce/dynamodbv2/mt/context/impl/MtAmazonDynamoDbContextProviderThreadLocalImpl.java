/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.context.impl;

import static com.google.common.base.Preconditions.checkArgument;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.Optional;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbContextProviderThreadLocalImpl implements MtAmazonDynamoDbContextProvider {

    private static final ThreadLocal<String> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    // TODO enforce length limit?
    private static boolean isValid(String tenantId) {
        for (char c : tenantId.toCharArray()) {
            if (!(Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '-')) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setContext(String tenantId) {
        checkArgument(tenantId == null || isValid(tenantId));
        CONTEXT_THREAD_LOCAL.set(tenantId);
    }

    @Override
    public Optional<String> getContextOpt() {
        return Optional.ofNullable(CONTEXT_THREAD_LOCAL.get());
    }

}