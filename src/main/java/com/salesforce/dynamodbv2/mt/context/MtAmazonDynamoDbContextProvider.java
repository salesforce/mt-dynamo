/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.context;

/**
 * Interface that holds the tenant context.
 *
 * @author msgroi
 */
public interface MtAmazonDynamoDbContextProvider {

    String getContext();

    /**
     * Sets the tenant context.
     */
    default void setContext(String tenantId) {
        // defaults to no-op
    }

    /**
     * Sets the context to the specific tenantId, executes the runnable, resets back to original tenantId.
     */
    default void withContext(String tenantId, Runnable runnable) {
        String origContext = getContext();
        try {
            setContext(tenantId);
            runnable.run();
        } finally {
            setContext(origContext);
        }
    }

}
